import { createServer } from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import { WebSocketServer, type RawData, type WebSocket } from 'ws';
import * as mediasoup from '../../mediasoup/node/lib/index.js';
import {
  injectEncodedChunk,
  setupPipeline
} from '../../mediasoup/node/lib/webCodecsPipeline.js';
import type * as MediasoupTypes from '../../mediasoup/node/lib/types.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, '..');
const localWorkerBin = path.resolve(
  rootDir,
  '..',
  'mediasoup',
  'worker',
  'out',
  'Release',
  process.platform === 'win32' ? 'mediasoup-worker.exe' : 'mediasoup-worker'
);
const HTTP_PORT = Number(process.env.PORT ?? 3000);
const PIPELINE_SSRC = 12345678;
const PIPELINE_PAYLOAD_TYPE = 96;

type PeerRole = 'producer' | 'consumer';

type JsonMessage = {
  id?: string;
  action?: string;
  data?: unknown;
};

type EncodedChunkHeader = {
  event: 'encodedChunk';
  timestamp: number;
  type: 'key' | 'delta';
  duration?: number;
  metadata?: {
    decoderConfig?: {
      codec?: string;
      descriptionBase64?: string;
    };
  };
};

type PeerState = {
  id: string;
  role: PeerRole;
  socket: WebSocket;
  transport?: MediasoupTypes.WebRtcTransport;
  consumer?: MediasoupTypes.Consumer;
  statsTimer?: NodeJS.Timeout;
};

const app = express();
const httpServer = createServer(app);
const wss = new WebSocketServer({ server: httpServer, path: '/ws' });
const peers = new Map<string, PeerState>();

let router: MediasoupTypes.Router;
let producer: MediasoupTypes.Producer | undefined;
let producerPeerId: string | undefined;
let injectedChunks = 0;
let injectedPackets = 0;
let peerSeq = 0;

app.use(express.static(path.join(rootDir, 'dist')));

app.get('/health', (_req, res) => {
  res.json({
    ok: true,
    producerPeerId,
    producerId: producer?.id,
    consumers: countConsumers(),
    injectedChunks,
    injectedPackets
  });
});

await bootstrapMediasoup();

wss.on('connection', async socket => {
  const peer = await registerPeer(socket);

  socket.send(
    JSON.stringify({
      event: 'roleAssigned',
      data: {
        peerId: peer.id,
        role: peer.role,
        producerPeerId,
        producerId: producer?.id,
        ssrc: PIPELINE_SSRC,
        payloadType: PIPELINE_PAYLOAD_TYPE,
        consumerCount: countConsumers()
      }
    })
  );
  broadcastRoomState();

  socket.on('message', async (message, isBinary) => {
    try {
      if (isBinary) {
        handleEncodedChunk(peer, message);
        return;
      }

      const parsed = JSON.parse(message.toString()) as JsonMessage;
      await handleRequest(socket, peer, parsed);
    } catch (error) {
      sendError(socket, error);
    }
  });

  socket.on('close', () => {
    cleanupPeer(peer);
  });
});

httpServer.listen(HTTP_PORT, '127.0.0.1', () => {
  console.log(`[signaling] HTTP/WebSocket server listening on http://127.0.0.1:${HTTP_PORT}`);
  console.log('[signaling] Start the client separately with: npm run client');
});

async function bootstrapMediasoup(): Promise<void> {
  const worker = await mediasoup.createWorker({
    workerBin: localWorkerBin,
    logLevel: 'warn',
    rtcMinPort: 40000,
    rtcMaxPort: 40100
  });

  worker.on('died', () => {
    console.error('mediasoup worker died');
    process.exit(1);
  });

  router = await worker.createRouter({
    mediaCodecs: [
      {
        kind: 'video',
        mimeType: 'video/H264',
        clockRate: 90000,
        parameters: {
          'packetization-mode': 1,
          'profile-level-id': '42e01f',
          'level-asymmetry-allowed': 1
        },
        rtcpFeedback: [
          { type: 'nack' },
          { type: 'nack', parameter: 'pli' },
          { type: 'ccm', parameter: 'fir' }
        ]
      }
    ]
  });

  console.log('mediasoup router ready, waiting for first producer peer');
}

async function registerPeer(socket: WebSocket): Promise<PeerState> {
  const id = `peer-${++peerSeq}`;
  const role: PeerRole = producerPeerId ? 'consumer' : 'producer';
  const peer: PeerState = { id, role, socket };

  peers.set(id, peer);

  if (role === 'producer') {
    producerPeerId = id;
    injectedChunks = 0;
    injectedPackets = 0;
    producer = await setupPipeline(router, {
      ssrc: PIPELINE_SSRC,
      payloadType: PIPELINE_PAYLOAD_TYPE,
      profileLevelId: '42e01f'
    });
    console.log(`[room] ${id} joined as producer, pipeline producer ${producer.id}`);
  } else {
    console.log(`[room] ${id} joined as consumer of ${producerPeerId}`);
  }

  logPeerCount('join', peer);

  return peer;
}

async function handleRequest(
  socket: WebSocket,
  peer: PeerState,
  message: JsonMessage
): Promise<void> {
  const { id, action, data } = message;

  if (!id || !action) {
    throw new Error('Invalid request');
  }

  if (action === 'getRouterRtpCapabilities') {
    reply(socket, id, router.rtpCapabilities);
    return;
  }

  if (action === 'createConsumerTransport') {
    assertConsumerPeer(peer);

    peer.transport = await router.createWebRtcTransport({
      listenInfos: [
        { protocol: 'udp', ip: '127.0.0.1' },
        { protocol: 'tcp', ip: '127.0.0.1' }
      ],
      enableUdp: true,
      enableTcp: true,
      preferUdp: true,
      initialAvailableOutgoingBitrate: 1_000_000
    });

    peer.transport.on('icestatechange', state => {
      socket.send(JSON.stringify({ event: 'transportState', data: { ice: state } }));
    });
    peer.transport.on('dtlsstatechange', state => {
      socket.send(JSON.stringify({ event: 'transportState', data: { dtls: state } }));
    });

    reply(socket, id, {
      id: peer.transport.id,
      iceParameters: peer.transport.iceParameters,
      iceCandidates: peer.transport.iceCandidates,
      dtlsParameters: peer.transport.dtlsParameters
    });
    return;
  }

  if (action === 'connectConsumerTransport') {
    assertConsumerPeer(peer);

    if (!peer.transport) {
      throw new Error('Consumer transport does not exist');
    }

    const { dtlsParameters } = data as {
      dtlsParameters: MediasoupTypes.DtlsParameters;
    };

    await peer.transport.connect({ dtlsParameters });
    reply(socket, id, {});
    return;
  }

  if (action === 'consume') {
    assertConsumerPeer(peer);

    if (!peer.transport) {
      throw new Error('Consumer transport does not exist');
    }
    if (!producer) {
      throw new Error('Producer is not ready yet');
    }

    const { rtpCapabilities } = data as {
      rtpCapabilities: MediasoupTypes.RtpCapabilities;
    };

    if (!router.canConsume({ producerId: producer.id, rtpCapabilities })) {
      throw new Error('Client cannot consume the WebCodecs producer');
    }

    peer.consumer = await peer.transport.consume({
      producerId: producer.id,
      rtpCapabilities,
      paused: true
    });

    await peer.consumer.enableTraceEvent(['rtp', 'keyframe', 'pli', 'fir']);

    peer.consumer.on('transportclose', () => {
      stopStatsPump(peer);
    });

    reply(socket, id, {
      id: peer.consumer.id,
      producerId: producer.id,
      kind: peer.consumer.kind,
      rtpParameters: peer.consumer.rtpParameters
    });
    return;
  }

  if (action === 'resumeConsumer') {
    assertConsumerPeer(peer);

    if (!peer.consumer) {
      throw new Error('Consumer does not exist');
    }

    await peer.consumer.resume();
    startStatsPump(socket, peer);
    reply(socket, id, {});
    return;
  }

  throw new Error(`Unknown action: ${action}`);
}

function handleEncodedChunk(peer: PeerState, raw: RawData): void {
  if (peer.role !== 'producer' || peer.id !== producerPeerId) {
    throw new Error('Only the active producer can send encoded chunks');
  }

  const packet = Buffer.isBuffer(raw) ? raw : Buffer.concat(raw as Buffer[]);

  if (packet.byteLength < 4) {
    throw new Error('Encoded chunk packet is too small');
  }

  const headerLength = packet.readUInt32BE(0);
  const headerEnd = 4 + headerLength;

  if (headerEnd > packet.byteLength) {
    throw new Error('Encoded chunk header is truncated');
  }

  const header = JSON.parse(
    packet.subarray(4, headerEnd).toString('utf8')
  ) as EncodedChunkHeader;
  const data = packet.subarray(headerEnd);
  const descriptionBase64 =
    header.metadata?.decoderConfig?.descriptionBase64;

  injectEncodedChunk(
    header.duration === undefined
      ? {
          data,
          timestamp: header.timestamp,
          type: header.type
        }
      : {
          data,
          timestamp: header.timestamp,
          type: header.type,
          duration: header.duration
        },
    descriptionBase64
      ? {
          decoderConfig: {
            description: Buffer.from(descriptionBase64, 'base64')
          }
        }
      : undefined
  );

  injectedChunks++;
  injectedPackets++;

  if (injectedChunks % 10 === 0 || header.type === 'key') {
    peer.socket.send(
      JSON.stringify({
        event: 'pipelineStats',
        data: {
          injectedChunks,
          injectedPackets,
          lastChunkType: header.type,
          consumerCount: countConsumers()
        }
      })
    );
    broadcastRoomState();
  }
}

function cleanupPeer(peer: PeerState): void {
  stopStatsPump(peer);
  peer.consumer?.close();
  peer.transport?.close();
  peers.delete(peer.id);

  if (peer.id === producerPeerId) {
    console.log(`[room] producer ${peer.id} disconnected`);
    producer?.close();
    producer = undefined;
    producerPeerId = undefined;

    for (const other of peers.values()) {
      stopStatsPump(other);
      other.consumer?.close();
      delete other.consumer;
      other.socket.send(JSON.stringify({ event: 'producerClosed' }));
    }
  }

  logPeerCount('leave', peer);
  broadcastRoomState();
}

function assertConsumerPeer(peer: PeerState): void {
  if (peer.role !== 'consumer') {
    throw new Error('This action is only available to consumer peers');
  }
}

function startStatsPump(socket: WebSocket, peer: PeerState): void {
  stopStatsPump(peer);

  peer.statsTimer = setInterval(async () => {
    try {
      const [producerStats, consumerStats, transportStats] = await Promise.all([
        producer?.getStats(),
        peer.consumer?.getStats(),
        peer.transport?.getStats()
      ]);

      socket.send(
        JSON.stringify({
          event: 'serverStats',
          data: {
            producer: producerStats,
            consumer: consumerStats,
            transport: transportStats
          }
        })
      );
    } catch (error) {
      console.error('[stats] failed', error);
    }
  }, 2000);
}

function stopStatsPump(peer: PeerState): void {
  if (peer.statsTimer) {
    clearInterval(peer.statsTimer);
    delete peer.statsTimer;
  }
}

function broadcastRoomState(): void {
  const data = {
    producerPeerId,
    producerId: producer?.id,
    consumerCount: countConsumers(),
    injectedChunks,
    injectedPackets
  };

  for (const peer of peers.values()) {
    peer.socket.send(JSON.stringify({ event: 'roomState', data }));
  }
}

function countConsumers(): number {
  return [...peers.values()].filter(peer => peer.role === 'consumer').length;
}

function countProducers(): number {
  return [...peers.values()].filter(peer => peer.role === 'producer').length;
}

function logPeerCount(event: 'join' | 'leave', peer: PeerState): void {
  console.log(
    `[room] ${event}: ${peer.id} (${peer.role}) | total=${peers.size}, producers=${countProducers()}, consumers=${countConsumers()}`
  );
}

function reply(socket: WebSocket, id: string, data: unknown): void {
  socket.send(JSON.stringify({ id, ok: true, data }));
}

function sendError(socket: WebSocket, error: unknown, id?: string): void {
  const message = error instanceof Error ? error.message : String(error);

  socket.send(JSON.stringify({ id, ok: false, error: message }));
  console.error(message);
}
