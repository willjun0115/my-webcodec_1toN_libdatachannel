import { createServer } from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import express from 'express';
import { WebSocketServer, type RawData, type WebSocket } from 'ws';
import * as mediasoup from '../../mediasoup/node/lib/index.js';
// @ts-ignore
import {
  injectEncodedChunk,
  setupPipeline
} from '../../mediasoup/node/lib/webCodecsPipeline.js';
import type * as MediasoupTypes from '../../mediasoup/node/lib/types.js';

// 파일 및 디렉터리 경로 설정
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootDir = path.resolve(__dirname, '..');

// C++ mediasoup worker 실행 파일 경로 설정
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

/**
 * 서버 PC의 사설 IPv4 주소를 자동 탐색하는 함수
 */
function getLocalIp(): string {
  if (process.env.ANNOUNCED_IP) {
    return process.env.ANNOUNCED_IP;
  }
  const interfaces = os.networkInterfaces();
  const candidates: string[] = [];

  for (const name of Object.keys(interfaces)) {
    const lowerName = name.toLowerCase();
    // 가상 어댑터 이름 필터링
    if (
      lowerName.includes('virtual') ||
      lowerName.includes('vmware') ||
      lowerName.includes('vethernet') ||
      lowerName.includes('vbox') ||
      lowerName.includes('wsl') ||
      lowerName.includes('host-only') ||
      lowerName.includes('bluetooth')
    ) {
      continue;
    }

    for (const iface of interfaces[name] ?? []) {
      if (iface.family === 'IPv4' && !iface.internal) {
        // 서브넷 필터링
        if (!iface.address.startsWith('192.168.56.')) {
          return iface.address;
        }
        candidates.push(iface.address);
      }
    }
  }

  if (candidates.length > 0 && candidates[0]) {
    return candidates[0];
  }

  // 예외 처리 필터링으로 검색되지 않았을 경우 일반 비내부 IPv4 반환
  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name] ?? []) {
      if (iface.family === 'IPv4' && !iface.internal) {
        return iface.address;
      }
    }
  }

  return '127.0.0.1';
}

// mediasoup WebRTC 트랜스포트에 발표할 외부/LAN IP 주소
const ANNOUNCED_IP = getLocalIp();
// WebCodecs 파이프라인 전용 SSRC 및 H.264 PayloadType 설정
const PIPELINE_SSRC = 12345678;
const PIPELINE_PAYLOAD_TYPE = 96;

// 클라이언트 피어의 역할: 방송 송신자(producer) 또는 시청 수신자(consumer)
type PeerRole = 'producer' | 'consumer';

// WebSocket JSON 시그널링 메시지 규격
type JsonMessage = {
  id?: string;
  action?: string;
  data?: unknown;
};

// WebCodecs 바이너리 청크 패킷의 JSON 헤더 규격
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

// 피어 접속 상태 관리 객체
type PeerState = {
  id: string;
  role: PeerRole;
  socket: WebSocket;
  transport?: MediasoupTypes.WebRtcTransport;
  consumer?: MediasoupTypes.Consumer;
  statsTimer?: NodeJS.Timeout;
};

// Express 서버 및 WebSocket 시그널링 서버 생성
const app = express();
const httpServer = createServer(app);
const wss = new WebSocketServer({ server: httpServer, path: '/ws' });
const peers = new Map<string, PeerState>();

// mediasoup 글로벌 객체
let router: MediasoupTypes.Router;
let producer: MediasoupTypes.Producer | undefined;
let producerPeerId: string | undefined;
let injectedChunks = 0;
let injectedPackets = 0;
let peerSeq = 0;

// 정적 파일(Vite 빌드 아티팩트) 제공
app.use(express.static(path.join(rootDir, 'dist')));

// 헬스체크 및 현재 룸 상태 모니터링 REST API
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

// mediasoup C++ Worker 및 Router 초기화 실행
await bootstrapMediasoup();

// WebSocket 클라이언트 접속 이벤트 처리
wss.on('connection', async socket => {
  // 새로운 클라이언트 등록 (첫 접속자는 Producer, 이후는 Consumer 자동 지정)
  const peer = await registerPeer(socket);

  // 역할 지정 및 초기 룸 정보 응답 전달
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

  // 소켓 메시지 수신 (바이너리: WebCodecs 인코딩 비트스트림, 텍스트: JSON 시그널링)
  socket.on('message', async (message, isBinary) => {
    try {
      if (isBinary) {
        // Producer가 보낸 WebCodecs 바이너리 청크 수신 ➔ mediasoup 주입
        handleEncodedChunk(peer, message);
        return;
      }

      // Consumer의 WebRTC 트랜스포트 수립 및 Consume 요청 시그널링 처리
      const parsed = JSON.parse(message.toString()) as JsonMessage;
      await handleRequest(socket, peer, parsed);
    } catch (error) {
      sendError(socket, error);
    }
  });

  // 소켓 연결 종료 시 자원 정돈
  socket.on('close', () => {
    cleanupPeer(peer);
  });
});

// HTTP/WebSocket 시그널링 서버 대기
httpServer.listen(HTTP_PORT, '0.0.0.0', () => {
  console.log(`[signaling] HTTP/WebSocket server listening on http://0.0.0.0:${HTTP_PORT}`);
  console.log(`[signaling] WebRTC Announced IP: ${ANNOUNCED_IP}`);
});

/**
 * mediasoup 초기화 함수
 * - C++ mediasoup Worker 프로세스 생성
 * - H.264 프로파일(42e01f)을 지원하는 Router 부트스트랩
 */
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

  // H.264 코덱 및 RTCP Feedback(NACK, PLI, FIR)을 가진 Router 생성
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

/**
 * 클라이언트 피어 등록 함수
 */
async function registerPeer(socket: WebSocket): Promise<PeerState> {
  const id = `peer-${++peerSeq}`;
  const role: PeerRole = producerPeerId ? 'consumer' : 'producer';
  const peer: PeerState = { id, role, socket };

  peers.set(id, peer);

  if (role === 'producer') {
    producerPeerId = id;
    injectedChunks = 0;
    injectedPackets = 0;
    // WebCodecs H.264 비트스트림을 주입받을 단일 DirectTransport Producer 생성
    producer = await setupPipeline(router, {
      ssrc: PIPELINE_SSRC,
      payloadType: PIPELINE_PAYLOAD_TYPE,
      profileLevelId: '42e01f'
    });
    console.log(`[room] ${id} joined as producer, pipeline producer ${producer?.id}`);
  } else {
    console.log(`[room] ${id} joined as consumer of ${producerPeerId}`);
  }

  logPeerCount('join', peer);

  return peer;
}

/**
 * WebSocket 시그널링 메시지 처리 함수 (Consumer 전용)
 * - getRouterRtpCapabilities: mediasoup Router의 RTP 역량 응답
 * - createConsumerTransport: Consumer 전용 WebRTC 트랜스포트 생성 (ICE/DTLS 정보 전달)
 * - connectConsumerTransport: Consumer 클라이언트의 DTLS 파라미터 연결
 * - consume: Producer의 비디오 스트림을 수신할 Consumer 객체 생성
 * - resumeConsumer: Consumer 일시정지 해제 및 스트림 송신 시작
 */
async function handleRequest(
  socket: WebSocket,
  peer: PeerState,
  message: JsonMessage
): Promise<void> {
  const { id, action, data } = message;

  if (!id || !action) {
    throw new Error('Invalid request');
  }

  // 1. Router의 RTP Capabilities 반환
  if (action === 'getRouterRtpCapabilities') {
    reply(socket, id, router.rtpCapabilities);
    return;
  }

  // 2. Consumer WebRTC Transport 생성
  if (action === 'createConsumerTransport') {
    assertConsumerPeer(peer);

    peer.transport = await router.createWebRtcTransport({
      listenInfos: [
        { protocol: 'udp', ip: '0.0.0.0', announcedAddress: ANNOUNCED_IP },
        { protocol: 'tcp', ip: '0.0.0.0', announcedAddress: ANNOUNCED_IP }
      ],
      enableUdp: true,
      enableTcp: true,
      preferUdp: true,
      initialAvailableOutgoingBitrate: 1_000_000
    });

    // 트랜스포트 상태 변화 소켓 브로드캐스트
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

  // 3. Consumer WebRTC Transport DTLS 연결
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

  // 4. Producer 미디어 스트림 Consume 생성
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

    // 일시정지 상태로 Consumer 생성
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

  // 5. Consumer 스트림 일시정지 해제 및 통계 타이머 시작
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

/**
 * Producer가 보낸 바이너리 WebCodecs 청크 패킷 처리 함수
 * - 패킷 구조: [4바이트 Header 길이] + [JSON Header (SPS/PPS 메타데이터)] + [Raw H.264 NAL Unit 데이터]
 * - injectEncodedChunk()를 이용해 mediasoup 파이프라인으로 직접 비트스트림 주입
 */
function handleEncodedChunk(peer: PeerState, raw: RawData): void {
  if (peer.role !== 'producer' || peer.id !== producerPeerId) {
    throw new Error('Only the active producer can send encoded chunks');
  }

  const packet = Buffer.isBuffer(raw) ? raw : Buffer.concat(raw as Buffer[]);

  if (packet.byteLength < 4) {
    throw new Error('Encoded chunk packet is too small');
  }

  // 1. 헤더 길이 파싱 (BigEndian Uint32)
  const headerLength = packet.readUInt32BE(0);
  const headerEnd = 4 + headerLength;

  if (headerEnd > packet.byteLength) {
    throw new Error('Encoded chunk header is truncated');
  }

  // 2. JSON 헤더 및 raw H.264 비트스트림 분리
  const header = JSON.parse(
    packet.subarray(4, headerEnd).toString('utf8')
  ) as EncodedChunkHeader;
  const data = packet.subarray(headerEnd);
  const descriptionBase64 =
    header.metadata?.decoderConfig?.descriptionBase64;

  // 3. mediasoup C++ 내부 RTP 파이프라인으로 H.264 비트스트림 직접 주입
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

  // 10청크마다 또는 키프레임 발생 시 클라이언트에 주입 통계 알림
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

/**
 * 피어 접속 해제 처리 및 자원 정리 함수
 */
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

/**
 * Consumer별 2초 주기 실시간 통계 펌프 시작 함수
 */
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

/**
 * 룸 전체 상태(Producer ID, Consumer 수, 주입 청크 수) 브로드캐스트
 */
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
