import { Device } from 'mediasoup-client';
import type { types as MediasoupClientTypes } from 'mediasoup-client';
import './styles.css';

type PeerRole = 'producer' | 'consumer';

type ResponseMessage = {
  id?: string;
  ok?: boolean;
  data?: unknown;
  error?: string;
  event?: string;
};

type PendingRequest = {
  resolve: (data: unknown) => void;
  reject: (error: Error) => void;
};

type BrowserChunkType = 'key' | 'delta';

const startButton = document.querySelector<HTMLButtonElement>('#startButton')!;
const localVideo = document.querySelector<HTMLVideoElement>('#localVideo')!;
const remoteVideo = document.querySelector<HTMLVideoElement>('#remoteVideo')!;
const localCaption = document.querySelector<HTMLElement>('#localCaption')!;
const remoteCaption = document.querySelector<HTMLElement>('#remoteCaption')!;
const socketState = document.querySelector<HTMLElement>('#socketState')!;
const roleState = document.querySelector<HTMLElement>('#roleState')!;
const peerIdLabel = document.querySelector<HTMLElement>('#peerId')!;
const consumerCount = document.querySelector<HTMLElement>('#consumerCount')!;
const chunkCount = document.querySelector<HTMLElement>('#chunkCount')!;
const packetCount = document.querySelector<HTMLElement>('#packetCount')!;
const consumerState = document.querySelector<HTMLElement>('#consumerState')!;
const rtpReceived = document.querySelector<HTMLElement>('#rtpReceived')!;
const framesDecoded = document.querySelector<HTMLElement>('#framesDecoded')!;
const logBox = document.querySelector<HTMLPreElement>('#log')!;

let requestSeq = 0;
let encodedChunks = 0;
let socket: WebSocket;
let role: PeerRole | undefined;
const pending = new Map<string, PendingRequest>();

startButton.addEventListener('click', () => {
  startButton.disabled = true;
  start().catch(error => {
    startButton.disabled = false;
    writeLog(`failed: ${error instanceof Error ? error.message : String(error)}`);
  });
});

async function start(): Promise<void> {
  socket = await connectSocket();
  socketState.textContent = 'connected';

  const assignment = await waitForRoleAssignment();
  role = assignment.role;
  roleState.textContent = role;
  peerIdLabel.textContent = assignment.peerId;

  if (role === 'producer') {
    await startProducer();
  } else {
    await startConsumer();
  }
}

async function startProducer(): Promise<void> {
  assertWebCodecsSupport();

  remoteVideo.removeAttribute('src');
  remoteVideo.srcObject = null;
  remoteCaption.textContent = 'Consumers receive this stream';
  localCaption.textContent = 'Producer camera preview';
  consumerState.textContent = 'producer';

  const stream = await navigator.mediaDevices.getUserMedia({
    video: {
      width: { ideal: 640 },
      height: { ideal: 360 },
      frameRate: { ideal: 30, max: 30 }
    },
    audio: false
  });

  localVideo.srcObject = stream;
  await startEncoder(stream);
  writeLog('producer pipeline started');
}

async function startConsumer(): Promise<void> {
  localVideo.removeAttribute('src');
  localVideo.srcObject = null;
  localCaption.textContent = 'Consumer peer';
  remoteCaption.textContent = 'Producer stream';

  await setupConsumer();
  writeLog('consumer connected');
}

async function connectSocket(): Promise<WebSocket> {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const ws = new WebSocket(`${protocol}//${window.location.host}/ws`);

  ws.binaryType = 'arraybuffer';

  ws.addEventListener('message', event => {
    if (typeof event.data !== 'string') {
      return;
    }

    const message = JSON.parse(event.data) as ResponseMessage;

    if (message.id) {
      const entry = pending.get(message.id);

      if (!entry) {
        return;
      }

      pending.delete(message.id);

      if (message.ok) {
        entry.resolve(message.data);
      } else {
        entry.reject(new Error(message.error ?? 'request failed'));
      }

      return;
    }

    handleEvent(message);
  });

  ws.addEventListener('close', () => {
    socketState.textContent = 'closed';
  });

  ws.addEventListener('error', () => {
    socketState.textContent = 'error';
  });

  await new Promise<void>((resolve, reject) => {
    ws.addEventListener('open', () => resolve(), { once: true });
    ws.addEventListener('error', () => reject(new Error('WebSocket failed')), {
      once: true
    });
  });

  return ws;
}

function handleEvent(message: ResponseMessage): void {
  if (message.event === 'roleAssigned') {
    const assignment = message.data as {
      peerId: string;
      role: PeerRole;
      consumerCount: number;
    };

    role = assignment.role;
    roleState.textContent = assignment.role;
    peerIdLabel.textContent = assignment.peerId;
    consumerCount.textContent = String(assignment.consumerCount);
    writeLog(`assigned as ${assignment.role}`);
    return;
  }

  if (message.event === 'roomState') {
    const state = message.data as {
      consumerCount: number;
      injectedChunks: number;
      injectedPackets: number;
    };

    consumerCount.textContent = String(state.consumerCount);
    packetCount.textContent = String(state.injectedPackets);

    if (role === 'consumer') {
      chunkCount.textContent = String(state.injectedChunks);
    }
    return;
  }

  if (message.event === 'pipelineStats') {
    const stats = message.data as {
      injectedChunks: number;
      injectedPackets: number;
      lastChunkType: string;
      consumerCount: number;
    };

    packetCount.textContent = String(stats.injectedPackets);
    consumerCount.textContent = String(stats.consumerCount);
    writeLog(
      `server injected ${stats.injectedChunks} chunks, last=${stats.lastChunkType}`
    );
    return;
  }

  if (message.event === 'transportState') {
    writeLog(`transport ${JSON.stringify(message.data)}`);
    return;
  }

  if (message.event === 'serverStats') {
    writeLog(summarizeServerStats(message.data));
    return;
  }

  if (message.event === 'producerClosed') {
    consumerState.textContent = 'producer closed';
    writeLog('producer disconnected; refresh to rejoin the room');
  }
}

async function waitForRoleAssignment(): Promise<{
  peerId: string;
  role: PeerRole;
}> {
  for (let attempt = 0; attempt < 50; attempt++) {
    if (role) {
      return {
        peerId: peerIdLabel.textContent ?? '-',
        role
      };
    }

    await new Promise(resolve => setTimeout(resolve, 100));
  }

  throw new Error('Timed out waiting for role assignment');
}

async function setupConsumer(): Promise<void> {
  consumerState.textContent = 'loading';

  const routerRtpCapabilities =
    await request<MediasoupClientTypes.RtpCapabilities>(
      'getRouterRtpCapabilities'
    );
  const device = new Device();

  await device.load({ routerRtpCapabilities });

  const transportOptions =
    await request<MediasoupClientTypes.TransportOptions>(
      'createConsumerTransport'
    );
  const recvTransport = device.createRecvTransport(transportOptions);

  recvTransport.on('connectionstatechange', state => {
    writeLog(`recv transport ${state}`);
  });

  recvTransport.on('connect', ({ dtlsParameters }, callback, errback) => {
    request('connectConsumerTransport', { dtlsParameters })
      .then(() => callback())
      .catch(error => errback(error as Error));
  });

  const consumerOptions = await request<{
    id: string;
    producerId: string;
    kind: MediasoupClientTypes.MediaKind;
    rtpParameters: MediasoupClientTypes.RtpParameters;
  }>('consume', { rtpCapabilities: device.rtpCapabilities });

  const consumer = await recvTransport.consume(consumerOptions);
  const remoteStream = new MediaStream([consumer.track]);

  consumer.track.addEventListener('unmute', () => {
    writeLog('consumer track unmuted');
    void remoteVideo.play().catch(error => {
      writeLog(`remote video play failed: ${error.message}`);
    });
  });
  consumer.track.addEventListener('mute', () => {
    writeLog('consumer track muted');
  });

  remoteVideo.srcObject = remoteStream;
  remoteVideo.muted = true;
  remoteVideo.playsInline = true;

  await request('resumeConsumer');
  void remoteVideo.play().catch(error => {
    writeLog(`remote video play failed: ${error.message}`);
  });
  pollConsumerStats(recvTransport);

  consumerState.textContent = 'receiving';
}

function pollConsumerStats(
  recvTransport: MediasoupClientTypes.Transport
): void {
  window.setInterval(async () => {
    const report = await recvTransport.getStats();

    for (const stat of report.values()) {
      if (stat.type !== 'inbound-rtp' || stat.kind !== 'video') {
        continue;
      }

      rtpReceived.textContent = String(stat.packetsReceived ?? 0);
      framesDecoded.textContent = String(stat.framesDecoded ?? 0);
      break;
    }
  }, 1000);
}

async function startEncoder(stream: MediaStream): Promise<void> {
  const track = stream.getVideoTracks()[0];

  if (!track) {
    throw new Error('camera stream has no video track');
  }

  const Processor = window.MediaStreamTrackProcessor;
  const processor = new Processor({ track });
  const reader = processor.readable.getReader();
  const settings = track.getSettings();
  const width = settings.width ?? 640;
  const height = settings.height ?? 360;
  const frameRate = settings.frameRate ?? 30;
  const canvas = new OffscreenCanvas(width, height);
  const context = canvas.getContext('2d');
  let frameIndex = 0;

  if (!context) {
    throw new Error('OffscreenCanvas 2D context is unavailable');
  }

  const ctx = context;

  const config: VideoEncoderConfig = {
    codec: 'avc1.42E01F',
    width,
    height,
    framerate: frameRate,
    bitrate: 1_200_000,
    latencyMode: 'realtime',
    hardwareAcceleration: 'prefer-hardware',
    avc: { format: 'avc' }
  };
  const support = await VideoEncoder.isConfigSupported(config);

  if (!support.supported) {
    throw new Error(`VideoEncoder config is not supported: ${JSON.stringify(config)}`);
  }

  const encoder = new VideoEncoder({
    output: (chunk, metadata) => {
      sendEncodedChunk(chunk, metadata);
    },
    error: error => {
      writeLog(`encoder error: ${error.message}`);
    }
  });

  encoder.configure(support.config ?? config);
  writeLog(`encoder configured: ${width}x${height}@${Math.round(frameRate)}`);

  void pumpFrames();

  async function pumpFrames(): Promise<void> {
    while (true) {
      const { done, value } = await reader.read();

      if (done || !value) {
        break;
      }

      encodeFrame(value);
    }
  }

  function encodeFrame(frame: VideoFrame): void {
    frameIndex++;

    let processedFrame: VideoFrame | undefined;

    try {
      const frameWidth = frame.displayWidth || frame.codedWidth || width;
      const frameHeight = frame.displayHeight || frame.codedHeight || height;

      if (canvas.width !== frameWidth || canvas.height !== frameHeight) {
        canvas.width = frameWidth;
        canvas.height = frameHeight;
      }

      ctx.drawImage(frame, 0, 0, frameWidth, frameHeight);
      ctx.fillStyle = 'rgba(10, 92, 120, 0.78)';
      ctx.fillRect(14, 14, 250, 72);
      ctx.fillStyle = '#ffffff';
      ctx.font = '20px sans-serif';
      ctx.fillText('1:N Producer', 24, 46);
      ctx.font = '14px sans-serif';
      ctx.fillText(`encoded frame ${frameIndex}`, 24, 70);

      processedFrame = new VideoFrame(
        canvas,
        frame.duration === null
          ? { timestamp: frame.timestamp }
          : { timestamp: frame.timestamp, duration: frame.duration }
      );

      encoder.encode(processedFrame, {
        keyFrame: frameIndex % Math.max(1, Math.floor(frameRate * 2)) === 1
      });
    } finally {
      frame.close();
      processedFrame?.close();
    }
  }
}

function sendEncodedChunk(
  chunk: EncodedVideoChunk,
  metadata?: EncodedVideoChunkMetadata
): void {
  if (!socket || socket.readyState !== WebSocket.OPEN) {
    return;
  }

  const data = new Uint8Array(chunk.byteLength);
  const description = metadata?.decoderConfig?.description;
  const header = {
    event: 'encodedChunk',
    timestamp: chunk.timestamp,
    type: chunk.type as BrowserChunkType,
    duration: chunk.duration,
    metadata: description
      ? {
          decoderConfig: {
            codec: metadata.decoderConfig?.codec,
            descriptionBase64: arrayBufferToBase64(description)
          }
        }
      : undefined
  };

  chunk.copyTo(data);

  const headerBytes = new TextEncoder().encode(JSON.stringify(header));
  const packet = new Uint8Array(4 + headerBytes.byteLength + data.byteLength);
  const view = new DataView(packet.buffer);

  view.setUint32(0, headerBytes.byteLength);
  packet.set(headerBytes, 4);
  packet.set(data, 4 + headerBytes.byteLength);
  socket.send(packet);

  encodedChunks++;
  chunkCount.textContent = String(encodedChunks);
}

function request<T>(action: string, data?: unknown): Promise<T> {
  const id = String(++requestSeq);

  socket.send(JSON.stringify({ id, action, data }));

  return new Promise<T>((resolve, reject) => {
    pending.set(id, {
      resolve: value => resolve(value as T),
      reject
    });
  });
}

function assertWebCodecsSupport(): void {
  if (
    !('VideoEncoder' in window) ||
    !('VideoFrame' in window) ||
    !('MediaStreamTrackProcessor' in window)
  ) {
    throw new Error('This browser does not expose the required WebCodecs APIs');
  }
}

function arrayBufferToBase64(buffer: AllowSharedBufferSource): string {
  const bytes =
    buffer instanceof ArrayBuffer
      ? new Uint8Array(buffer)
      : new Uint8Array(buffer as SharedArrayBuffer);
  let binary = '';

  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }

  return btoa(binary);
}

function writeLog(line: string): void {
  const time = new Date().toLocaleTimeString();

  logBox.textContent = `[${time}] ${line}\n${logBox.textContent}`;
}

function summarizeServerStats(data: unknown): string {
  const stats = data as {
    producer?: Array<Record<string, unknown>>;
    consumer?: Array<Record<string, unknown>>;
    transport?: Array<Record<string, unknown>>;
  };
  const producer = stats.producer?.[0];
  const consumer = stats.consumer?.[0];
  const transport = stats.transport?.[0];
  const producerPackets =
    producer?.packetCount ?? producer?.packetsReceived ?? producer?.packetsSent;
  const consumerPackets =
    consumer?.packetCount ?? consumer?.packetsSent ?? consumer?.packetsReceived;
  const transportSent = transport?.rtpBytesSent ?? transport?.bytesSent;

  return `server stats: producerPackets=${String(
    producerPackets ?? 'n/a'
  )}, consumerPackets=${String(
    consumerPackets ?? 'n/a'
  )}, transportSent=${String(transportSent ?? 'n/a')}`;
}

declare global {
  interface Window {
    MediaStreamTrackProcessor: new (init: {
      track: MediaStreamTrack;
    }) => {
      readable: ReadableStream<VideoFrame>;
    };
  }
}
