import { Device } from 'mediasoup-client';
import type { types as MediasoupClientTypes } from 'mediasoup-client';
import './styles.css';

// 피어 역할 타입 (방송 송신자 또는 시청 수신자)
type PeerRole = 'producer' | 'consumer';

// WebSocket 응답 메시지 규격
type ResponseMessage = {
  id?: string;
  ok?: boolean;
  data?: unknown;
  error?: string;
  event?: string;
};

// 소켓 비동기 요청 대기 매핑용
type PendingRequest = {
  resolve: (data: unknown) => void;
  reject: (error: Error) => void;
};

type BrowserChunkType = 'key' | 'delta';

// DOM 요소 획득
const startButton = document.querySelector<HTMLButtonElement>('#startButton')!;
const dumpButton = document.querySelector<HTMLButtonElement>('#dumpButton')!;
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
const simulcastState = document.querySelector<HTMLElement>('#simulcastState')!;
const layerControls = document.querySelector<HTMLElement>('#layerControls')!;
const layerHighBtn = document.querySelector<HTMLButtonElement>('#layerHighBtn')!;
const layerLowBtn = document.querySelector<HTMLButtonElement>('#layerLowBtn')!;
const activeLayerInfo = document.querySelector<HTMLElement>('#activeLayerInfo')!;
const logBox = document.querySelector<HTMLPreElement>('#log')!;

// 통계 데이터 덤프 규격 (webrtc-internals style custom stats)
interface ProducerStatSample {
  timestamp: number;
  timeIso: string;
  fps: number;
  bitrateKbps: number;
  encodedChunksDelta: number;
  totalEncodedChunks: number;
  totalBytesSent: number;
  keyFrames: number;
  deltaFrames: number;
  encodeQueueSize: number;
  socketBufferedAmount: number;
  simulcastLayers?: {
    high: { chunks: number; bytes: number; fps: number };
    low: { chunks: number; bytes: number; fps: number };
  };
  memoryUsedMb?: number | undefined;
  eventLoopLagMs?: number | undefined;
}

interface ConsumerStatSample {
  timestamp: number;
  timeIso: string;
  packetsReceived?: number;
  packetsLost?: number;
  jitter?: number;
  framesDecoded?: number;
  framesDropped?: number;
  framesPerSecond?: number;
  bytesReceived?: number;
  bitrateKbps?: number;
  memoryUsedMb?: number | undefined;
  eventLoopLagMs?: number | undefined;
}

interface ServerEventLogEntry {
  timeIso: string;
  event: string;
}

interface StatsDumpFile {
  metadata: {
    peerId: string;
    role: PeerRole | 'unknown';
    startTime: string;
    dumpTime: string;
    sampleIntervalMs: number;
    userAgent: string;
  };
  producerStats?: ProducerStatSample[] | undefined;
  consumerStats?: ConsumerStatSample[] | undefined;
  serverEvents?: ServerEventLogEntry[] | undefined;
}

// 시퀀스 및 상태 변수
let requestSeq = 0;
let encodedChunks = 0;
let socket: WebSocket;
let role: PeerRole | undefined;
const pending = new Map<string, PendingRequest>();

// 통계 수집기 상태
const startTimeIso = new Date().toISOString();
const producerStatsHistory: ProducerStatSample[] = [];
const consumerStatsHistory: ConsumerStatSample[] = [];
const serverEventsLog: ServerEventLogEntry[] = [];
const MAX_LOG_LINES = 50;
const MAX_SERVER_EVENT_ENTRIES = 100;

let activeEncoderHigh: VideoEncoder | null = null;
let activeEncoderLow: VideoEncoder | null = null;
let currentPreferredSpatialLayer = 1; // 1: High (720p), 0: Low (360p)

let recentChunksCount = 0;
let recentBytesSent = 0;
let totalBytesSent = 0;
let recentKeyFrames = 0;
let recentDeltaFrames = 0;
let recentChunksLayer0 = 0;
let recentChunksLayer1 = 0;
let recentBytesLayer0 = 0;
let recentBytesLayer1 = 0;

let lastConsumerBytesReceived = 0;
let lastConsumerTimestamp = 0;
let lastConsumerFramesDecoded = 0;
let producerSamplingTimer: number | null = null;

// 'Start' 버튼 클릭 시 연결 시작
startButton.addEventListener('click', () => {
  startButton.disabled = true;
  start().catch(error => {
    startButton.disabled = false;
    writeLog(`failed: ${error instanceof Error ? error.message : String(error)}`);
  });
});

// 'Dump Stats' 버튼 클릭 시 통계 JSON 파일 다운로드
dumpButton.addEventListener('click', () => {
  exportStatsDump();
});

// 시뮬캐스트 화질 전환 핸들러 (Consumer)
async function selectSimulcastLayer(spatialLayer: number): Promise<void> {
  currentPreferredSpatialLayer = spatialLayer;
  if (spatialLayer === 1) {
    layerHighBtn.classList.add('active');
    layerLowBtn.classList.remove('active');
    activeLayerInfo.textContent = 'Requested: High (720p)';
  } else {
    layerLowBtn.classList.add('active');
    layerHighBtn.classList.remove('active');
    activeLayerInfo.textContent = 'Requested: Low (360p)';
  }

  try {
    await request('setConsumerPreferredLayers', { spatialLayer });
    writeLog(`Requested simulcast spatialLayer: ${spatialLayer}`);
  } catch (err) {
    writeLog(`Failed to change layer: ${err instanceof Error ? err.message : String(err)}`);
  }
}

layerHighBtn.addEventListener('click', () => {
  void selectSimulcastLayer(1);
});

layerLowBtn.addEventListener('click', () => {
  void selectSimulcastLayer(0);
});

/**
 * 앱 시작 메인 함수
 * 1. WebSocket 연결 수립
 * 2. 서버로부터 할당받은 역할(Producer / Consumer) 대기 및 확인
 * 3. 역할별 파이프라인 시작
 */
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

/**
 * Producer (방송 송신자) 실행 함수
 * 1. WebCodecs 지원 여부 검증
 * 2. getUserMedia() 카메라 캡처 시작 (640x360 @ 30fps)
 * 3. WebCodecs 인코더 파이프라인 시작 (MediaStreamTrackProcessor + VideoEncoder)
 */
async function startProducer(): Promise<void> {
  assertWebCodecsSupport();

  remoteVideo.removeAttribute('src');
  remoteVideo.srcObject = null;
  remoteCaption.textContent = 'Consumers receive this stream';
  localCaption.textContent = 'Producer camera preview';
  consumerState.textContent = 'producer';

  // 카메라 스트림 획득
  const stream = await navigator.mediaDevices.getDisplayMedia({
    video: {
      width: { ideal: 1280 },
      height: { ideal: 720 },
      frameRate: { ideal: 30, max: 30 }
    },
    audio: false
  });

  localVideo.srcObject = stream;
  await startEncoder(stream);
  writeLog('producer pipeline started');
}

/**
 * Consumer (시청 수신자) 실행 함수
 * - mediasoup-client 디바이스 로드 및 WebRTC 수신 트랜스포트 설정
 */
async function startConsumer(): Promise<void> {
  localVideo.removeAttribute('src');
  localVideo.srcObject = null;
  localCaption.textContent = 'Consumer peer';
  remoteCaption.textContent = 'Producer stream';

  await setupConsumer();
  writeLog('consumer connected');
}

/**
 * WebSocket 시그널링 서버 연결 함수
 */
async function connectSocket(): Promise<WebSocket> {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const ws = new WebSocket(`${protocol}//${window.location.host}/ws`);

  ws.binaryType = 'arraybuffer';

  // 서버로부터의 메시지 수신 처리
  ws.addEventListener('message', event => {
    if (typeof event.data !== 'string') {
      return;
    }

    const message = JSON.parse(event.data) as ResponseMessage;

    // 요청-응답 비동기 매핑 (ID 매칭)
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

    // 서버 푸시 이벤트 처리
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

/**
 * 서버 푸시 이벤트(역할 할당, 룸 상태 변경, 압축된 파이프라인 통계 등) 처리
 */
function handleEvent(message: ResponseMessage): void {
  if (message.event && isLifecycleEvent(message.event)) {
    serverEventsLog.push({
      timeIso: new Date().toISOString(),
      event: message.event
    });
    if (serverEventsLog.length > MAX_SERVER_EVENT_ENTRIES) {
      serverEventsLog.shift();
    }
  }

  if (message.event === 'roleAssigned') {
    const assignment = message.data as {
      peerId: string;
      role: PeerRole;
      consumerCount: number;
      simulcast?: boolean;
    };

    role = assignment.role;
    roleState.textContent = assignment.role;
    peerIdLabel.textContent = assignment.peerId;
    consumerCount.textContent = String(assignment.consumerCount);

    if (role === 'producer') {
      simulcastState.textContent = '2 Layers (720p/360p)';
      layerControls.style.display = 'none';
    } else {
      simulcastState.textContent = 'Active (Receiver)';
      layerControls.style.display = 'flex';
    }

    writeLog(`assigned as ${assignment.role} (simulcast: ${assignment.simulcast ? 'yes' : 'no'})`);
    return;
  }

  if (message.event === 'consumerLayersChanged') {
    const layers = message.data as { spatialLayer?: number; temporalLayer?: number } | undefined;
    const sl = layers?.spatialLayer;
    const label = sl === 1 ? 'High (720p)' : sl === 0 ? 'Low (360p)' : 'Switching...';
    activeLayerInfo.textContent = `Active: ${label}`;
    writeLog(`[simulcast] Consumer layer changed: ${label} (spatial: ${sl ?? 'none'})`);
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
    if (role === 'consumer') {
      chunkCount.textContent = String(stats.injectedChunks);
    }
    return;
  }


  if (message.event === 'producerClosed') {
    consumerState.textContent = 'producer closed';
    writeLog('producer disconnected; refresh to rejoin the room');
  }
}

/**
 * 서버로부터 역할(producer/consumer) 지정 응답을 기다리는 헬퍼 함수
 */
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

/**
 * Consumer 전용 WebRTC 트랜스포트 설정 및 비디오 재생 함수
 * 1. getRouterRtpCapabilities 요청 ➔ mediasoup-client Device 로드
 * 2. createConsumerTransport 요청 ➔ RecvTransport 생성 (DTLS 이벤트 연결)
 * 3. consume 요청 ➔ Consumer 트랙 수신
 * 4. remoteVideo 태그에 MediaStream 바인딩 및 resumeConsumer 요청으로 재생
 */
async function setupConsumer(): Promise<void> {
  consumerState.textContent = 'loading';

  // 1. Router의 RTP 역량 가져오기
  const routerRtpCapabilities =
    await request<MediasoupClientTypes.RtpCapabilities>(
      'getRouterRtpCapabilities'
    );
  const device = new Device();

  // 2. mediasoup-client 디바이스 로드
  await device.load({ routerRtpCapabilities });

  // 3. 수신 전용 WebRTC Transport 생성
  const transportOptions =
    await request<MediasoupClientTypes.TransportOptions>(
      'createConsumerTransport'
    );
  const recvTransport = device.createRecvTransport(transportOptions);

  recvTransport.on('connectionstatechange', state => {
    writeLog(`recv transport ${state}`);
  });

  // DTLS 파라미터 연결 시그널링
  recvTransport.on('connect', ({ dtlsParameters }, callback, errback) => {
    request('connectConsumerTransport', { dtlsParameters })
      .then(() => callback())
      .catch(error => errback(error as Error));
  });

  // 4. Consumer 객체 요청 및 생성
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

  // 비디오 태그에 미디어 스트림 바인딩
  remoteVideo.srcObject = remoteStream;
  remoteVideo.muted = true;
  remoteVideo.playsInline = true;

  // 5. Consumer 일시정지 해제 및 기본 화질(High) 지정
  await request('resumeConsumer');
  try {
    await request('setConsumerPreferredLayers', {
      spatialLayer: currentPreferredSpatialLayer
    });
  } catch (err) {
    writeLog(`Failed to set initial preferred layer: ${err instanceof Error ? err.message : String(err)}`);
  }

  void remoteVideo.play().catch(error => {
    writeLog(`remote video play failed: ${error.message}`);
  });
  pollConsumerStats(recvTransport);

  consumerState.textContent = 'receiving';
}

/**
 * 1초 주기로 Consumer WebRTC 패킷 및 디코딩 프레임 수 수집
 */
function pollConsumerStats(
  recvTransport: MediasoupClientTypes.Transport
): void {
  window.setInterval(async () => {
    const report = await recvTransport.getStats();

    let targetStat: Record<string, unknown> | null = null;

    // RTX 트랙이 아닌 주 비디오 수신 트랙(inbound-rtp) 탐색
    for (const stat of report.values()) {
      if (stat.type !== 'inbound-rtp' || stat.kind !== 'video') {
        continue;
      }

      // framesDecoded가 존재하는 주 스트림 우선 선택
      if (typeof stat.framesDecoded === 'number' && stat.framesDecoded > 0) {
        targetStat = stat as Record<string, unknown>;
        break;
      }

      // 가장 많은 패킷/바이트를 수신한 메인 비디오 스트림 선택 (RTX 제외)
      if (
        !targetStat ||
        ((stat.bytesReceived as number) ?? 0) > ((targetStat.bytesReceived as number) ?? 0)
      ) {
        targetStat = stat as Record<string, unknown>;
      }
    }

    if (!targetStat) {
      return;
    }

    const videoQuality = remoteVideo.getVideoPlaybackQuality
      ? remoteVideo.getVideoPlaybackQuality()
      : null;

    const packetsReceived = (targetStat.packetsReceived as number) ?? 0;
    const packetsLost = (targetStat.packetsLost as number) ?? 0;
    const jitter = (targetStat.jitter as number) ?? 0;
    const bytesReceived = (targetStat.bytesReceived as number) ?? 0;

    // WebRTC getStats 또는 <video> HTML5 Playback Quality fallback
    let framesDecodedCount = (targetStat.framesDecoded as number) ?? 0;
    if (framesDecodedCount === 0 && videoQuality && videoQuality.totalVideoFrames > 0) {
      framesDecodedCount = videoQuality.totalVideoFrames;
    }

    rtpReceived.textContent = String(packetsReceived);
    framesDecoded.textContent = String(framesDecodedCount);

    const now = Date.now();
    let bitrateKbps = 0;
    let fps = (targetStat.framesPerSecond as number) ?? 0;

    if (lastConsumerTimestamp > 0 && now > lastConsumerTimestamp) {
      const timeDiffSec = (now - lastConsumerTimestamp) / 1000;
      const bytesDiff = bytesReceived - lastConsumerBytesReceived;
      bitrateKbps = Math.round(((bytesDiff * 8) / 1000) / timeDiffSec);

      if (fps === 0 && timeDiffSec > 0 && lastConsumerFramesDecoded > 0) {
        fps = Math.round((framesDecodedCount - lastConsumerFramesDecoded) / timeDiffSec);
      }
    }

    lastConsumerBytesReceived = bytesReceived;
    lastConsumerFramesDecoded = framesDecodedCount;
    lastConsumerTimestamp = now;

    consumerStatsHistory.push({
      timestamp: now,
      timeIso: new Date().toISOString(),
      packetsReceived,
      packetsLost,
      jitter,
      framesDecoded: framesDecodedCount,
      framesDropped: (targetStat.framesDropped as number) ?? (videoQuality?.droppedVideoFrames ?? 0),
      framesPerSecond: Math.max(0, fps),
      bytesReceived,
      bitrateKbps,
      memoryUsedMb: getMemoryUsageMb()
    });
  }, 1000);
}

/**
 * Producer 전용 WebCodecs 시뮬캐스트(Simulcast) 인코딩 파이프라인 함수
 * 1. MediaStreamTrackProcessor로 프레임(VideoFrame) 리더 생성
 * 2. High (1280x720 @ 1.5Mbps) 및 Low (640x360 @ 350kbps) 듀얼 OffscreenCanvas 준비
 * 3. 듀얼 VideoEncoder (avc1.42E01F) 실시간 H.264 인코더 구성
 * 4. pumpFrames 루프에서 동일한 타임스탬프로 High/Low 동시 인코딩 및 키프레임 동기화
 */
async function startEncoder(stream: MediaStream): Promise<void> {
  const track = stream.getVideoTracks()[0];

  if (!track) {
    throw new Error('camera stream has no video track');
  }

  // 1. 카메라 Track에서 비디오 프레임 추출 스트림 생성
  const Processor = window.MediaStreamTrackProcessor;
  const processor = new Processor({ track });
  const reader = processor.readable.getReader();
  const settings = track.getSettings();

  const widthHigh = settings.width ?? 1280;
  const heightHigh = settings.height ?? 720;
  const widthLow = Math.max(320, Math.floor(widthHigh / 2) & ~1);
  const heightLow = Math.max(180, Math.floor(heightHigh / 2) & ~1);
  const frameRate = settings.frameRate ?? 30;

  // 2. High / Low 오프스크린 캔버스 및 2D 컨텍스트 준비
  const canvasHigh = new OffscreenCanvas(widthHigh, heightHigh);
  const rawCtxHigh = canvasHigh.getContext('2d');
  const canvasLow = new OffscreenCanvas(widthLow, heightLow);
  const rawCtxLow = canvasLow.getContext('2d');

  if (!rawCtxHigh || !rawCtxLow) {
    throw new Error('OffscreenCanvas 2D context is unavailable');
  }

  const ctxHigh: OffscreenCanvasRenderingContext2D = rawCtxHigh;
  const ctxLow: OffscreenCanvasRenderingContext2D = rawCtxLow;

  let frameIndex = 0;

  // 3. H.264 High Layer (Layer 1) 설정
  const configHigh: VideoEncoderConfig = {
    codec: 'avc1.42E01F',
    width: widthHigh,
    height: heightHigh,
    framerate: frameRate,
    bitrate: 1_500_000,
    latencyMode: 'realtime',
    hardwareAcceleration: 'prefer-hardware',
    avc: { format: 'avc' }
  };

  // 4. H.264 Low Layer (Layer 0) 설정
  const configLow: VideoEncoderConfig = {
    codec: 'avc1.42E01F',
    width: widthLow,
    height: heightLow,
    framerate: frameRate,
    bitrate: 350_000,
    latencyMode: 'realtime',
    hardwareAcceleration: 'prefer-hardware',
    avc: { format: 'avc' }
  };

  const [supportHigh, supportLow] = await Promise.all([
    VideoEncoder.isConfigSupported(configHigh),
    VideoEncoder.isConfigSupported(configLow)
  ]);

  if (!supportHigh.supported || !supportLow.supported) {
    throw new Error('VideoEncoder does not support the simulcast configuration');
  }

  // 5. High 인코더 (Layer 1)
  const encoderHigh = new VideoEncoder({
    output: (chunk, metadata) => {
      sendEncodedChunk(1, chunk, metadata);
    },
    error: error => {
      writeLog(`encoder (high) error: ${error.message}`);
    }
  });
  encoderHigh.configure(supportHigh.config ?? configHigh);
  activeEncoderHigh = encoderHigh;

  // 6. Low 인코더 (Layer 0)
  const encoderLow = new VideoEncoder({
    output: (chunk, metadata) => {
      sendEncodedChunk(0, chunk, metadata);
    },
    error: error => {
      writeLog(`encoder (low) error: ${error.message}`);
    }
  });
  encoderLow.configure(supportLow.config ?? configLow);
  activeEncoderLow = encoderLow;

  startProducerStatsSampling();
  writeLog(
    `simulcast encoders configured: High ${widthHigh}x${heightHigh} (1.5M), Low ${widthLow}x${heightLow} (350k) @ ${Math.round(frameRate)}fps`
  );

  void pumpFrames();

  // 리더에서 프레임 순차적으로 읽어서 인코딩
  async function pumpFrames(): Promise<void> {
    while (true) {
      const { done, value } = await reader.read();

      if (done || !value) {
        break;
      }

      encodeFrame(value);
    }
  }

  // Canvas 그래픽 오버레이 처리 후 High/Low 인코더 동시 전달
  function encodeFrame(frame: VideoFrame): void {
    frameIndex++;

    let frameHigh: VideoFrame | undefined;
    let frameLow: VideoFrame | undefined;

    try {
      const isKey = frameIndex % Math.max(1, Math.floor(frameRate * 2)) === 1;

      // --- High Layer 렌더링 ---
      ctxHigh.drawImage(frame, 0, 0, widthHigh, heightHigh);
      ctxHigh.fillStyle = 'rgba(10, 92, 120, 0.85)';
      ctxHigh.fillRect(14, 14, 280, 72);
      ctxHigh.fillStyle = '#ffffff';
      ctxHigh.font = 'bold 20px sans-serif';
      ctxHigh.fillText('Producer [High 720p]', 24, 46);
      ctxHigh.font = '14px sans-serif';
      ctxHigh.fillText(`frame ${frameIndex} | 1.5 Mbps`, 24, 70);

      frameHigh = new VideoFrame(
        canvasHigh,
        frame.duration === null
          ? { timestamp: frame.timestamp }
          : { timestamp: frame.timestamp, duration: frame.duration }
      );
      encoderHigh.encode(frameHigh, { keyFrame: isKey });

      // --- Low Layer 렌더링 ---
      ctxLow.drawImage(frame, 0, 0, widthLow, heightLow);
      ctxLow.fillStyle = 'rgba(30, 41, 59, 0.85)';
      ctxLow.fillRect(10, 10, 210, 56);
      ctxLow.fillStyle = '#38bdf8';
      ctxLow.font = 'bold 15px sans-serif';
      ctxLow.fillText('Producer [Low 360p]', 18, 34);
      ctxLow.font = '12px sans-serif';
      ctxLow.fillStyle = '#e2e8f0';
      ctxLow.fillText(`frame ${frameIndex} | 350 kbps`, 18, 52);

      frameLow = new VideoFrame(
        canvasLow,
        frame.duration === null
          ? { timestamp: frame.timestamp }
          : { timestamp: frame.timestamp, duration: frame.duration }
      );
      encoderLow.encode(frameLow, { keyFrame: isKey });
    } finally {
      frame.close();
      frameHigh?.close();
      frameLow?.close();
    }
  }
}

/**
 * Producer 통계 1초 주기 샘플링 타이머 시작
 */
function startProducerStatsSampling(): void {
  if (producerSamplingTimer !== null) return;

  producerSamplingTimer = window.setInterval(() => {
    if (!role || role !== 'producer') return;

    const now = Date.now();

    const stat: ProducerStatSample = {
      timestamp: now,
      timeIso: new Date().toISOString(),
      fps: recentChunksCount,
      bitrateKbps: Math.round((recentBytesSent * 8) / 1000),
      encodedChunksDelta: recentChunksCount,
      totalEncodedChunks: encodedChunks,
      totalBytesSent,
      keyFrames: recentKeyFrames,
      deltaFrames: recentDeltaFrames,
      encodeQueueSize:
        (activeEncoderHigh ? activeEncoderHigh.encodeQueueSize : 0) +
        (activeEncoderLow ? activeEncoderLow.encodeQueueSize : 0),
      socketBufferedAmount: socket ? socket.bufferedAmount : 0,
      simulcastLayers: {
        high: {
          chunks: recentChunksLayer1,
          bytes: recentBytesLayer1,
          fps: recentChunksLayer1
        },
        low: {
          chunks: recentChunksLayer0,
          bytes: recentBytesLayer0,
          fps: recentChunksLayer0
        }
      },
      memoryUsedMb: getMemoryUsageMb()
    };

    producerStatsHistory.push(stat);

    recentChunksCount = 0;
    recentBytesSent = 0;
    recentKeyFrames = 0;
    recentDeltaFrames = 0;
    recentChunksLayer0 = 0;
    recentChunksLayer1 = 0;
    recentBytesLayer0 = 0;
    recentBytesLayer1 = 0;
  }, 1000);
}

/**
 * 브라우저 JS 힙 메모리 사용량 (MB) 측정 헬퍼
 */
function getMemoryUsageMb(): number | undefined {
  const perf = window.performance as unknown as {
    memory?: {
      usedJSHeapSize?: number;
    };
  };

  if (perf.memory?.usedJSHeapSize) {
    return Math.round((perf.memory.usedJSHeapSize / (1024 * 1024)) * 100) / 100;
  }
  return undefined;
}

/**
 * 인코딩된 WebCodecs H.264 청크(EncodedVideoChunk)를 바이너리 패킷으로 송신하는 함수
 * - 패킷 구성: [4바이트 Header 길이] + [JSON Header (SPS/PPS 메타데이터, layer)] + [Raw H.264 Bitstream Data]
 */
function sendEncodedChunk(
  layer: number,
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
    layer,
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

  // JSON 헤더를 UTF-8 바이트 배열로 변환
  const headerBytes = new TextEncoder().encode(JSON.stringify(header));
  const packet = new Uint8Array(4 + headerBytes.byteLength + data.byteLength);
  const view = new DataView(packet.buffer);

  // 헤더 길이 BigEndian Uint32 기록
  view.setUint32(0, headerBytes.byteLength);
  packet.set(headerBytes, 4);
  packet.set(data, 4 + headerBytes.byteLength);

  // WebSocket으로 바이너리 패킷 송신
  socket.send(packet);

  encodedChunks++;
  recentChunksCount++;
  recentBytesSent += chunk.byteLength;
  totalBytesSent += chunk.byteLength;

  if (layer === 1) {
    recentChunksLayer1++;
    recentBytesLayer1 += chunk.byteLength;
  } else {
    recentChunksLayer0++;
    recentBytesLayer0 += chunk.byteLength;
  }

  if (chunk.type === 'key') {
    recentKeyFrames++;
  } else {
    recentDeltaFrames++;
  }
  chunkCount.textContent = String(encodedChunks);
}

/**
 * 통계 데이터를 JSON 덤프 파일로 다운로드 추출
 */
function exportStatsDump(): void {
  const currentPeerId = peerIdLabel.textContent ?? 'unknown';
  const dumpData: StatsDumpFile = {
    metadata: {
      peerId: currentPeerId,
      role: role ?? 'unknown',
      startTime: startTimeIso,
      dumpTime: new Date().toISOString(),
      sampleIntervalMs: 1000,
      userAgent: navigator.userAgent
    },
    producerStats: role === 'producer' ? producerStatsHistory : undefined,
    consumerStats: role === 'consumer' ? consumerStatsHistory : undefined,
    serverEvents: serverEventsLog
  };

  const jsonString = JSON.stringify(dumpData, null, 2);
  const blob = new Blob([jsonString], { type: 'application/json' });
  const url = URL.createObjectURL(blob);

  const timestampStr = new Date()
    .toISOString()
    .replace(/[:.]/g, '-')
    .slice(0, 19);
  const filename = `webrtc-dump-${role ?? 'peer'}-${currentPeerId}-${timestampStr}.json`;

  const link = document.createElement('a');
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);

  URL.revokeObjectURL(url);

  const sampleCount =
    role === 'producer'
      ? producerStatsHistory.length
      : consumerStatsHistory.length;
  writeLog(`dump exported: ${filename} (${sampleCount} samples)`);
}

/**
 * WebSocket 시그널링 요청 프로미스 헬퍼
 */
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

/**
 * 브라우저의 WebCodecs 및 MediaStreamTrackProcessor API 지원 여부 확인
 */
function assertWebCodecsSupport(): void {
  if (
    !('VideoEncoder' in window) ||
    !('VideoFrame' in window) ||
    !('MediaStreamTrackProcessor' in window)
  ) {
    throw new Error('This browser does not expose the required WebCodecs APIs');
  }
}

/**
 * ArrayBuffer ➔ Base64 인코딩 변환 함수 (SPS/PPS 메타데이터 전달용)
 */
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

function isLifecycleEvent(event: string): boolean {
  return event === 'roleAssigned' || event === 'consumerLayersChanged' || event === 'producerClosed';
}

function writeLog(line: string): void {
  const time = new Date().toLocaleTimeString();
  const lines = logBox.textContent ? logBox.textContent.split('\n') : [];

  lines.unshift(`[${time}] ${line}`);
  logBox.textContent = lines.slice(0, MAX_LOG_LINES).join('\n');
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
