# WebCodecs mediasoup 1:N

## 한국어

이 프로젝트는 브라우저의 WebCodecs API로 카메라 영상을 H.264로 인코딩한 뒤, 로컬 mediasoup 파이프라인을 통해 1:N 구조로 전달하는 예제입니다. 첫 번째로 접속한 브라우저는 producer가 되어 카메라 영상을 송신하고, 이후 접속한 브라우저들은 consumer가 되어 같은 스트림을 수신합니다.

## 주요 기능

- WebCodecs `VideoEncoder`를 사용한 실시간 H.264 인코딩
- WebSocket 기반 signaling
- mediasoup WebRTC transport를 통한 consumer 수신
- producer/consumer 역할 자동 할당
- 연결 상태, RTP 패킷, 디코딩 프레임 수 등 기본 통계 표시
- 브라우저 로그는 최근 50줄만 유지하고, 파이프라인 상태는 초당 1회로 압축 전송

## 요구 사항

- Node.js
- npm
- WebCodecs와 `MediaStreamTrackProcessor`를 지원하는 브라우저
- 카메라 접근 권한
- 프로젝트 상위 폴더에 빌드된 `mediasoup` 소스/산출물

서버는 다음 로컬 mediasoup 경로를 참조합니다.

```text
../mediasoup/node/lib/index.js
../mediasoup/node/lib/webCodecsPipeline.js
../mediasoup/worker/out/Release/mediasoup-worker.exe
```

Windows가 아닌 환경에서는 worker 실행 파일 이름이 `mediasoup-worker`입니다.

## 실행 방법

1. 의존성을 설치합니다.

```bash
npm install
```

2. 개발 서버를 실행합니다.

```bash
npm run dev
```

이 명령은 signaling 서버와 Vite 클라이언트를 동시에 실행합니다.

- signaling 서버: `http://127.0.0.1:3000`
- Vite 클라이언트: `http://127.0.0.1:5173`
- WebSocket 경로: `/ws`

3. 브라우저에서 `http://127.0.0.1:5173`에 접속한 뒤 `Start` 버튼을 누릅니다.

4. 첫 번째 브라우저 탭 또는 창은 producer가 됩니다. 카메라 권한을 허용하면 WebCodecs로 인코딩된 영상이 서버로 전송됩니다.

5. 같은 주소를 다른 브라우저 탭 또는 창에서 다시 열고 `Start`를 누르면 consumer로 접속하여 producer의 영상을 수신합니다.

## 기타 명령어

```bash
npm run server
```

signaling 서버만 실행합니다.

```bash
npm run client
```

Vite 클라이언트만 실행합니다.

```bash
npm run build
```

TypeScript 타입 검사와 Vite 프로덕션 빌드를 실행합니다.

```bash
npm run preview
```

빌드 결과물을 Vite preview 서버로 확인합니다.

## 참고 사항

- 서버는 기본적으로 `127.0.0.1:3000`에서 실행됩니다. `PORT` 환경 변수로 포트를 변경할 수 있습니다.
- mediasoup worker는 RTP 포트 범위 `40000-40100`을 사용합니다.
- producer가 종료되면 consumer는 producer 종료 이벤트를 받고, 새 producer로 다시 참여하려면 페이지를 새로고침해야 합니다.
- 브라우저에서 WebCodecs H.264 인코딩을 지원하지 않으면 producer 시작에 실패할 수 있습니다.

---

## English

This project is a 1:N streaming example that encodes camera video with the browser WebCodecs API as H.264, then forwards it through a local mediasoup pipeline. The first browser that joins becomes the producer and sends camera video. Later browsers become consumers and receive the same stream.

## Key Features

- Real-time H.264 encoding with the WebCodecs `VideoEncoder`
- WebSocket-based signaling
- Consumer receiving through mediasoup WebRTC transports
- Automatic producer/consumer role assignment
- Basic status and stats display, including connection state, RTP packets, and decoded frames
- Browser logs retain only the latest 50 lines, and pipeline telemetry is compacted to one update per second

## Requirements

- Node.js
- npm
- A browser that supports WebCodecs and `MediaStreamTrackProcessor`
- Camera permission
- A built `mediasoup` source/output directory in the parent folder of this project

The server references the following local mediasoup paths.

```text
../mediasoup/node/lib/index.js
../mediasoup/node/lib/webCodecsPipeline.js
../mediasoup/worker/out/Release/mediasoup-worker.exe
```

On non-Windows environments, the worker executable name is `mediasoup-worker`.

## How to Run

1. Install dependencies.

```bash
npm install
```

2. Start the development servers.

```bash
npm run dev
```

This command starts both the signaling server and the Vite client.

- signaling server: `http://127.0.0.1:3000`
- Vite client: `http://127.0.0.1:5173`
- WebSocket path: `/ws`

3. Open `http://127.0.0.1:5173` in a browser and click the `Start` button.

4. The first browser tab or window becomes the producer. After camera permission is granted, WebCodecs-encoded video is sent to the server.

5. Open the same address in another browser tab or window and click `Start` again. It joins as a consumer and receives the producer video.

## Other Commands

```bash
npm run server
```

Starts only the signaling server.

```bash
npm run client
```

Starts only the Vite client.

```bash
npm run build
```

Runs TypeScript type checking and creates a Vite production build.

```bash
npm run preview
```

Serves the built output with the Vite preview server.

## Notes

- The server runs on `127.0.0.1:3000` by default. You can change it with the `PORT` environment variable.
- The mediasoup worker uses RTP ports `40000-40100`.
- If the producer closes, consumers receive a producer-closed event. Refresh the page to join again with a new producer.
- Producer startup can fail if the browser does not support WebCodecs H.264 encoding.
