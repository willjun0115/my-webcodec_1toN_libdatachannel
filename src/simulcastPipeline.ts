import type * as MediasoupTypes from '../../mediasoup/node/lib/types.js';

const RTP_VERSION = 0x80;
const H264_PAYLOAD_TYPE = 96;
const H264_CLOCK_RATE = 90000;
const H264_MAX_PAYLOAD_SIZE = 1200;
const DEFAULT_H264_PROFILE_LEVEL_ID = '42e01f';

export const PIPELINE_SSRC_LOW = 12345678;
export const PIPELINE_SSRC_HIGH = 12345679;
export const PIPELINE_PAYLOAD_TYPE = H264_PAYLOAD_TYPE;

export interface EncodedChunkInput {
  data: Uint8Array | Buffer;
  timestamp: number; // in microseconds
  type: 'key' | 'delta';
  duration?: number;
}

export interface SimulcastLayerConfig {
  ssrc: number;
  rid: string;
  scalabilityMode: string;
}

export class WebCodecsSimulcastPipeline {
  private directTransport: MediasoupTypes.DirectTransport | undefined = undefined;
  private producer: MediasoupTypes.Producer | undefined = undefined;
  private seqMap = new Map<number, number>(); // ssrc -> sequence number
  private cachedParameterSets = new Map<number, Uint8Array[]>(); // layer -> SPS/PPS nalus

  readonly layers: SimulcastLayerConfig[] = [
    { ssrc: PIPELINE_SSRC_LOW, rid: 'q', scalabilityMode: 'L1T1' },
    { ssrc: PIPELINE_SSRC_HIGH, rid: 'h', scalabilityMode: 'L1T1' }
  ];

  constructor() {
    this.seqMap.set(PIPELINE_SSRC_LOW, Math.floor(Math.random() * 0xffff));
    this.seqMap.set(PIPELINE_SSRC_HIGH, Math.floor(Math.random() * 0xffff));
  }

  async setup(
    router: MediasoupTypes.Router,
    profileLevelId: string = DEFAULT_H264_PROFILE_LEVEL_ID
  ): Promise<MediasoupTypes.Producer> {
    this.directTransport = await router.createDirectTransport();
    await this.directTransport.connect();

    this.producer = await this.directTransport.produce({
      kind: 'video',
      rtpParameters: {
        codecs: [
          {
            mimeType: 'video/H264',
            payloadType: H264_PAYLOAD_TYPE,
            clockRate: H264_CLOCK_RATE,
            parameters: {
              'packetization-mode': 1,
              'profile-level-id': profileLevelId,
              'level-asymmetry-allowed': 1
            },
            rtcpFeedback: [
              { type: 'nack' },
              { type: 'nack', parameter: 'pli' },
              { type: 'ccm', parameter: 'fir' }
            ]
          }
        ],
        encodings: this.layers.map(layer => ({
          ssrc: layer.ssrc,
          rid: layer.rid,
          scalabilityMode: layer.scalabilityMode
        })),
        headerExtensions: [],
        rtcp: { cname: 'webcodecs-simulcast-pipeline' }
      }
    });

    console.log(`[SimulcastPipeline] Multi-SSRC Producer ready: ${this.producer.id} (layers: ${this.layers.length})`);
    return this.producer;
  }

  injectChunk(
    layerIndex: number,
    chunk: EncodedChunkInput,
    descriptionBase64?: string
  ): void {
    if (!this.producer) {
      throw new Error('Simulcast pipeline is not initialized');
    }

    const layer = this.layers[layerIndex];
    if (!layer) {
      throw new Error(`Invalid simulcast layer index: ${layerIndex}`);
    }

    const ssrc = layer.ssrc;
    if (descriptionBase64) {
      this.updateCachedParameterSets(layerIndex, Buffer.from(descriptionBase64, 'base64'));
    }

    const rawChunk = chunk.data instanceof Uint8Array ? chunk.data : new Uint8Array(chunk.data);
    const chunkNalus = this.extractH264Nalus(rawChunk);
    if (chunkNalus.length === 0) {
      return;
    }

    const cachedSets = this.cachedParameterSets.get(layerIndex) ?? [];
    const nalus = chunk.type === 'key' && cachedSets.length > 0
      ? [...cachedSets, ...chunkNalus]
      : chunkNalus;

    const rtpTimestamp = this.webCodecsTimestampToRtpTimestamp(chunk.timestamp);

    for (let i = 0; i < nalus.length; ++i) {
      const nalu = nalus[i]!;
      const isLastNalu = i === nalus.length - 1;
      const packets = this.packetizeH264Nalu(nalu, {
        ssrc,
        payloadType: H264_PAYLOAD_TYPE,
        timestamp: rtpTimestamp,
        marker: isLastNalu
      });

      for (const packet of packets) {
        this.producer.send(Buffer.from(packet));
      }
    }
  }

  close(): void {
    this.producer?.close();
    this.directTransport?.close();
    this.producer = undefined;
    this.directTransport = undefined;
    this.cachedParameterSets.clear();
  }

  private packetizeH264Nalu(
    nalu: Uint8Array,
    opts: { ssrc: number; payloadType: number; timestamp: number; marker: boolean }
  ): Uint8Array[] {
    if (nalu.length <= H264_MAX_PAYLOAD_SIZE) {
      return [this.buildRtpPacket(nalu, opts)];
    }

    const packets: Uint8Array[] = [];
    const naluType = nalu[0]! & 0x1f;
    const naluHeader = nalu[0]! & 0xe0;
    let offset = 1;

    while (offset < nalu.length) {
      const isFirst = offset === 1;
      const end = Math.min(offset + H264_MAX_PAYLOAD_SIZE - 2, nalu.length);
      const isLast = end === nalu.length;
      const fuHeader = (isFirst ? 0x80 : 0) | (isLast ? 0x40 : 0) | naluType;
      const payload = new Uint8Array(2 + end - offset);
      payload[0] = naluHeader | 28;
      payload[1] = fuHeader;
      payload.set(nalu.subarray(offset, end), 2);

      packets.push(
        this.buildRtpPacket(payload, { ...opts, marker: isLast && opts.marker })
      );
      offset = end;
    }

    return packets;
  }

  private buildRtpPacket(
    payload: Uint8Array,
    opts: { ssrc: number; payloadType: number; timestamp: number; marker: boolean }
  ): Uint8Array {
    const packet = new Uint8Array(12 + payload.length);
    const view = new DataView(packet.buffer, packet.byteOffset, packet.byteLength);
    let seq = this.seqMap.get(opts.ssrc) ?? 0;

    view.setUint8(0, RTP_VERSION);
    view.setUint8(1, (opts.marker ? 0x80 : 0) | (opts.payloadType & 0x7f));
    view.setUint16(2, seq);
    view.setUint32(4, opts.timestamp);
    view.setUint32(8, opts.ssrc);
    packet.set(payload, 12);

    seq = (seq + 1) & 0xffff;
    this.seqMap.set(opts.ssrc, seq);

    return packet;
  }

  private webCodecsTimestampToRtpTimestamp(timestampUs: number): number {
    return Math.floor((timestampUs * H264_CLOCK_RATE) / 1_000_000) >>> 0;
  }

  private updateCachedParameterSets(layerIndex: number, description: Uint8Array): void {
    const parameterSets = this.extractParameterSetsFromAvcc(description);
    if (parameterSets.length > 0) {
      this.cachedParameterSets.set(layerIndex, parameterSets);
    }
  }

  private extractH264Nalus(data: Uint8Array): Uint8Array[] {
    const annexBNalus = this.splitAnnexB(data);
    if (annexBNalus.length > 0) {
      return annexBNalus;
    }
    return this.splitLengthPrefixedNalus(data);
  }

  private splitAnnexB(data: Uint8Array): Uint8Array[] {
    const starts: Array<{ index: number; size: number }> = [];

    for (let i = 0; i < data.length - 2; ++i) {
      if (data[i] === 0 && data[i + 1] === 0 && data[i + 2] === 1) {
        starts.push({ index: i, size: 3 });
        i += 2;
      } else if (
        i < data.length - 3 &&
        data[i] === 0 &&
        data[i + 1] === 0 &&
        data[i + 2] === 0 &&
        data[i + 3] === 1
      ) {
        starts.push({ index: i, size: 4 });
        i += 3;
      }
    }

    return starts
      .map((start, idx) => {
        const naluStart = start.index + start.size;
        const nextStart = starts[idx + 1];
        const naluEnd = nextStart ? nextStart.index : data.length;
        return data.subarray(naluStart, naluEnd);
      })
      .filter(nalu => nalu.length > 0);
  }

  private splitLengthPrefixedNalus(data: Uint8Array): Uint8Array[] {
    const nalus: Uint8Array[] = [];
    let offset = 0;

    while (offset + 4 <= data.length) {
      const length =
        ((data[offset] ?? 0) << 24) |
        ((data[offset + 1] ?? 0) << 16) |
        ((data[offset + 2] ?? 0) << 8) |
        (data[offset + 3] ?? 0);
      const naluStart = offset + 4;
      const naluEnd = naluStart + length;

      if (length <= 0 || naluEnd > data.length) {
        return data.length > 0 ? [data] : [];
      }

      nalus.push(data.subarray(naluStart, naluEnd));
      offset = naluEnd;
    }

    return nalus;
  }

  private extractParameterSetsFromAvcc(description: Uint8Array): Uint8Array[] {
    if (description.length < 7) {
      return [];
    }

    const parameterSets: Uint8Array[] = [];
    let offset = 5;
    const spsCount = (description[offset++] ?? 0) & 0x1f;

    for (let i = 0; i < spsCount; ++i) {
      if (offset + 2 > description.length) {
        return parameterSets;
      }
      const length = ((description[offset] ?? 0) << 8) | (description[offset + 1] ?? 0);
      offset += 2;
      if (offset + length > description.length) {
        return parameterSets;
      }
      parameterSets.push(description.subarray(offset, offset + length));
      offset += length;
    }

    if (offset >= description.length) {
      return parameterSets;
    }

    const ppsCount = description[offset++] ?? 0;
    for (let i = 0; i < ppsCount; ++i) {
      if (offset + 2 > description.length) {
        return parameterSets;
      }
      const length = ((description[offset] ?? 0) << 8) | (description[offset + 1] ?? 0);
      offset += 2;
      if (offset + length > description.length) {
        return parameterSets;
      }
      parameterSets.push(description.subarray(offset, offset + length));
      offset += length;
    }

    return parameterSets;
  }
}
