/// <reference types="vite/client" />

declare module '*webCodecsPipeline.js' {
  export function setupPipeline(router: any, options: any): Promise<any>;
  export function injectEncodedChunk(chunk: any, metadata?: any, payloadType?: number, metadata2?: any): void;
}
