import { defineConfig } from 'vite';
import basicSsl from '@vitejs/plugin-basic-ssl';

import { resolve } from 'node:path';

export default defineConfig({
  plugins: [basicSsl()],
  server: {
    host: '0.0.0.0',
    port: 5173,
    proxy: {
      '/ws': {
        target: 'ws://127.0.0.1:3000',
        ws: true
      }
    }
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    rollupOptions: {
      input: {
        main: resolve(__dirname, 'index.html'),
        visualizer: resolve(__dirname, 'stats-visualizer.html')
      }
    }
  }
});
