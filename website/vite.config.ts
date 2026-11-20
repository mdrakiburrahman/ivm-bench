import { defineConfig } from 'vite';

export default defineConfig(({ mode }) => ({
  build: { copyPublicDir: mode === 'fixture' },
}));
