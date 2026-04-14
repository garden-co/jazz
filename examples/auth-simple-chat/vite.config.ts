import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { jazzPlugin } from "jazz-tools/dev/vite";
import { DEFAULT_ADMIN_SECRET, DEFAULT_APP_ID, SYNC_SERVER_URL } from "./constants";

export default defineConfig({
  plugins: [
    react(),
    jazzPlugin({
      server: {
        jwksUrl: "http://127.0.0.1:3001/.well-known/jwks.json",
        appId: DEFAULT_APP_ID,
        adminSecret: DEFAULT_ADMIN_SECRET,
        port: Number(new URL(SYNC_SERVER_URL).port),
      },
    }),
  ],
  build: { target: "es2020" },
  worker: { format: "es" },
  optimizeDeps: {
    exclude: ["jazz-wasm"],
  },
  server: {
    proxy: {
      "/auth": {
        target: "http://127.0.0.1:3001",
        changeOrigin: true,
      },
      "/api/auth": {
        target: "http://127.0.0.1:3001",
        changeOrigin: true,
      },
    },
  },
});
