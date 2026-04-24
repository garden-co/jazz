import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { jazzPlugin } from "jazz-tools/dev/vite";

// Pin the Jazz dev server to a fixed port so the standalone Hono backend
// (started by tsx) knows where to reach it without coordinating via .env.
const JAZZ_SERVER_PORT = 4002;

export default defineConfig({
  plugins: [
    react(),
    jazzPlugin({
      server: {
        port: JAZZ_SERVER_PORT,
        jwksUrl: "http://localhost:3001/api/auth/jwks",
      },
    }),
  ],
  worker: { format: "es" },
  server: {
    proxy: {
      "/api": {
        target: "http://localhost:3001",
        changeOrigin: true,
      },
    },
  },
});
