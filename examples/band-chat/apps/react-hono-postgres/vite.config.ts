import { fileURLToPath } from "node:url";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// The Hono server (server/index.ts) runs Better Auth, the REST/RPC endpoints and the live WebSocket. In
// development it listens on 3001 and Vite proxies /api to it.
const backend = "http://127.0.0.1:3001";

export default defineConfig({
  plugins: [react()],
  resolve: { alias: { "@": fileURLToPath(new URL(".", import.meta.url)) } },
  build: { target: "es2022" },
  server: {
    host: "127.0.0.1",
    port: 3000,
    strictPort: true,
    proxy: { "/api": { target: backend, ws: true } },
  },
});
