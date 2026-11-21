import { fileURLToPath } from "node:url";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// The Hono server (server/index.ts) signs people in. In development it
// listens on 3001 and Vite proxies to it, so the browser sees one origin.
const backend = "http://127.0.0.1:3001";

export default defineConfig({
  plugins: [react()],
  resolve: { alias: { "@": fileURLToPath(new URL(".", import.meta.url)) } },
  worker: { format: "es" },
  server: {
    host: "127.0.0.1",
    port: 3000,
    strictPort: true,
    proxy: { "/api": { target: backend } },
  },
});
