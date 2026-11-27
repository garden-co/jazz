import { fileURLToPath } from "node:url";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// The Hono server (server/index.ts) seals live queries and runs the writes. In
// development it listens on 3001 and Vite proxies /api to it; the dev origin
// (localhost:5173) is pre-trusted by Managed Better Auth and listed in neon.ts.
const backend = "http://127.0.0.1:3001";

export default defineConfig({
  plugins: [react()],
  resolve: { alias: { "@": fileURLToPath(new URL(".", import.meta.url)) } },
  build: { target: "es2022" },
  server: {
    host: "localhost",
    port: 5173,
    strictPort: true,
    proxy: { "/api": { target: backend } },
  },
});
