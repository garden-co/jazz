import { fileURLToPath } from "node:url";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// The Hono server (server/index.ts) runs Jazz and Better Auth. In development
// it listens on 3001 and Vite proxies to it, so the browser sees one origin.
const backend = "http://127.0.0.1:3001";

export default defineConfig({
  plugins: [react()],
  // jazz-wasm is wasm-bindgen output, which Vite's dependency pre-bundler cannot handle.
  optimizeDeps: { exclude: ["jazz-wasm"] },
  resolve: { alias: { "@": fileURLToPath(new URL(".", import.meta.url)) } },
  worker: { format: "es" },
  server: {
    host: "127.0.0.1",
    port: 3000,
    strictPort: true,
    proxy: {
      "/api": { target: backend },
      "/jazz": { target: backend, ws: true },
    },
  },
});
