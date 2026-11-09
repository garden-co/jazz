import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// WorkOS sign-in and Jazz both run in server/server.ts; the browser reaches them on this origin.
const backend = { target: "http://127.0.0.1:3001", changeOrigin: false };
export default defineConfig({
  plugins: [react()],
  server: {
    host: "127.0.0.1",
    proxy: {
      "/auth": backend,
      "/api/auth": backend,
      "/jazz": { ...backend, ws: true },
    },
  },
  // jazz-wasm is wasm-bindgen output, which Vite's dependency pre-bundler cannot handle.
  optimizeDeps: { exclude: ["jazz-wasm"] },
});
