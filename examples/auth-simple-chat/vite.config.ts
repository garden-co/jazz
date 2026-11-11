import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Jazz runs inside the auth server (server/auth-server.ts): no separate sync server.
export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/api/auth": { target: "http://127.0.0.1:3001", changeOrigin: true },
      "/jazz": { target: "http://127.0.0.1:3001", changeOrigin: true, ws: true },
    },
  },
  // jazz-wasm is wasm-bindgen output, which Vite's dependency pre-bundler cannot handle.
  optimizeDeps: { exclude: ["jazz-wasm"] },
});
