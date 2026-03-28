import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  build: { target: "es2020" },
  worker: { format: "es" },
  optimizeDeps: {
    exclude: ["jazz-wasm"],
  },
  server: {
    proxy: {
      "/sync": {
        target: "http://127.0.0.1:1625",
        changeOrigin: true,
      },
      "/events": {
        target: "http://127.0.0.1:1625",
        changeOrigin: true,
      },
      "/auth": {
        target: "http://127.0.0.1:3001",
        changeOrigin: true,
      },
      "/health": {
        target: "http://127.0.0.1:1625",
        changeOrigin: true,
      },
      "/api/auth": {
        target: "http://127.0.0.1:3001",
        changeOrigin: true,
      },
    },
  },
});
