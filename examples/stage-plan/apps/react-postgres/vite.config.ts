import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// In dev, Vite proxies the app's server (REST endpoints and the live WebSocket).
export default defineConfig({
  plugins: [react()],
  build: { target: "es2022" },
  server: { proxy: { "/api": { target: "http://127.0.0.1:4320", ws: true } } },
});
