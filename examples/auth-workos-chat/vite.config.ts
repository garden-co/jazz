import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { jazzPlugin } from "jazz-tools/dev/vite";

// WorkOS sign-in and Jazz both run in server/server.ts; the browser reaches them on this origin.
const backend = { target: "http://127.0.0.1:3001", changeOrigin: false };
export default defineConfig({
  plugins: [react(), jazzPlugin({ server: false })],
  server: {
    host: "127.0.0.1",
    proxy: {
      "/auth": backend,
      "/api/auth": backend,
      "/jazz": { ...backend, ws: true },
    },
  },
});
