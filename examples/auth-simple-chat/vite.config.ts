import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { jazzPlugin } from "jazz-tools/dev/vite";

// Jazz runs inside the auth server (server/auth-server.ts): no separate sync server.
export default defineConfig({
  plugins: [react(), jazzPlugin({ server: false })],
  server: {
    proxy: {
      "/api/auth": { target: "http://127.0.0.1:3001", changeOrigin: true },
      "/jazz": { target: "http://127.0.0.1:3001", changeOrigin: true, ws: true },
    },
  },
});
