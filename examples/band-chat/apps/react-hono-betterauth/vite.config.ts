import { fileURLToPath } from "node:url";
import react from "@vitejs/plugin-react";
import { jazzPlugin } from "jazz-tools/dev/vite";
import { defineConfig } from "vite";
import { readConfig } from "./src/lib/config.mjs";

// The Hono server (server/index.ts) runs Jazz and Better Auth. In development
// it listens on 3001 and Vite proxies to it, so the browser sees one origin.
const backend = "http://127.0.0.1:3001";

export default defineConfig({
  plugins: [react(), jazzPlugin({ server: false })],
  resolve: { alias: { "@": fileURLToPath(new URL(".", import.meta.url)) } },
  // The browser and the backend name the same Jazz app (a local default in
  // development; `pnpm build` checks it is set).
  define: { "import.meta.env.VITE_JAZZ_APP_ID": JSON.stringify(readConfig().appId) },
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
