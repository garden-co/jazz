import { defineConfig } from "vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import basicSsl from "@vitejs/plugin-basic-ssl";

const JAZZ_SERVER_PORT = process.env.VITE_JAZZ_SERVER_PORT ?? "4200";

export default defineConfig({
  plugins: [svelte(), basicSsl()],
  build: { target: "es2020" },
  worker: { format: "es" },
  optimizeDeps: {
    exclude: ["jazz-wasm"],
  },
  server: {
    proxy: {
      "/sync": `http://127.0.0.1:${JAZZ_SERVER_PORT}`,
      "/events": `http://127.0.0.1:${JAZZ_SERVER_PORT}`,
      "/health": `http://127.0.0.1:${JAZZ_SERVER_PORT}`,
      "/auth": `http://127.0.0.1:${JAZZ_SERVER_PORT}`,
    },
  },
});
