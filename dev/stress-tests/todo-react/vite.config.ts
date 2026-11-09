import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { jazzBackend } from "./jazz.server.js";

export default defineConfig({
  plugins: [
    react(),
    jazzBackend({ dataDir: process.env.JAZZ_IN_MEMORY === "1" ? undefined : ".jazz" }),
  ],
  build: { target: "es2020" },
  worker: { format: "es" },
  optimizeDeps: {
    exclude: ["jazz-wasm"],
  },
});
