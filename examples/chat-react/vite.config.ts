import path from "node:path";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";
import { jazzBackend } from "./jazz.server.js";

export default defineConfig({
  plugins: [react(), tailwindcss(), jazzBackend({ dataDir: ".jazz" })],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  // jazz-wasm is wasm-bindgen output, which Vite's dependency pre-bundler cannot handle.
  optimizeDeps: { exclude: ["jazz-wasm"] },
});
