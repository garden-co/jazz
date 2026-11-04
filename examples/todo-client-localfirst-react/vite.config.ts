import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { jazzBackend } from "./jazz.server.js";

export default defineConfig({
  plugins: [react(), jazzBackend({ dataDir: ".jazz" })],
  // jazz-wasm is wasm-bindgen output, which Vite's dependency pre-bundler cannot handle.
  optimizeDeps: { exclude: ["jazz-wasm"] },
});
