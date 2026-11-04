import { defineConfig } from "vite";
import { jazzBackend } from "./jazz.server.js";

export default defineConfig({
  plugins: [jazzBackend({ dataDir: ".jazz" })],
  // jazz-wasm is wasm-bindgen output, which Vite's dependency pre-bundler cannot handle.
  optimizeDeps: { exclude: ["jazz-wasm"] },
});
