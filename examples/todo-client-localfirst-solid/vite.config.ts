import { defineConfig } from "vite";
import solidPlugin from "vite-plugin-solid";
import { jazzBackend } from "./jazz.server.js";

export default defineConfig({
  plugins: [solidPlugin(), jazzBackend({ dataDir: ".jazz" })],
  // jazz-wasm is wasm-bindgen output, which Vite's dependency pre-bundler cannot handle.
  optimizeDeps: { exclude: ["jazz-wasm"] },
});
