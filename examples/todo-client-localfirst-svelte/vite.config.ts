import { defineConfig } from "vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import { jazzBackend } from "./jazz.server.js";

export default defineConfig({
  plugins: [svelte(), jazzBackend({ dataDir: ".jazz" })],
  // jazz-wasm is wasm-bindgen output, which Vite's dependency pre-bundler cannot handle.
  optimizeDeps: { exclude: ["jazz-wasm"] },
});
