import { defineConfig } from "vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import { fileURLToPath } from "node:url";
import { jazzBackend } from "./jazz.server.js";

export default defineConfig({
  resolve: {
    alias: {
      "jazz-tools/svelte": fileURLToPath(
        new URL("../../../packages/jazz-tools/src/svelte/index.ts", import.meta.url),
      ),
    },
  },
  plugins: [svelte(), jazzBackend({ dataDir: ".jazz" })],
  // jazz-wasm is wasm-bindgen output, which Vite's dependency pre-bundler cannot handle.
  optimizeDeps: { exclude: ["jazz-wasm"] },
});
