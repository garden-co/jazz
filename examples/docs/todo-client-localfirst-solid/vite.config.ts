import { defineConfig } from "vite";
import solidPlugin from "vite-plugin-solid";
import { fileURLToPath } from "node:url";
import { jazzBackend } from "./jazz.server.js";

export default defineConfig({
  resolve: {
    alias: {
      "jazz-tools/solid": fileURLToPath(
        new URL("../../../packages/jazz-tools/src/solid/index.ts", import.meta.url),
      ),
    },
  },
  plugins: [solidPlugin(), jazzBackend({ dataDir: ".jazz" })],
  // jazz-wasm is wasm-bindgen output, which Vite's dependency pre-bundler cannot handle.
  optimizeDeps: { exclude: ["jazz-wasm"] },
});
