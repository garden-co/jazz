import { defineConfig } from "vite";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";

export default defineConfig({
  plugins: [wasm(), topLevelAwait()],
  build: { target: "es2020" },
  worker: { format: "es", plugins: () => [wasm(), topLevelAwait()] },
  optimizeDeps: {
    exclude: ["groove-wasm"],
  },
});
