import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";
import { resolve } from "node:path";

const jazzTools = resolve(__dirname, "../../packages/jazz-tools/src");
const jazzWasm = resolve(__dirname, "../../crates/jazz-wasm/pkg");

export default defineConfig({
  plugins: [react(), wasm(), topLevelAwait()],
  resolve: {
    alias: {
      "jazz-tools/react": resolve(jazzTools, "react/index.ts"),
      "jazz-tools/permissions": resolve(jazzTools, "permissions/index.ts"),
      "jazz-wasm": jazzWasm,
    },
  },
  worker: {
    plugins: () => [wasm(), topLevelAwait()],
  },
  optimizeDeps: { exclude: ["jazz-wasm"] },
});
