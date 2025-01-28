import { defineConfig } from "vite";
import type { Plugin } from "vite";
import dts from "vite-plugin-dts";
import topLevelAwait from "vite-plugin-top-level-await";
import wasm from "vite-plugin-wasm";

// Custom plugin to fix WASM paths
const fixWasmPaths = (): Plugin => ({
  name: "fix-wasm-paths",
  renderChunk(code: string, chunk: { fileName: string }) {
    if (chunk.fileName.includes("jazz_crypto_rs")) {
      // Fix the path to look in the node directory for the WASM file
      return code.replace(
        "const path = require('path').join(__dirname, 'jazz_crypto_rs_bg.wasm')",
        "const path = require('path').join(__dirname, 'node/jazz_crypto_rs_bg.wasm')",
      );
    }
    return code;
  },
});

export default defineConfig({
  build: {
    lib: {
      entry: "./wrapper.ts",
      formats: ["es"],
      fileName: "wrapper",
    },
    rollupOptions: {
      external: [],
    },
    emptyOutDir: false,
    target: "esnext",
  },
  plugins: [
    wasm(),
    dts(),
    topLevelAwait({
      // Required to be true for library builds
      promiseExportName: "__tla",
      promiseImportName: (i) => `__tla_${i}`,
    }),
    fixWasmPaths(),
  ],
});
