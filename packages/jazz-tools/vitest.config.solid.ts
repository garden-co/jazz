import { defineConfig } from "vitest/config";
import solid from "vite-plugin-solid";
import { resolve } from "node:path";

const sealedWasmPackage = process.env.JAZZ_CORRECTNESS_WASM_PACKAGE;
if (process.env.JAZZ_CORRECTNESS_ARTIFACT_RUN === "1" && !sealedWasmPackage)
  throw new Error("sealed correctness consumer is missing its admitted WASM package");

export default defineConfig({
  plugins: [solid()],
  resolve: {
    alias: sealedWasmPackage ? { "jazz-wasm": resolve(sealedWasmPackage, "jazz_wasm.js") } : {},
  },
  test: {
    environment: "jsdom",
    include: ["src/solid/**/*.test.ts", "src/solid/**/*.test.tsx"],
    exclude: ["node_modules/**"],
  },
});
