import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";
import { resolve } from "node:path";

const sealedWasmPackage = process.env.JAZZ_CORRECTNESS_WASM_PACKAGE;
if (process.env.JAZZ_CORRECTNESS_ARTIFACT_RUN === "1" && !sealedWasmPackage)
  throw new Error("sealed correctness consumer is missing its admitted WASM package");

/**
 * Vitest configuration for React component/hook tests in react-core.
 * Uses happy-dom and @vitejs/plugin-react.
 * This configuration uses happy-dom because it exercises React hooks.
 */
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: sealedWasmPackage ? { "jazz-wasm": resolve(sealedWasmPackage, "jazz_wasm.js") } : {},
  },
  test: {
    environment: "happy-dom",
    include: ["src/**/*.test.tsx"],
    exclude: ["node_modules/**", "src/solid/**"],
  },
});
