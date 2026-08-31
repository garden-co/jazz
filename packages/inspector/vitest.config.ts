import { resolve } from "node:path";
import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react-swc";

const sealedWasmPackage = process.env.JAZZ_CORRECTNESS_WASM_PACKAGE;
if (process.env.JAZZ_CORRECTNESS_ARTIFACT_RUN === "1" && !sealedWasmPackage)
  throw new Error("sealed correctness consumer is missing its admitted WASM package");

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: [
      {
        find: "jazz-tools/react",
        replacement: resolve(__dirname, "../jazz-tools/src/react/index.ts"),
      },
      {
        find: "jazz-tools/testing",
        replacement: resolve(__dirname, "../jazz-tools/src/testing/index.ts"),
      },
      {
        find: "jazz-tools",
        replacement: resolve(__dirname, "../jazz-tools/src/index.ts"),
      },
      ...(sealedWasmPackage
        ? [{ find: "jazz-wasm", replacement: resolve(sealedWasmPackage, "jazz_wasm.js") }]
        : []),
    ],
  },
  test: {
    environment: "happy-dom",
    include: ["src/**/*.test.{ts,tsx}"],
    setupFiles: ["./src/test/setup.ts"],
  },
});
