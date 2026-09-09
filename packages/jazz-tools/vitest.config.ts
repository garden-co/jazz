import { availableParallelism } from "node:os";
import { defineConfig } from "vitest/config";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import { resolve } from "node:path";

const sealedWasmPackage = process.env.JAZZ_CORRECTNESS_WASM_PACKAGE;
if (process.env.JAZZ_CORRECTNESS_ARTIFACT_RUN === "1" && !sealedWasmPackage)
  throw new Error("sealed correctness consumer is missing its admitted WASM package");

export default defineConfig({
  plugins: [svelte()],
  resolve: {
    alias: [
      { find: /^react-native$/, replacement: resolve(__dirname, "tests/react-native/ui-host.ts") },
      ...(sealedWasmPackage
        ? [{ find: "jazz-wasm", replacement: resolve(sealedWasmPackage, "jazz_wasm.js") }]
        : []),
    ],
  },
  test: {
    environment: "node",
    // Native/WASM integration tests use process isolation; this is unrelated
    // to the removed SQLite docs-index resolver.
    pool: "forks",
    // Each file loads its own native/WASM runtime, while browser and example
    // suites run alongside this one. Extra forks multiply startup work and
    // can starve short delivery checks; keep capacity for those other suites.
    maxWorkers: Math.max(1, Math.min(8, Math.floor(availableParallelism() / 2))),
    include: ["src/**/*.test.ts", "tests/ts-dsl/**/*.test.ts", "tests/topology/**/*.test.ts"],
    exclude: ["tests/browser/**", "node_modules/**", "src/**/*.svelte.test.ts", "src/solid/**"],
  },
});
