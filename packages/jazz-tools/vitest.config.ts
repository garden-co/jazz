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
    // Many tests compile and import real schema projects or start a runtime
    // cold. They take 1-2s alone, but over 5s on a loaded CI runner (merge
    // groups 192, 197 and 198 each failed one at vitest's 5s default), so the
    // suite gets the same headroom the browser suite has.
    testTimeout: 30_000,
    include: ["src/**/*.test.ts", "tests/ts-dsl/**/*.test.ts", "tests/topology/**/*.test.ts"],
    exclude: ["tests/browser/**", "node_modules/**", "src/**/*.svelte.test.ts", "src/solid/**"],
  },
});
