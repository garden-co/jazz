import { defineConfig } from "vitest/config";
import { svelte } from "@sveltejs/vite-plugin-svelte";

export default defineConfig({
  plugins: [svelte()],
  test: {
    environment: "node",
    // Native/WASM integration tests use process isolation; this is unrelated
    // to the removed SQLite docs-index resolver.
    pool: "forks",
    include: ["src/**/*.test.ts", "tests/ts-dsl/**/*.test.ts", "tests/topology/**/*.test.ts"],
    exclude: ["tests/browser/**", "node_modules/**", "src/**/*.svelte.test.ts", "src/solid/**"],
  },
});
