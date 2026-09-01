import { defineConfig } from "vitest/config";

export default defineConfig({
  // The workspace Node partition overlaps this package with its topology
  // browser project, so unit transforms cannot share that project's cache.
  cacheDir: "node_modules/.vite-record-player-unit",
  test: {
    include: ["tests/*.test.ts"],
  },
});
