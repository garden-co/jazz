import { defineConfig } from "vitest/config";
import { resolve } from "node:path";

// Durable Object tests: Workers run in local workerd (Miniflare), never on
// Cloudflare. Files run one at a time: each starts its own workerd.
export default defineConfig({
  resolve: {
    alias: [{ find: /^jazz-tools$/, replacement: resolve(__dirname, "src/index.ts") }],
  },
  test: {
    environment: "node",
    pool: "forks",
    fileParallelism: false,
    testTimeout: 60_000,
    setupFiles: ["tests/setup/lazy-node-engine.ts"],
    include: ["tests/durable-objects/**/*.test.ts"],
  },
});
