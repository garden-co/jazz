import { defineConfig } from "vitest/config";

// Separate from the Node-only suite: these tests need workerd and DO WASM builds.
export default defineConfig({
  test: {
    environment: "node",
    include: [
      "tests/durable-objects/*.test.ts",
      "src/backend/core-providers/durable-objects.test.ts",
    ],
    pool: "forks",
    maxWorkers: 1,
    testTimeout: 180_000,
  },
});
