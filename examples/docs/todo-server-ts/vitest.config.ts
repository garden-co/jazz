import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    hookTimeout: 6_000,
    testTimeout: 6_000,
  },
});
