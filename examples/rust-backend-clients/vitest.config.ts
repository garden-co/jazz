import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    include: ["tests/**/*.test.ts"],
    // The first test builds the Rust CLI (cargo), which takes minutes cold.
    hookTimeout: 900_000,
    testTimeout: 180_000,
  },
});
