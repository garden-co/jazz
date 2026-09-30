import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["tests/unit/**/*.test.ts", "tests/permissions/**/*.test.ts"],
    testTimeout: 30000,
  },
});
