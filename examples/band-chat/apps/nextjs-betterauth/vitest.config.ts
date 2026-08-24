import { defineConfig } from "vitest/config";
export default defineConfig({
  test: {
    include: ["src/**/*.test.ts", "tests/contracts/**/*.test.ts", "tests/permissions/**/*.test.ts"],
  },
});
