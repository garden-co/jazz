import { defineConfig } from "vitest/config";

export default defineConfig({
  test: { include: ["tests/server/**/*.test.ts"], testTimeout: 30_000 },
});
