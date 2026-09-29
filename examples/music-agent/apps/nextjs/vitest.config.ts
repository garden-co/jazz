import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

export default defineConfig({
  resolve: {
    alias: [{ find: /^@\//, replacement: fileURLToPath(new URL("./", import.meta.url)) }],
  },
  test: { include: ["tests/**/*.test.ts"], testTimeout: 120_000, hookTimeout: 60_000 },
});
