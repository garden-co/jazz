// vitest.config.ts
import { defineConfig } from "vitest/config";

export default defineConfig({
  root: "./",
  test: {
    workspace: [
      "packages/*",
      // "tests/browser-integration",
      "tests/cloudflare-workers",
    ],
    maxConcurrency: 5,
  },
});
