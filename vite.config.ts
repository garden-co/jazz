// vitest.config.ts
import { defineConfig } from "vitest/config";

export default defineConfig({
  root: "./",
  test: {
    typecheck: {
      enabled: true,
      checker: "tsc",
    },
    workspace: [
      "packages/*",
      "tests/browser-integration",
      "tests/cloudflare-workers",
    ],
    coverage: {
      enabled: false,
      provider: "istanbul",
      include: ["packages/*/src/**/*.ts"],
      exclude: ["packages/*/src/tests", "packages/jazz-svelte/**"],
      reporter: ["html"],
      thresholds: {
        global: {
          branches: 80,
          functions: 80,
          lines: 80,
          statements: 80,
        },
      },
    },
    include: ["packages/*/tests/**/*.test.{js,ts,svelte}"],
    maxConcurrency: 5,
  },
});
