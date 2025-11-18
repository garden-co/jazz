import { defineProject } from "vitest/config";

export default defineProject({
  test: {
    name: "jazz-webhook",
    include: ["src/**/*.test.{js,ts}"],
    setupFiles: ["src/test/setup.ts"],
    testTimeout: 30000, // Increased timeout for webhook tests with real HTTP server
    typecheck: {
      enabled: true,
      checker: "tsc",
    },
  },
});
