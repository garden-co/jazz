import { defineConfig } from "vitest/config";

/** Node policy and workflow receipts. Browser topology receipts use vitest.config.browser.ts. */
export default defineConfig({
  test: {
    include: [
      "schema.test.ts",
      "tests/config.test.ts",
      "tests/imports.test.ts",
      "tests/permissions/**/*.test.ts",
    ],
    environment: "node",
    testTimeout: 60_000,
  },
});
