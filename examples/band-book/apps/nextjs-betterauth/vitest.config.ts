import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

/** Node receipts: policies, bootstrap and invites against a local Jazz authority, plus pure helpers. */
export default defineConfig({
  resolve: { alias: { "@": fileURLToPath(new URL(".", import.meta.url)) } },
  test: {
    include: ["tests/permissions/**/*.test.ts", "tests/unit/**/*.test.ts"],
    testTimeout: 60_000,
    hookTimeout: 60_000,
  },
});
