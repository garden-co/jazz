import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

/** Node policy receipts and browser topology receipts deliberately use separate
 * runners: importing browser commands in the normal Vitest pool is an error. */
export default defineConfig({
  resolve: { alias: { "@": fileURLToPath(new URL(".", import.meta.url)) } },
  test: {
    include: ["tests/permissions/**/*.test.ts", "tests/unit/**/*.test.ts"],
  },
});
