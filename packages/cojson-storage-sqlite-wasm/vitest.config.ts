import { defineProject } from "vitest/config";
import { playwright } from "@vitest/browser-playwright";

export default defineProject({
  optimizeDeps: {
    exclude: ["@sqlite.org/sqlite-wasm"],
  },
  test: {
    name: "cojson-storage-sqlite-wasm",
    browser: {
      enabled: true,
      provider: playwright(),
      instances: [
        {
          headless: process.env.HEADLESS !== "false",
          browser: "chromium",
        },
      ],
    },
    include: ["src/**/*.test.ts"],
  },
});
