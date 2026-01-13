import { defineProject } from "vitest/config";
import { playwright } from "@vitest/browser-playwright";

export default defineProject({
  test: {
    name: "cojson-storage-indexeddb",
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
