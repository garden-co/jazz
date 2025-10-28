import { defineProject } from "vitest/config";

export default defineProject({
  test: {
    name: "cojson-storage-indexeddb",
    browser: {
      enabled: true,
      provider: "playwright",
      instances: [{ browser: "chromium", headless: true }],
    },
  },
});
