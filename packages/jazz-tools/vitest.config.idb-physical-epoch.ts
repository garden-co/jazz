import { defineConfig } from "vitest/config";
import { playwright } from "@vitest/browser-playwright";

// This raw physical-store receipt deliberately avoids the normal browser
// global setup, which starts a NAPI-backed server unrelated to IndexedDB.
export default defineConfig({
  test: {
    include: ["tests/browser/indexeddb-physical-epoch.test.ts"],
    browser: {
      enabled: true,
      provider: playwright(),
      instances: [{ browser: "chromium", headless: true }],
    },
  },
});
