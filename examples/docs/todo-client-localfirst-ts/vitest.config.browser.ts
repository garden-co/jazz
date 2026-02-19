import { defineConfig } from "vitest/config";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";
import { playwright } from "@vitest/browser-playwright";

export default defineConfig({
  plugins: [wasm(), topLevelAwait()],
  worker: {
    plugins: () => [wasm(), topLevelAwait()],
  },
  test: {
    browser: {
      enabled: true,
      provider: playwright(),
      instances: [{ browser: "chromium", headless: true }],
    },
    include: ["tests/browser/**/*.test.ts"],
    globalSetup: ["tests/browser/global-setup.ts"],
    testTimeout: 30000,
  },
});
