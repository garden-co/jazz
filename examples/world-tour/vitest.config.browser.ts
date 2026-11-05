import { defineConfig } from "vitest/config";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";
import vue from "@vitejs/plugin-vue";
import { playwright } from "@vitest/browser-playwright";
import { jazzBackend } from "./jazz.server.js";
import { APP_ID } from "./tests/browser/test-constants.js";

export default defineConfig({
  plugins: [wasm(), topLevelAwait(), vue(), jazzBackend({ appId: APP_ID })],
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
    testTimeout: 30000,
  },
});
