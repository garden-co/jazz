import { defineConfig } from "vitest/config";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";
import react from "@vitejs/plugin-react";
import { playwright } from "@vitest/browser-playwright";
import {
  openIsolatedApp,
  readIsolatedAttr,
  waitForIsolatedAttr,
  pressIsolatedKey,
  releaseIsolatedKey,
  closeIsolatedApp,
  debugIsolatedState,
  startFreshTestServer,
  stopFreshTestServer,
} from "./tests/browser/commands.js";

export default defineConfig({
  plugins: [wasm(), topLevelAwait(), react()],
  worker: {
    plugins: () => [wasm(), topLevelAwait()],
  },
  test: {
    browser: {
      enabled: true,
      provider: playwright(),
      instances: [{ browser: "chromium", headless: true }],
      commands: {
        openIsolatedApp,
        readIsolatedAttr,
        waitForIsolatedAttr,
        pressIsolatedKey,
        releaseIsolatedKey,
        closeIsolatedApp,
        debugIsolatedState,
        startFreshTestServer,
        stopFreshTestServer,
      },
    },
    include: ["tests/browser/**/*.test.{ts,tsx}"],
    globalSetup: ["tests/browser/global-setup.ts"],
    testTimeout: 30000,
  },
});
