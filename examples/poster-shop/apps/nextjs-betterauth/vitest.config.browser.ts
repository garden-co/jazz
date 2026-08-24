import { defineConfig } from "vitest/config";
import { playwright } from "@vitest/browser-playwright";
import react from "@vitejs/plugin-react";
import topLevelAwait from "vite-plugin-top-level-await";
import wasm from "vite-plugin-wasm";
import {
  jazzServerInfo,
  jazzServerJwtForUser,
} from "../../../../packages/jazz-tools/tests/browser/testing-server-node.js";

export default defineConfig({
  plugins: [wasm(), topLevelAwait(), react()],
  worker: { plugins: () => [wasm(), topLevelAwait()] },
  test: {
    include: ["tests/browser/**/*.test.tsx"],
    globalSetup: ["../../../../packages/jazz-tools/tests/browser/global-setup.ts"],
    browser: {
      enabled: true,
      provider: playwright(),
      instances: [{ browser: "chromium", headless: true }],
      commands: {
        jazzServerInfo: async (_context, appId, schema) => jazzServerInfo(appId, schema),
        jazzServerJwtForUser: async (_context, userId, claims, appId) =>
          jazzServerJwtForUser(userId, claims, appId),
      },
    },
    testTimeout: 30_000,
    sequence: { concurrent: false },
  },
});
