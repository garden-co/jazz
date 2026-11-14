import { defineConfig } from "vitest/config";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";
import react from "@vitejs/plugin-react";
import { playwright } from "@vitest/browser-playwright";
import {
  jazzAppFront,
  jazzAppRequest,
  jazzAppUser,
  jazzServerInfo,
  stopJazzServerByUrl,
  jazzDeployApp,
} from "../../packages/jazz-tools/tests/browser/testing-server-node.js";
import {
  TEST_ANNOUNCEMENTS_CHAT_ID,
  TEST_APP_ID,
  TEST_CHAT_ID,
} from "./tests/browser/test-constants.js";

export default defineConfig(async () => {
  // permissions.ts reads NEXT_PUBLIC_* env vars at module-eval time. Set them
  // here so the tests' deploy and the browser
  // bundle both see consistent values.
  process.env.NEXT_PUBLIC_CHAT_ID = TEST_CHAT_ID;
  process.env.NEXT_PUBLIC_ANNOUNCEMENTS_CHAT_ID = TEST_ANNOUNCEMENTS_CHAT_ID;

  return {
    plugins: [wasm(), topLevelAwait(), react()],
    worker: {
      plugins: () => [wasm(), topLevelAwait()],
    },
    define: {
      __APP_ID__: JSON.stringify(TEST_APP_ID),
      __CHAT_ID__: JSON.stringify(TEST_CHAT_ID),
      __ANNOUNCEMENTS_CHAT_ID__: JSON.stringify(TEST_ANNOUNCEMENTS_CHAT_ID),
      "process.env.NEXT_PUBLIC_CHAT_ID": JSON.stringify(TEST_CHAT_ID),
      "process.env.NEXT_PUBLIC_ANNOUNCEMENTS_CHAT_ID": JSON.stringify(TEST_ANNOUNCEMENTS_CHAT_ID),
    },
    test: {
      browser: {
        enabled: true,
        provider: playwright(),
        instances: [{ browser: "chromium", headless: true }],
        commands: {
          jazzDeployApp: async (_context, options) => jazzDeployApp(options),
          jazzServerInfo: async (_context, appId) => jazzServerInfo(appId),
          jazzServerStop: async (_context, serverUrl) => stopJazzServerByUrl(serverUrl),
          jazzAppFront: async (_context, coreUrl, app) => jazzAppFront(coreUrl, app),
          jazzAppUser: async (_context, frontUrl, userId, claims) =>
            jazzAppUser(frontUrl, userId, claims),
          jazzAppRequest: async (_context, url, init) => jazzAppRequest(url, init),
        },
      },
      include: ["tests/browser/**/*.test.{ts,tsx}"],
      globalSetup: ["../../packages/jazz-tools/tests/browser/global-setup.ts"],
      testTimeout: 30000,
    },
  };
});
