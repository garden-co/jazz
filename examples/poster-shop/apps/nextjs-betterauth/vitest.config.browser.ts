import { defineConfig } from "vitest/config";
import { playwright } from "@vitest/browser-playwright";
import react from "@vitejs/plugin-react";
import topLevelAwait from "vite-plugin-top-level-await";
import wasm from "vite-plugin-wasm";
import {
  blockJazzServerNetwork,
  jazzServerInfo,
  jazzServerJwtForUser,
  unblockJazzServerNetwork,
} from "../../../../packages/jazz-tools/tests/browser/testing-server-node.js";

export default defineConfig({
  define: {
    __JAZZ_EXAMPLE_TOPOLOGY_SEED__: JSON.stringify(process.env.JAZZ_EXAMPLE_TOPOLOGY_SEED ?? "47"),
  },
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
        jazzServerBlockNetwork: async ({ context }, serverUrl) =>
          blockJazzServerNetwork(context, serverUrl),
        jazzServerUnblockNetwork: async ({ context }, serverUrl) =>
          unblockJazzServerNetwork(context, serverUrl),
        jazzServerJwtForUser: async (_context, userId, claims, appId) =>
          jazzServerJwtForUser(userId, claims, appId),
        jazzBrowserTopologyLog: async (_context, status, label, elapsedMs) => {
          console.info(`[jazz-browser-topology] ${status} ${label} (${elapsedMs}ms)`);
        },
      },
    },
    testTimeout: 30_000,
    sequence: { concurrent: false },
  },
});
