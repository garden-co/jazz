import react from "@vitejs/plugin-react";
import { playwright } from "@vitest/browser-playwright";
import { defineConfig } from "vitest/config";
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
    "process.env.JAZZ_EXAMPLE_TOPOLOGY_SEED": JSON.stringify(
      process.env.JAZZ_EXAMPLE_TOPOLOGY_SEED,
    ),
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
        jazzBrowserTopologyLog(_context, status, label, elapsedMs) {
          console.info(`[jazz-browser-topology] ${status} ${label} (${elapsedMs}ms)`);
        },
        jazzServerInfo: async (_context, appId, schema) => jazzServerInfo(appId, schema),
        jazzServerBlockNetwork: async ({ context }, serverUrl) =>
          blockJazzServerNetwork(context, serverUrl),
        jazzServerUnblockNetwork: async ({ context }, serverUrl) =>
          unblockJazzServerNetwork(context, serverUrl),
        jazzServerJwtForUser: async (_context, userId, claims, appId) =>
          jazzServerJwtForUser(userId, claims, appId),
      },
    },
    setupFiles: ["tests/browser/setup-react.ts"],
    testTimeout: 90_000,
    sequence: { concurrent: false },
  },
});
