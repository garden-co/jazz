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

/** Browser topology is deliberately separate from the fast node policy receipts. */
export default defineConfig({
  define: {
    __JAZZ_EXAMPLE_TOPOLOGY_SEED__: JSON.stringify(process.env.JAZZ_EXAMPLE_TOPOLOGY_SEED ?? "47"),
  },
  plugins: [wasm(), topLevelAwait(), react()],
  worker: { plugins: () => [wasm(), topLevelAwait()] },
  test: {
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
    include: ["tests/browser/**/*.test.tsx"],
    sequence: { concurrent: false },
    testTimeout: 30_000,
  },
});
