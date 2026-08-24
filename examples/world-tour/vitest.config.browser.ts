import { defineConfig } from "vitest/config";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";
import vue from "@vitejs/plugin-vue";
import { playwright } from "@vitest/browser-playwright";
import {
  blockJazzServerNetwork,
  jazzServerInfo,
  jazzServerJwtForUser,
  unblockJazzServerNetwork,
} from "../../packages/jazz-tools/tests/browser/testing-server-node.js";

// This file runs in Vitest's Node-side Vite configuration. Browser test
// modules consume the injected constant below rather than `process.env`.
const suppliedTopologySeed = Number(process.env.JAZZ_EXAMPLE_TOPOLOGY_SEED ?? 29);
const topologySeed = Number.isSafeInteger(suppliedTopologySeed) ? suppliedTopologySeed : 29;

export default defineConfig({
  define: {
    __JAZZ_EXAMPLE_TOPOLOGY_SEED__: JSON.stringify(topologySeed),
  },
  plugins: [wasm(), topLevelAwait(), vue()],
  worker: {
    plugins: () => [wasm(), topLevelAwait()],
  },
  test: {
    browser: {
      enabled: true,
      provider: playwright(),
      instances: [{ browser: "chromium", headless: true }],
      // Keep this adapter explicit and Node-side. Browser modules call it only
      // through jazz-tools' validated runtime command contract.
      commands: {
        jazzBrowserTopologyLog: async (_context, status, label, elapsedMs) => {
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
    include: ["tests/browser/**/*.test.ts"],
    globalSetup: ["tests/browser/global-setup.ts"],
    testTimeout: 30000,
    sequence: { concurrent: false },
  },
});
