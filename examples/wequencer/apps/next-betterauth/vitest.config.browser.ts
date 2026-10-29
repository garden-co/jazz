import react from "@vitejs/plugin-react";
import { playwright } from "@vitest/browser-playwright";
import { defineConfig } from "vitest/config";
import topLevelAwait from "vite-plugin-top-level-await";
import wasm from "vite-plugin-wasm";

import {
  jazzServerInfo,
  createJazzServerTransportControl,
  blockJazzServerTransport,
  unblockJazzServerTransport,
  stopJazzServerTransportControl,
  jazzServerJwtForUser,
  stopJazzServerByUrl,
} from "../../../../packages/jazz-tools/tests/browser/testing-server-node.js";

function jazzBrowserTopologyLog(
  _context: unknown,
  status: "start" | "complete" | "failed",
  label: string,
  elapsedMs: number,
) {
  console.info(`[jazz-browser-topology] ${status} ${label} (${elapsedMs}ms)`);
}

export default defineConfig({
  define: {
    __JAZZ_EXAMPLE_TOPOLOGY_SEED__: JSON.stringify(process.env.JAZZ_EXAMPLE_TOPOLOGY_SEED ?? "61"),
  },
  plugins: [wasm(), topLevelAwait(), react()],
  worker: { plugins: () => [wasm(), topLevelAwait()] },
  test: {
    include: ["tests/browser/**/*.test.ts"],
    globalSetup: ["../../../../packages/jazz-tools/tests/browser/global-setup.ts"],
    browser: {
      enabled: true,
      provider: playwright(),
      instances: [{ browser: "chromium", headless: true }],
      commands: {
        jazzBrowserTopologyLog,
        jazzServerInfo: async (_context, appId) => jazzServerInfo(appId),
        jazzServerTransportControlCreate: async (_context, url) =>
          createJazzServerTransportControl(url),
        jazzServerTransportControlBlock: async (_context, url, direction: "both" | "inbound") =>
          blockJazzServerTransport(url, direction),
        jazzServerTransportControlUnblock: async (_context, url) => unblockJazzServerTransport(url),
        jazzServerTransportControlStop: async (_context, url) =>
          stopJazzServerTransportControl(url),
        jazzServerStop: async (_context, serverUrl) => stopJazzServerByUrl(serverUrl),
        jazzServerJwtForUser: async (_context, userId, claims, appId) =>
          jazzServerJwtForUser(userId, claims, appId),
      },
    },
    testTimeout: 30_000,
    sequence: { concurrent: false },
  },
});
