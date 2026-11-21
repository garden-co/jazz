import react from "@vitejs/plugin-react";
import { playwright } from "@vitest/browser-playwright";
import { defineConfig } from "vitest/config";
import topLevelAwait from "vite-plugin-top-level-await";
import wasm from "vite-plugin-wasm";

import {
  jazzServerInfo,
  jazzBrowserServerLifecycle,
  jazzServerTransportControlCommands,
  jazzAppFront,
  jazzTestDeployment,
  jazzAppRequest,
  jazzAppUser,
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
  plugins: [jazzBrowserServerLifecycle, wasm(), topLevelAwait(), react()],
  worker: { plugins: () => [wasm(), topLevelAwait()] },
  test: {
    include: ["tests/browser/**/*.test.ts"],
    browser: {
      enabled: true,
      provider: playwright(),
      instances: [{ browser: "chromium", headless: true }],
      commands: {
        jazzBrowserTopologyLog,
        jazzServerInfo: async (_context, appId) => jazzServerInfo(appId),
        ...jazzServerTransportControlCommands(),
        jazzServerStop: async (_context, serverUrl) => stopJazzServerByUrl(serverUrl),
        jazzAppFront: async (_context, coreUrl, app) => jazzAppFront(coreUrl, app),
        jazzTestDeployment: async (_context, bound, permissions) =>
          jazzTestDeployment(bound, permissions),
        jazzAppUser: async (_context, frontUrl, userId, claims) =>
          jazzAppUser(frontUrl, userId, claims),
        jazzAppRequest: async (_context, url, init) => jazzAppRequest(url, init),
      },
    },
    testTimeout: 30_000,
    sequence: { concurrent: false },
  },
});
