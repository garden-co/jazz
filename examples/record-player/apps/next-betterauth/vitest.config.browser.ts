import { defineConfig } from "vitest/config";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";
import react from "@vitejs/plugin-react";
import { playwright } from "@vitest/browser-playwright";
import { resolve } from "node:path";
import { topologyReceipt } from "./vitest-receipts.mjs";
import {
  blockJazzServerNetwork,
  jazzServerInfo,
  jazzServerJwtForUser,
  stopJazzServerByUrl,
  unblockJazzServerNetwork,
} from "../../../../packages/jazz-tools/tests/browser/testing-server-node.js";

const sealedWasmPackage = process.env.JAZZ_CORRECTNESS_WASM_PACKAGE;
if (process.env.JAZZ_CORRECTNESS_ARTIFACT_RUN === "1" && !sealedWasmPackage)
  throw new Error("sealed correctness consumer is missing its admitted WASM package");

function jazzBrowserTopologyLog(
  _context: unknown,
  status: "start" | "complete" | "failed",
  label: string,
  elapsedMs: number,
) {
  console.info(`[jazz-browser-topology] ${status} ${label} (${elapsedMs}ms)`);
}

export default defineConfig({
  // The workspace browser partition runs this project while the package's unit
  // and provider projects run in the Node partition. They must not concurrently
  // replace Vite's optimized-dependency cache while Chromium imports a test.
  cacheDir: "node_modules/.vite-record-player-topology",
  resolve: {
    alias: sealedWasmPackage ? { "jazz-wasm": resolve(sealedWasmPackage, "jazz_wasm.js") } : {},
  },
  optimizeDeps: {
    include: ["react", "react-dom/client", "react/jsx-dev-runtime"],
  },
  define: {
    "process.env.NEXT_PUBLIC_JAZZ_APP_ID": JSON.stringify("record-player-browser-tests"),
    "process.env.NEXT_PUBLIC_JAZZ_SERVER_URL": "undefined",
  },
  plugins: [wasm(), topLevelAwait(), react()],
  worker: { plugins: () => [wasm(), topLevelAwait()] },
  test: {
    // This project owns the Jazz server lifecycle. Keep the mocked provider
    // receipt in vitest.config.provider.ts so `test:browser` remains a true
    // topology-only gate.
    include: [topologyReceipt],
    globalSetup: ["../../../../packages/jazz-tools/tests/browser/global-setup.ts"],
    browser: {
      enabled: true,
      // Other parallel Vitest projects can reach any port in the default
      // range. Let Vite retry EADDRINUSE instead of treating a source-code
      // port convention as an operating-system reservation.
      api: { port: 63318, strictPort: false },
      provider: playwright(),
      instances: [{ browser: "chromium", headless: true }],
      commands: {
        jazzBrowserTopologyLog,
        jazzServerInfo: async (_context, appId, schema) => jazzServerInfo(appId, schema),
        jazzServerStop: async (_context, serverUrl) => stopJazzServerByUrl(serverUrl),
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
