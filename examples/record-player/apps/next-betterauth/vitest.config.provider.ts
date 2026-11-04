import { defineConfig } from "vitest/config";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";
import react from "@vitejs/plugin-react";
import { playwright } from "@vitest/browser-playwright";
import { providerReceipt } from "./vitest-receipts.mjs";

/**
 * The provider receipt mocks Jazz and Better Auth deliberately. Keep it out of
 * the topology project: that project owns a server lifecycle and must run only
 * when a test actually exercises an edge/core topology.
 */
export default defineConfig({
  // Keep this separate from the topology project's cache: CI deliberately
  // overlaps the package's Node and browser partitions.
  cacheDir: "node_modules/.vite-record-player-provider",
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
    include: [providerReceipt],
    browser: {
      enabled: true,
      provider: playwright(),
      instances: [{ browser: "chromium", headless: true }],
    },
    setupFiles: ["tests/browser/setup-react.ts"],
    testTimeout: 15_000,
    sequence: { concurrent: false },
  },
});
