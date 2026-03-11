import { defineConfig } from "vitest/config";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";
import { resolve } from "node:path";
import { playwright } from "@vitest/browser-playwright";

const realisticBrowserScenarios = process.env.JAZZ_REALISTIC_BROWSER_SCENARIOS ?? "";
const realisticBrowserRunId = process.env.JAZZ_REALISTIC_BROWSER_RUN_ID ?? "";

export default defineConfig({
  define: {
    __JAZZ_REALISTIC_BROWSER_SCENARIOS__: JSON.stringify(realisticBrowserScenarios),
    __JAZZ_REALISTIC_BROWSER_RUN_ID__: JSON.stringify(realisticBrowserRunId),
  },
  plugins: [wasm(), topLevelAwait()],
  server: {
    headers: {
      "Cross-Origin-Opener-Policy": "same-origin",
      "Cross-Origin-Embedder-Policy": "require-corp",
    },
    fs: {
      allow: [resolve(__dirname, "../..")],
    },
  },
  optimizeDeps: {
    include: ["react/jsx-dev-runtime", "react/jsx-runtime"],
  },
  resolve: {
    alias: {
      // Needed because jazz-tools browser tests import from source (../../src/),
      // bypassing node_modules resolution. Consumers don't need this.
      "jazz-wasm": resolve(__dirname, "../../crates/jazz-wasm/pkg"),
    },
  },
  worker: {
    plugins: () => [wasm(), topLevelAwait()],
  },
  test: {
    browser: {
      enabled: true,
      provider: playwright(),
      instances: [{ browser: "chromium", headless: true }],
    },
    include: ["tests/browser/**/*.test.ts", "tests/browser/**/*.test.tsx"],
    globalSetup: ["tests/browser/global-setup.ts"],
    testTimeout: 30000,
  },
});
