import { defineConfig } from "vitest/config";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";
import { resolve } from "node:path";
import { playwright } from "@vitest/browser-playwright";
import { svelte } from "@sveltejs/vite-plugin-svelte";

export default defineConfig({
  plugins: [wasm(), topLevelAwait(), svelte({ hot: false })],
  server: {
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
