import { defineConfig } from "vitest/config";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";
import react from "@vitejs/plugin-react";
import { resolve } from "node:path";

export default defineConfig({
  plugins: [wasm(), topLevelAwait(), react()],
  resolve: {
    alias: {
      "groove-wasm": resolve(__dirname, "../../crates/groove-wasm/pkg"),
      "jazz-ts": resolve(__dirname, "../../packages/jazz-ts/src/index.ts"),
      "jazz-react": resolve(__dirname, "../../packages/jazz-react/src/index.ts"),
    },
  },
  worker: {
    plugins: () => [wasm(), topLevelAwait()],
  },
  test: {
    browser: {
      enabled: true,
      name: "chromium",
      provider: "playwright",
      headless: true,
    },
    include: ["tests/browser/**/*.test.tsx"],
    globalSetup: ["tests/browser/global-setup.ts"],
    testTimeout: 30000,
  },
});
