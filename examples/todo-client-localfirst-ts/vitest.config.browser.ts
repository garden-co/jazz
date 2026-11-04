import { defineConfig } from "vitest/config";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";
import { playwright } from "@vitest/browser-playwright";
import { jazzBackend } from "./jazz.server.js";

export default defineConfig({
  plugins: [wasm(), topLevelAwait(), jazzBackend()],
  worker: {
    plugins: () => [wasm(), topLevelAwait()],
  },
  test: {
    browser: {
      enabled: true,
      provider: playwright(),
      instances: [{ browser: "chromium", headless: true }],
    },
    include: ["tests/browser/**/*.test.ts"],
    testTimeout: 30000,
  },
});
