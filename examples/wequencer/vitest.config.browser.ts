import { defineConfig } from "vitest/config";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import { playwright } from "@vitest/browser-playwright";

export default defineConfig({
  plugins: [wasm(), topLevelAwait(), svelte({ hot: false })],
  worker: {
    plugins: () => [wasm(), topLevelAwait()],
  },
  test: {
    browser: {
      enabled: true,
      provider: playwright(),
      instances: [{ browser: "chromium", headless: true }],
    },
    include: ["schema/**/*.test.ts", "src/**/*.test.ts"],
    testTimeout: 30000,
  },
});
