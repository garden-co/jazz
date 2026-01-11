import react from "@vitejs/plugin-react";
import topLevelAwait from "vite-plugin-top-level-await";
import wasm from "vite-plugin-wasm";
import { defineConfig } from "vitest/config";

export default defineConfig({
  plugins: [react(), wasm(), topLevelAwait()],
  optimizeDeps: {
    exclude: ["groove-wasm"],
  },
  test: {
    // Use browser environment for WASM and React support
    browser: {
      enabled: true,
      name: "chromium",
      provider: "playwright",
      headless: true,
    },
    include: ["test/**/*.test.ts", "test/**/*.test.tsx"],
  },
});
