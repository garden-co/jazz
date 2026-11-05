import { resolve } from "node:path";
import { defineConfig } from "vitest/config";
import { playwright } from "@vitest/browser-playwright";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";
import { verifyManifest } from "../../dev/artifacts/provenance.mjs";

// This suite exercises real browser storage without a native server. Keep its
// artifact check: an unavailable NAPI binding does not permit stale WASM.
const root = resolve(__dirname, "../..");
const mismatch = verifyManifest(root, "wasm", "fast");
if (mismatch)
  throw new Error(`Build the current WASM fast artifact before storage tests: ${mismatch}`);

export default defineConfig({
  plugins: [wasm(), topLevelAwait()],
  resolve: {
    alias: [
      { find: /^jazz-wasm$/, replacement: resolve(root, "crates/jazz-wasm/pkg/jazz_wasm.js") },
    ],
  },
  server: { fs: { allow: [root] } },
  optimizeDeps: {
    include: [
      "@opentelemetry/exporter-logs-otlp-http",
      "@opentelemetry/exporter-trace-otlp-http",
      "@opentelemetry/resources",
      "@opentelemetry/sdk-logs",
      "@opentelemetry/sdk-trace-base",
    ],
  },
  worker: { plugins: () => [wasm(), topLevelAwait()] },
  test: {
    include: ["tests/browser/sqlite-storage.test.ts"],
    browser: {
      enabled: true,
      provider: playwright(),
      instances: [{ browser: "chromium", headless: true }],
    },
    testTimeout: 30000,
  },
});
