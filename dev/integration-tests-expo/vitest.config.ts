import { defineConfig } from "vitest/config";

// Node-only unit tests for the pure-TS runner modules (expect shim, support
// helpers, sequential runner). RN test bodies, UI and App are NOT run here —
// they are validated by `tsc --noEmit` and on-device in CI.
export default defineConfig({
  test: {
    environment: "node",
    include: ["runner/**/*.test.ts"],
  },
});
