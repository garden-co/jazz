import { defineConfig } from "@playwright/test";

// Browser/tenant adapter contract only; native server admission has its own gate.
export default defineConfig({
  testDir: "./tests/browser",
  testMatch: "inspector-sessions.contract.ts",
  timeout: 30_000,
  workers: 1,
  use: { headless: true },
});
