import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./tests/browser",
  testMatch: "**/*.spec.ts",
  timeout: 30_000,
  fullyParallel: false,
  workers: 1,
  globalSetup: "./tests/browser/global-setup.ts",
  use: {
    headless: true,
  },
});
