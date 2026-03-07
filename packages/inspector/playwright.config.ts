import { defineConfig } from "@playwright/test";

const INSPECTOR_DEV_SERVER_PORT = 41737;

export default defineConfig({
  testDir: "./tests/browser",
  testMatch: "**/*.spec.ts",
  timeout: 30_000,
  fullyParallel: false,
  workers: 1,
  globalSetup: "./tests/browser/global-setup.ts",
  use: {
    baseURL: `http://127.0.0.1:${INSPECTOR_DEV_SERVER_PORT}`,
    headless: true,
  },
  webServer: {
    command: `pnpm dev --host 127.0.0.1 --port ${INSPECTOR_DEV_SERVER_PORT}`,
    url: `http://127.0.0.1:${INSPECTOR_DEV_SERVER_PORT}`,
    reuseExistingServer: !process.env.CI,
    timeout: 60_000,
  },
});
