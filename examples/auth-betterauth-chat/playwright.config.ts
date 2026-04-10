import { defineConfig, devices } from "@playwright/test";
import { APP_ORIGIN, DEFAULT_APP_ID } from "./constants";

export default defineConfig({
  testDir: "./e2e",
  testMatch: "**/*.spec.ts",
  timeout: 90_000,
  fullyParallel: false,
  workers: 1,
  retries: process.env.CI ? 2 : 0,
  globalSetup: "./e2e/global-setup.ts",
  use: {
    baseURL: APP_ORIGIN,
    trace: "on-first-retry",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
  webServer: {
    command: `pnpm dev --hostname 127.0.0.1`,
    url: APP_ORIGIN,
    reuseExistingServer: !process.env.CI,
    timeout: 60_000,
  },
});
