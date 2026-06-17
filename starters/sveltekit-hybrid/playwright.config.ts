import { defineConfig, devices } from "@playwright/test";

const BASE_URL = "http://localhost:5173";
const PROD = process.env.JAZZ_E2E_PROD === "1";

export default defineConfig({
  testDir: "./e2e",
  testMatch: "**/*.spec.ts",
  timeout: 90_000,
  fullyParallel: false,
  workers: 1,
  retries: process.env.CI ? 2 : 0,
  use: {
    baseURL: BASE_URL,
    trace: "on-first-retry",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
  webServer: {
    command: PROD ? "node --env-file=.env build" : "pnpm dev",
    env: PROD
      ? { PORT: "5173", ORIGIN: BASE_URL }
      : { BETTER_AUTH_SECRET: "test-secret-do-not-use-in-production" },
    url: BASE_URL,
    reuseExistingServer: false,
    timeout: PROD ? 120_000 : 60_000,
  },
});
