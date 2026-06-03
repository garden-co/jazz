import { defineConfig, devices } from "@playwright/test";

const BASE_URL = "http://localhost:5173";

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
    // `dev:e2e` clears the persisted dev-server store before starting, so each
    // run gets a fresh, empty backend. Never reuse a running server, since that
    // would skip the reset and leak rows between runs.
    command: "pnpm run dev:e2e",
    url: BASE_URL,
    reuseExistingServer: false,
    timeout: 120_000,
  },
});
