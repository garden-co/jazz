import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./e2e",
  testMatch: "**/*.spec.ts",
  fullyParallel: false,
  workers: 1,
  timeout: 90_000,
  retries: process.env.CI ? 2 : 0,
  use: {
    baseURL: "http://localhost:5176",
    trace: "on-first-retry",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
  webServer: {
    command: "pnpm dev --host 127.0.0.1 --port 5176",
    url: "http://localhost:5176",
    reuseExistingServer: !process.env.CI,
    timeout: 60_000,
    env: {
      VITE_JAZZ_APP_ID: "00000000-0000-0000-0000-000000000123",
      VITE_E2E: "true",
    },
  },
});
