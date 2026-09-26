import { defineConfig, devices } from "@playwright/test";

const BASE_URL = "http://localhost:3000";
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
    trace: "retain-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
  webServer: {
    // Run the server binary directly: pnpm 12.6 `pnpm exec` starts its child in a
    // new process group, which Playwright's shutdown does not kill, so the run hangs.
    command: PROD ? "node node_modules/next/dist/bin/next start" : "pnpm dev",
    env: PROD ? {} : { BETTER_AUTH_SECRET: "test-secret-do-not-use-in-production" },
    url: BASE_URL,
    reuseExistingServer: false,
    timeout: PROD ? 120_000 : 60_000,
  },
});
