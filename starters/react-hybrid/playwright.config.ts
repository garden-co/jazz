import { defineConfig, devices } from "@playwright/test";

const BASE_URL = "http://localhost:5173";
const PROD = process.env.JAZZ_E2E_PROD === "1";

const prodApiServer = {
  command: "node --env-file=.env server-dist/index.js",
  env: { PORT: "3001" },
  url: "http://localhost:3001/health",
  reuseExistingServer: false,
  timeout: 60_000,
};

const prodFrontend = {
  command: "pnpm exec vite preview --port 5173 --strictPort",
  url: BASE_URL,
  reuseExistingServer: false,
  timeout: 60_000,
};

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
  webServer: PROD
    ? [prodApiServer, prodFrontend]
    : {
        command: "pnpm dev",
        env: { BETTER_AUTH_SECRET: "test-secret-do-not-use-in-production" },
        url: BASE_URL,
        reuseExistingServer: false,
        timeout: 60_000,
      },
});
