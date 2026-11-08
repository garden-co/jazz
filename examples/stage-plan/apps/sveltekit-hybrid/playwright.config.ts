import { defineConfig, devices } from "@playwright/test";

// `pnpm test:e2e` drives `pnpm dev`. The examples E2E gate
// (dev/examples-e2e/run.mjs) builds the app first and sets JAZZ_E2E_PROD=1 to
// drive the production server (`node build`, port 3000) instead.
const PROD = process.env.JAZZ_E2E_PROD === "1";
const ORIGIN = process.env.APP_ORIGIN || (PROD ? "http://localhost:3000" : "http://localhost:5173");

export default defineConfig({
  testDir: "./e2e",
  testMatch: "**/*.spec.ts",
  timeout: 180_000,
  fullyParallel: false,
  workers: 1,
  use: { baseURL: ORIGIN, trace: "retain-on-failure" },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: {
    command: PROD ? "pnpm start" : "pnpm dev",
    url: ORIGIN,
    reuseExistingServer: !PROD,
    timeout: 180_000,
  },
});
