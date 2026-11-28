import { defineConfig, devices } from "@playwright/test";

// `pnpm test:e2e` drives `pnpm dev`. The examples E2E gate
// (dev/examples-e2e/run.mjs) builds the app first and sets JAZZ_E2E_PROD=1 to
// drive the production server instead.
const ORIGIN = process.env.APP_ORIGIN || "http://127.0.0.1:3000";
const PROD = process.env.JAZZ_E2E_PROD === "1";

export default defineConfig({
  testDir: "./e2e",
  testMatch: "**/*.spec.ts",
  // Fail a test if the app or one of its controls stops responding.
  timeout: 60_000,
  fullyParallel: false,
  workers: 1,
  use: { baseURL: ORIGIN, trace: "retain-on-failure", actionTimeout: 30_000 },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: {
    command: PROD ? "pnpm start" : "pnpm dev",
    url: ORIGIN,
    // These browser contexts share the same sign-in quota on the test server.
    env: PROD ? { BANDCHAT_E2E_DISABLE_AUTH_RATE_LIMIT: "1" } : undefined,
    reuseExistingServer: !PROD,
    timeout: 180_000,
  },
});
