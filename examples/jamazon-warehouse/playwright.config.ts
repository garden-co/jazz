import { randomUUID } from "node:crypto";
import { defineConfig, devices } from "@playwright/test";

// E2E_PORT lets the suite run beside another local app on port 3000.
const PORT = Number(process.env.E2E_PORT ?? 3000);
const BASE_URL = `http://localhost:${PORT}`;
const PROD = process.env.JAZZ_E2E_PROD === "1";

export default defineConfig({
  testDir: "./e2e",
  testMatch: "**/*.spec.ts",
  timeout: 180_000,
  fullyParallel: false,
  workers: 1,
  use: { baseURL: BASE_URL, trace: "retain-on-failure" },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: PROD
    ? {
        // The examples E2E gate (dev/examples-e2e/run.mjs) builds the app and
        // supplies a fresh app id and data directory.
        command: "pnpm start",
        url: BASE_URL,
        reuseExistingServer: false,
        timeout: 180_000,
      }
    : {
        // A fresh app per run, so the deterministic seed (and its five scarce
        // amps) starts over every time.
        command: `node node_modules/next/dist/bin/next dev -p ${PORT}`,
        env: {
          NEXT_PUBLIC_APP_ORIGIN: BASE_URL,
          NEXT_PUBLIC_JAZZ_APP_ID: process.env.NEXT_PUBLIC_JAZZ_APP_ID ?? randomUUID(),
          // Jazz runs inside the app; a fresh data directory per run as well.
          JAZZ_DATA_PATH: `.jazz/e2e-${randomUUID()}`,
        },
        url: BASE_URL,
        reuseExistingServer: false,
        timeout: 180_000,
      },
});
