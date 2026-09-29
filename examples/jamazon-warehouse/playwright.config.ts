import { randomUUID } from "node:crypto";
import { defineConfig, devices } from "@playwright/test";

const BASE_URL = "http://localhost:3000";

export default defineConfig({
  testDir: "./e2e",
  testMatch: "**/*.spec.ts",
  timeout: 180_000,
  fullyParallel: false,
  workers: 1,
  use: { baseURL: BASE_URL, trace: "retain-on-failure" },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: {
    // A fresh app per run, so the deterministic seed (and its five scarce
    // amps) starts over every time.
    command: "node node_modules/next/dist/bin/next dev",
    env: { NEXT_PUBLIC_JAZZ_APP_ID: process.env.NEXT_PUBLIC_JAZZ_APP_ID ?? randomUUID() },
    url: BASE_URL,
    reuseExistingServer: false,
    timeout: 180_000,
  },
});
