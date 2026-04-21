import { defineConfig, devices } from "@playwright/test";
import dotenv from "dotenv";
import path from "path";
// Read from ".env" file.
dotenv.config({ path: path.resolve(import.meta.dirname, ".env") });

export default defineConfig({
  testDir: "./e2e",
  testMatch: "**/*.spec.ts",
  timeout: 90_000,
  fullyParallel: false,
  workers: 1,
  retries: process.env.CI ? 2 : 0,
  globalSetup: "./e2e/global-setup.ts",
  use: {
    baseURL: process.env.NEXT_PUBLIC_APP_ORIGIN!,
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
    url: process.env.NEXT_PUBLIC_APP_ORIGIN!,
    reuseExistingServer: !process.env.CI,
    timeout: 60_000,
  },
});
