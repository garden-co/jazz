import { defineConfig, devices } from "@playwright/test";
import dotenv from "dotenv";

dotenv.config();

const SERVER_URL = process.env.NEXT_PUBLIC_SYNC_SERVER_URL!;
const APP_ID = process.env.APP_ID!;
const BACKEND_SECRET = process.env.BACKEND_SECRET!;
const ADMIN_SECRET = process.env.ADMIN_SECRET!;
const WEB_PORT = Number(process.env.WEB_PORT ?? "3000");

export default defineConfig({
  testDir: "./e2e",
  testMatch: "**/*.spec.ts",
  timeout: 90_000,
  fullyParallel: false,
  workers: 1,
  retries: process.env.CI ? 2 : 0,
  use: {
    baseURL: `http://localhost:${WEB_PORT}`,
    trace: "on-first-retry",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
  webServer: [
    {
      command: "pnpm run sync-server",
      url: `${SERVER_URL}/health`,
      reuseExistingServer: !process.env.CI,
      timeout: 60_000,
      env: {
        NEXT_PUBLIC_APP_ID: APP_ID,
        BACKEND_SECRET,
        ADMIN_SECRET,
        JAZZ_SERVER_PORT: String(new URL(SERVER_URL).port),
      },
    },
    {
      command: `pnpm dev --hostname 127.0.0.1 --port ${WEB_PORT}`,
      url: `http://localhost:${WEB_PORT}`,
      reuseExistingServer: !process.env.CI,
      timeout: 60_000,
      env: {
        NEXT_PUBLIC_SYNC_SERVER_URL: SERVER_URL,
        NEXT_PUBLIC_APP_ID: APP_ID,
        BACKEND_SECRET,
        ADMIN_SECRET,
      },
    },
  ],
});
