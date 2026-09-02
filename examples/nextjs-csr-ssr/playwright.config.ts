import { randomUUID } from "node:crypto";
import { defineConfig, devices } from "@playwright/test";

const WEB_PORT = Number(process.env.WEB_PORT ?? "3000");
const testAppId = randomUUID();

export default defineConfig({
  testDir: "./e2e",
  testMatch: "**/*.spec.ts",
  timeout: 90_000,
  testIgnore: "**/config-security.spec.ts",
  fullyParallel: false,
  workers: 1,
  retries: process.env.CI ? 2 : 0,
  use: {
    baseURL: `http://127.0.0.1:${WEB_PORT}`,
    trace: "on-first-retry",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
  webServer: {
    url: `http://127.0.0.1:${WEB_PORT}`,
    command: `pnpm dev --hostname 127.0.0.1 --port ${WEB_PORT}`,
    timeout: 60_000,
    env: {
      NEXT_PUBLIC_JAZZ_APP_ID: testAppId,
      NEXT_PUBLIC_JAZZ_SERVER_URL: "",
      BACKEND_SECRET: "",
      JAZZ_BACKEND_SECRET: "",
      ADMIN_SECRET: "",
      JAZZ_ADMIN_SECRET: "",
      JAZZ_E2E_IN_MEMORY: "1",
    },
  },
});
