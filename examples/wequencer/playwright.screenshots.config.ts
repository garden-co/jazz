import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./walkthrough",
  testMatch: "screenshots.test.ts",
  timeout: 120_000,
  reporter: "list",
  globalSetup: "./e2e/global-setup.ts",
  use: {
    baseURL: "http://localhost:5175",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
  webServer: {
    command: "npx vite dev --port 5175",
    port: 5175,
    reuseExistingServer: true,
    env: {
      VITE_JAZZ_SERVER_URL: `http://127.0.0.1:19878`,
      VITE_JAZZ_SERVER_PORT: "19878",
      VITE_JAZZ_APP_ID: "00000000-0000-0000-0000-000000000099",
      VITE_E2E: "true",
    },
  },
});
