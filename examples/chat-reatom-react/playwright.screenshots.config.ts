import { defineConfig, devices } from "@playwright/test";
import { WALKTHROUGH_PORT, WALKTHROUGH_APP_ID } from "./walkthrough/walkthrough-constants.js";

export default defineConfig({
  testDir: "./walkthrough",
  testMatch: "screenshots.test.ts",
  timeout: 180_000,
  reporter: "list",
  globalSetup: "./walkthrough/global-setup.ts",
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
    reuseExistingServer: false,
    env: {
      VITE_JAZZ_APP_ID: WALKTHROUGH_APP_ID,
      VITE_JAZZ_SERVER_URL: `http://127.0.0.1:${WALKTHROUGH_PORT}`,
    },
  },
});
