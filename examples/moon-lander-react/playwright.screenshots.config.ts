import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./walkthrough",
  testMatch: "screenshots.test.ts",
  timeout: 120_000,
  reporter: "list",
  globalSetup: "./walkthrough/screenshots-global-setup.ts",
  use: {
    baseURL: "http://localhost:5173",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
  webServer: {
    command: "npx vite",
    port: 5173,
    reuseExistingServer: true,
  },
});
