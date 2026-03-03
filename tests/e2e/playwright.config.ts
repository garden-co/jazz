import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./cases",
  testMatch: "**/*.spec.ts",
  timeout: 90_000,
  fullyParallel: false,
  workers: 1,
  use: {
    baseURL: `http://localhost:5173`,
    headless: true,
  },
  webServer: {
    command: `pnpm dev --host 127.0.0.1 --port 5173`,
    reuseExistingServer: !process.env.CI,
    url: `http://localhost:5173`,
  },
});
