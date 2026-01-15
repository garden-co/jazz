import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./tests",
  fullyParallel: false, // Sync tests need sequential execution
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: 1,
  reporter: "html",
  use: {
    baseURL: "http://localhost:5174",
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
      // Sync server (groove-server)
      command: "cargo run -p groove-server",
      cwd: "../../crates",
      url: "http://localhost:8080/",
      reuseExistingServer: !process.env.CI,
      timeout: 120000, // Give cargo time to compile
    },
    {
      // Vite dev server
      command: "npm run dev",
      url: "http://localhost:5174",
      reuseExistingServer: !process.env.CI,
    },
  ],
});
