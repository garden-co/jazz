import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./tests",
  testMatch: "**/*.spec.ts", // Only run Playwright spec files, not Vitest test files
  fullyParallel: false, // Run tests serially since they share state
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: 1,
  reporter: "html",
  use: {
    baseURL: "http://localhost:5180",
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
      command: "npm run dev",
      url: "http://localhost:5180",
      reuseExistingServer: !process.env.CI,
    },
    {
      command:
        "cargo run -p groove-server --manifest-path crates/Cargo.toml -- --port 8080",
      url: "http://localhost:8080",
      reuseExistingServer: !process.env.CI,
      cwd: "../..",
      timeout: 120000, // Rust build can take a while
    },
  ],
});
