import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./tests",
  testMatch: "**/*.spec.ts",
  fullyParallel: false, // Run tests serially since they share auth state
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: 1,
  reporter: "html",
  use: {
    baseURL: "http://localhost:5173",
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
      command: "npm run dev:auth",
      url: "http://localhost:3001/health",
      reuseExistingServer: !process.env.CI,
      timeout: 30000,
    },
    {
      command: "npm run dev:client",
      url: "http://localhost:5173",
      reuseExistingServer: !process.env.CI,
    },
    {
      // groove-server for sync tests (JWT validation with BetterAuth)
      command:
        "cd ../../crates && cargo run -p groove-server -- --config ../examples/auth-betterauth/groove-server.toml",
      port: 8080, // groove-server listens on this port
      reuseExistingServer: !process.env.CI,
      timeout: 120000, // Cargo build can take time
    },
  ],
});
