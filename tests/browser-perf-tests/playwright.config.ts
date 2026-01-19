import { defineConfig, devices } from "@playwright/test";
import isCI from "is-ci";

/**
 * Playwright configuration for browser benchmarks.
 * Note: This config is primarily for the benchmark CLI, not for running tests.
 * The benchmark CLI uses Playwright directly, not through the test runner.
 */
export default defineConfig({
  testDir: "./src/cli",

  /* Run tests in files in parallel */
  fullyParallel: false, // Benchmarks should run sequentially for consistency

  /* Fail the build on CI if you accidentally left test.only in the source code */
  forbidOnly: isCI,

  /* Retry on CI only */
  retries: 0, // No retries for benchmarks

  /* Single worker for consistent benchmark results */
  workers: 1,

  /* Reporter to use */
  reporter: "list",

  /* Shared settings for all the projects below */
  use: {
    /* Base URL to use in actions like `await page.goto('/')` */
    baseURL: "http://localhost:5173/",

    /* Collect trace when retrying the failed test */
    trace: "off",

    /* No screenshots for benchmarks */
    screenshot: "off",

    /* No video for benchmarks */
    video: "off",
  },

  /* Configure projects for major browsers */
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],

  /* Run your local dev server before starting the tests */
  webServer: [
    {
      command: "pnpm preview --port 5173",
      url: "http://localhost:5173/",
      reuseExistingServer: !isCI,
    },
    {
      command: "pnpm sync --in-memory",
      url: "http://localhost:4200/health",
      reuseExistingServer: !isCI,
    },
  ],
});
