import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./walkthrough",
  testMatch: "screenshots.test.ts",
  timeout: 180_000,
  reporter: "list",
  use: {
    baseURL: "http://localhost:5176",
  },
  projects: [
    {
      name: "chromium",
      use: {
        ...devices["Desktop Chrome"],
        launchOptions: {
          args: ["--use-gl=angle", "--use-angle=swiftshader", "--enable-unsafe-swiftshader"],
        },
      },
    },
  ],
  webServer: {
    command: "pnpm exec vite dev --port 5176",
    port: 5176,
    reuseExistingServer: false,
    cwd: import.meta.dirname,
    env: {
      VITE_E2E: "true",
    },
  },
});
