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
    command: "node ../../dev/examples/with-local-core.mjs pnpm exec vite dev --port 5176",
    port: 5176,
    reuseExistingServer: false,
    cwd: import.meta.dirname,
    env: {
      // Walkthrough screenshots start from an empty core, so the demo tour is always fresh.
      JAZZ_IN_MEMORY: "1",
    },
  },
});
