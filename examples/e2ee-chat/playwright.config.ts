import { defineConfig } from "@playwright/test";
export default defineConfig({
  testDir: "./e2e",
  timeout: 90000,
  expect: { timeout: 30000 },
  workers: 1,
  use: { baseURL: "http://127.0.0.1:5183", trace: "retain-on-failure" },
  webServer: {
    command: "pnpm dev --host 127.0.0.1 --port 5183 --strictPort",
    url: "http://127.0.0.1:5183",
    timeout: 120000,
    reuseExistingServer: false,
  },
});
