import { defineConfig } from "@playwright/test";
export default defineConfig({
  testDir: "./e2e",
  timeout: 180000,
  expect: { timeout: 30000 },
  workers: 1,
  use: { baseURL: "http://127.0.0.1:5183", trace: "retain-on-failure" },
});
