import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./tests",
  timeout: 90_000,
  workers: 1,
  use: { baseURL: "http://localhost:5187" },
  webServer: {
    command: "node node_modules/vite/bin/vite.js --mode test --port 5187 --strictPort",
    url: "http://localhost:5187",
    timeout: 120_000,
  },
});
