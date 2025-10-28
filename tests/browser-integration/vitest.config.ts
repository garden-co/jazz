import react from "@vitejs/plugin-react-swc";
import { defineProject } from "vitest/config";
import { customCommands } from "./src/commands";

export default defineProject({
  plugins: [react()],
  test: {
    name: "browser-integration-tests",
    browser: {
      enabled: true,
      provider: "playwright",
      headless: true,
      instances: [{ browser: "chromium" }],
      commands: customCommands,
    },
    testTimeout: process.env.CI ? 60_000 : 10_000,
  },
});
