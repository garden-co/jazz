import react from "@vitejs/plugin-react-swc";
import { playwright } from "@vitest/browser-playwright";
import { defineProject } from "vitest/config";
import { customCommands } from "./src/commands";

export default defineProject({
  plugins: [react()],
  test: {
    name: "browser-integration-tests",
    browser: {
      enabled: true,
      provider: playwright(),
      instances: [{ browser: "chromium", headless: true }],
      commands: customCommands,
    },
    include: ["src/**/*.test.ts"],
    testTimeout: process.env.CI ? 60_000 : 10_000,
  },
});
