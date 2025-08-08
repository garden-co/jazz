import { svelte } from "@sveltejs/vite-plugin-svelte";
import { svelteTesting } from "@testing-library/svelte/vite";
import { defineProject } from "vitest/config";

export default defineProject({
  plugins: [svelte(), svelteTesting()],
  test: {
    name: "jazz-tools",
    typecheck: {
      enabled: true,
      checker: "tsc",
    },
    projects: [
      {
        test: {
          include: ["src/**/*.test.browser.ts"],
          browser: {
            enabled: true,
            provider: "playwright",
            headless: true,
            screenshotFailures: false,
            instances: [{ browser: "chromium" }],
          },
          name: "browser",
        },
      },
      {
        test: {
          include: ["src/**/*.test.{js,ts,tsx,svelte}"],
          name: "unit",
        },
      },
    ],
  },
});
