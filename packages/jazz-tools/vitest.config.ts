import { svelte } from "@sveltejs/vite-plugin-svelte";
import { svelteTesting } from "@testing-library/svelte/vite";
import { defineProject } from "vitest/config";

export default defineProject({
  plugins: [
    svelte(),
    svelteTesting({
      resolveBrowser: false,
    }),
  ],
  resolve: {
    // 'browser' for Svelte Testing Library
    // 'node' for "msw/node"
    conditions: ["browser", "node"],
  },
  test: {
    name: "jazz-tools",
    include: ["src/**/*.test.{js,ts,tsx,svelte}"],
    typecheck: {
      enabled: true,
      checker: "tsc",
    },
  },
});
