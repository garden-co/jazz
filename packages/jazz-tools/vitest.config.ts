import { svelte } from "@sveltejs/vite-plugin-svelte";
import { svelteTesting } from "@testing-library/svelte/vite";
import { defineProject } from "vitest/config";
import { fileURLToPath } from "url";
import { dirname, resolve } from "path";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

export default defineProject({
  plugins: [
    svelte({
      // Exclude dist files from processing
      exclude: ["**/dist/**"],
    }),
    svelteTesting({
      resolveBrowser: false,
    }),
  ],
  resolve: {
    // 'browser' for Svelte Testing Library
    // 'node' for "msw/node"
    // '@jazz-tools/source' to use source files instead of dist during tests
    conditions: ["@jazz-tools/source", "browser", "node"],
    alias: {
      // Force source resolution for jazz-tools/svelte during tests
      "jazz-tools/svelte": resolve(__dirname, "./src/svelte/index.ts"),
    },
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
