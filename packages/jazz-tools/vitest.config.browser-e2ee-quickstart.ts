import { resolve } from "node:path";
import { mergeConfig, type ViteUserConfig } from "vitest/config";
import sqlite from "./vitest.config.browser-e2ee-sqlite.js";
import { quickstartScenario } from "./tests/sqlite-e2ee/quickstart-node.js";

const config = mergeConfig(sqlite as ViteUserConfig, {
  optimizeDeps: {
    include: ["react", "react-dom/client", "react/jsx-runtime", "react/jsx-dev-runtime"],
  },
  resolve: {
    dedupe: ["react", "react-dom"],
    alias: [
      {
        find: /^jazz-tools\/_dev\/schema-hash$/,
        replacement: resolve(__dirname, "src/_dev/schema-hash.ts"),
      },
      { find: /^jazz-tools$/, replacement: resolve(__dirname, "src/index.ts") },
      { find: /^jazz-tools\/e2ee$/, replacement: resolve(__dirname, "src/e2ee/index.ts") },
      { find: /^jazz-tools\/react$/, replacement: resolve(__dirname, "src/react/index.ts") },
      {
        find: /^jazz-tools\/permissions$/,
        replacement: resolve(__dirname, "src/permissions/index.ts"),
      },
    ],
  },
  test: {
    browser: {
      commands: {
        sqliteQuickstartScenario: async ({ context, page }, input) =>
          quickstartScenario(context, page, input),
      },
    },
  },
});
// The quickstart deploys its real generated membership policies to a fresh
// authority, separate from the explicit-policy SDK suite's catalogue.
config.test.include = ["tests/sqlite-e2ee/quickstart.test.ts"];
export default config;
