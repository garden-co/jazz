import { defineConfig, type Plugin } from "vitest/config";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import { builtinModules } from "node:module";

// sqlite was added in Node.js 22.5 and isn't in the standard builtins list
const allBuiltins = [...builtinModules, "sqlite"];
const allBuiltinsWithPrefix = allBuiltins.flatMap((m) => [m, `node:${m}`]);

// Plugin to handle node:sqlite resolution - use native require
function nodeSqlitePlugin(): Plugin {
  return {
    name: "node-sqlite",
    enforce: "pre",
    async resolveId(id) {
      // Treat node:sqlite and sqlite as needing special handling
      if (id === "node:sqlite" || id === "sqlite") {
        return `\0virtual:node-sqlite`;
      }
      return null;
    },
    async load(id) {
      if (id === `\0virtual:node-sqlite`) {
        // Use createRequire to load the native node:sqlite
        return `
          import { createRequire } from 'node:module';
          const require = createRequire(import.meta.url);
          const sqlite = require('node:sqlite');
          export const { DatabaseSync, StatementSync, constants, backup } = sqlite;
        `;
      }
      return null;
    },
  };
}

export default defineConfig({
  plugins: [nodeSqlitePlugin(), svelte({ hot: false })],
  test: {
    // Use Node environment for node:sqlite support
    environment: "node",
    // Use forks pool to avoid Vite's module resolution issues with node: builtins
    pool: "forks",
    include: ["src/**/*.test.ts", "tests/ts-dsl/**/*.test.ts", "tests/codegen/**/*.test.ts"],
    exclude: ["tests/browser/**", "node_modules/**"],
    server: {
      deps: {
        // Force these to be treated as external and not bundled
        external: allBuiltinsWithPrefix,
      },
    },
  },
  // Mark all node: modules as external for SSR
  ssr: {
    external: allBuiltinsWithPrefix,
    noExternal: true,
  },
});
