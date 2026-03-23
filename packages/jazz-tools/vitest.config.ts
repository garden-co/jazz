import { defineConfig, type Plugin } from "vitest/config";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import { builtinModules } from "node:module";
import { fileURLToPath } from "node:url";

// sqlite was added in Node.js 22.5 and isn't in the standard builtins list
const allBuiltins = [...builtinModules, "sqlite"];
const allBuiltinsWithPrefix = allBuiltins.flatMap((m) => [m, `node:${m}`]);
const jazzRnVitestStub = fileURLToPath(
  new URL("./test-support/jazz-rn-vitest-stub.ts", import.meta.url),
);

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
  plugins: [nodeSqlitePlugin(), svelte()],
  resolve: {
    alias: {
      // Node-side Vitest runs should not load the real RN native bridge package.
      // Stubbing jazz-rn keeps Vite 8 from traversing React Native and UniFFI internals.
      "jazz-rn": jazzRnVitestStub,
    },
  },
  test: {
    // Use Node environment for node:sqlite support
    environment: "node",
    // Use forks pool to avoid Vite's module resolution issues with node: builtins
    pool: "forks",
    include: ["src/**/*.test.ts", "tests/ts-dsl/**/*.test.ts"],
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
