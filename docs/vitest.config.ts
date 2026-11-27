import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

// Pure-function unit tests only (the diagram engine's geometry/router/layout
// layer, and the agent search Markdown renderer). No DOM environment needed — path-string and coordinate maths.
export default defineConfig({
  resolve: {
    // For `lib/private-store.test.ts`, which runs the directory store sample.
    alias: [
      {
        find: /^jazz-tools\/backend$/,
        replacement: fileURLToPath(new URL("lib/private-store-sample-backend.ts", import.meta.url)),
      },
    ],
  },
  test: {
    environment: "node",
    include: [
      "components/**/*.test.ts",
      "lib/agent-search.test.ts",
      "lib/search-analytics.test.ts",
      "lib/copy-analytics.test.ts",
      "lib/sidebar-rules.test.ts",
      "lib/search-include-refs.test.ts",
      "lib/bundle-sizes/**/*.test.ts",
      "lib/jazz-version/**/*.test.ts",
      "lib/languages/**/*.test.ts",
      "lib/code-tokenizers.test.ts",
      "lib/app-file-blocks.test.ts",
      "lib/llm-text.test.ts",
      "lib/quickstart-links.test.ts",
      "lib/private-store.test.ts",
      "lib/rotation-retries.test.ts",
    ],
    exclude: ["**/node_modules/**", "**/.next/**"],
  },
});
