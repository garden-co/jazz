import { defineConfig } from "vitest/config";

// Pure-function unit tests only (the diagram engine's geometry/router/layout
// layer, and the agent search Markdown renderer). No DOM environment needed — path-string and coordinate maths.
export default defineConfig({
  test: {
    environment: "node",
    include: [
      "components/**/*.test.ts",
      "lib/agent-search.test.ts",
      "lib/search-analytics.test.ts",
      "lib/copy-analytics.test.ts",
      "lib/sidebar-rules.test.ts",
      "lib/search-include-refs.test.ts",
    ],
    exclude: ["**/node_modules/**", "**/.next/**"],
  },
});
