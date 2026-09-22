import { defineConfig } from "vitest/config";

// Pure-function unit tests only (the diagram engine's geometry/router/layout
// layer). No DOM environment needed — path-string and coordinate maths.
export default defineConfig({
  test: {
    environment: "node",
    include: ["components/**/*.test.ts"],
    exclude: ["**/node_modules/**", "**/.next/**"],
  },
});
