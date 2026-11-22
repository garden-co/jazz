import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    entry: ["src/index.ts"],
    // Both the CLI shim and the package export load dist/index.js.
    fixedExtension: false,
  },
  test: {
    environment: "node",
    include: ["src/**/*.test.ts"],
  },
});
