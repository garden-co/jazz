import { defineConfig } from "tsup";

const cfg = {
  splitting: true,
  sourcemap: true,
  clean: true,
  treeshake: false,
  dts: false,
  format: ["esm" as const],
};

export default defineConfig([
  {
    ...cfg,
    entry: {
      index: "src/database-adapter/index.ts",
    },
    outDir: "dist/database-adapter",
  },
]);
