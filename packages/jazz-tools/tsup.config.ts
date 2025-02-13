import { defineConfig } from "tsup";

export default defineConfig({
  entry: {
    index: "src/index.ts",
    testing: "src/testing.ts",
  },
  format: ["esm"],
  dts: false,
  sourcemap: true,
  clean: true,
  minify: false,
});
