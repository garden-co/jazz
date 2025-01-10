import { defineConfig } from "tsup";

export default defineConfig({
  entry: {
    "index.web": "src/index.web.ts",
    "index.native": "src/index.native.ts",
    testing: "src/testing.ts",
  },
  format: ["esm"],
  dts: false,
  sourcemap: true,
  clean: true,
  minify: false,
});