import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/index.ts", "src/config/tailwind.config.ts"],
  format: ["esm"],
  dts: true,
  sourcemap: true,
  clean: true,
  external: ["react", "react-dom", "tailwindcss"],
  treeshake: true,
  splitting: false,
});
