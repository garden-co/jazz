import path from "node:path";
import vue from "@vitejs/plugin-vue";
import depsExternal from "rollup-plugin-node-externals";
import { defineConfig } from "vite";
import dts from "vite-plugin-dts";

export default defineConfig({
  plugins: [
    vue(),
    dts({ include: ["src/**/*.ts"], outDir: "dist" }),
    depsExternal(),
  ],
  build: {
    lib: {
      entry: {
        index: path.resolve(__dirname, "src/index.ts"),
        testing: path.resolve(__dirname, "src/testing.ts"),
      },
      name: "JazzVue",
      formats: ["es"],
      fileName: (_, entryName) => `${entryName}.js`,
    },
    rollupOptions: {
      external: ["vue"],
    },
    sourcemap: true,
    minify: false,
  },
});
