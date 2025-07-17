import path from "node:path";
import vue from "@vitejs/plugin-vue";
import vueJsx from "@vitejs/plugin-vue-jsx";
import depsExternal from "rollup-plugin-node-externals";
import { defineConfig } from "vite";
import dts from "vite-plugin-dts";

export default defineConfig({
  plugins: [
    vue(),
    vueJsx(),
    dts({ include: ["src/**/*.ts", "src/**/*.vue"], outDir: "dist" }),
    depsExternal(),
  ],
  build: {
    lib: {
      entry: {
        index: path.resolve(__dirname, "src/index.ts"),
        testing: path.resolve(__dirname, "src/testing.ts"),
        "inspector/index": path.resolve(__dirname, "src/inspector/index.ts"),
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
