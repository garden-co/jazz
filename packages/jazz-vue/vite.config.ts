import path from "path";
import vue from "@vitejs/plugin-vue";
import { defineConfig } from "vite";
import dts from "vite-plugin-dts";

export default defineConfig({
  // @ts-expect-error types
  plugins: [vue(), dts({ include: ["src/**/*.ts"], outDir: "dist" })],
  build: {
    lib: {
      entry: path.resolve(__dirname, "src/index.ts"),
      name: "JazzVue",
      formats: ["es"],
      fileName: (format) => `index.js`,
    },
    rollupOptions: {
      external: ["vue", "jazz-browser", "jazz-tools"],
      output: {
        globals: {
          vue: "Vue",
          "jazz-browser": "JazzBrowser",
          "jazz-tools": "JazzTools",
        },
      },
    },
  },
});
