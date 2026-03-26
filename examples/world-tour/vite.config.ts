import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";

export default defineConfig({
  plugins: [vue()],
  optimizeDeps: {
    exclude: ["jazz-wasm"],
  },
  build: {
    target: "es2020",
  },
  worker: {
    format: "es",
  },
});
