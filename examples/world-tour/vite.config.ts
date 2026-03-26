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
  server: {
    proxy: {
      "/jazz": {
        target: "http://127.0.0.1:4200",
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/jazz/, ""),
      },
    },
  },
});
