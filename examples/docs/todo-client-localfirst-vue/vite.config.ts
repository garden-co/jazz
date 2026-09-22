import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";
import { jazzPlugin } from "jazz-tools/dev/vite";

export default defineConfig({
  plugins: [vue(), jazzPlugin()],
});
