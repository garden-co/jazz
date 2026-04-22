import { defineConfig } from "vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import { jazzPlugin } from "jazz-tools/dev/vite";

export default defineConfig({
  plugins: [svelte(), jazzPlugin()],
});
