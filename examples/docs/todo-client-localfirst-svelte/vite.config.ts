import { defineConfig } from "vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import { jazzPlugin } from "jazz-tools/dev/vite";
import { fileURLToPath } from "node:url";

export default defineConfig({
  resolve: {
    alias: {
      "jazz-tools/svelte": fileURLToPath(
        new URL("../../../packages/jazz-tools/src/svelte/index.ts", import.meta.url),
      ),
    },
  },
  plugins: [svelte(), jazzPlugin()],
});
