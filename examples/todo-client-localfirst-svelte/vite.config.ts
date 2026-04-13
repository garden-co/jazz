import { defineConfig } from "vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import { jazzSvelteKit } from "jazz-tools/dev/sveltekit";

export default defineConfig({
  plugins: [svelte(), jazzSvelteKit()],
  build: { target: "es2020" },
  worker: { format: "es" },
  optimizeDeps: {
    exclude: ["jazz-wasm"],
  },
});
