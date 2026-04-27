import { sveltekit } from "@sveltejs/kit/vite";
import { jazzSvelteKit } from "jazz-tools/dev/sveltekit";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [sveltekit(), jazzSvelteKit()],
  server: {
    fs: {
      allow: ["../.."],
    },
  },
});
