import { sveltekit } from "@sveltejs/kit/vite";
import { jazzSvelteKit } from "jazz-tools/dev/sveltekit";
import { defineConfig } from "vite";

export default defineConfig({
  // jazzSvelteKit must come before sveltekit so that it populates
  // process.env.PUBLIC_JAZZ_* before SvelteKit's plugin captures the
  // public env for $env/dynamic/public.
  plugins: [jazzSvelteKit(), sveltekit()],
  server: {
    fs: {
      allow: ["../.."],
    },
  },
});
