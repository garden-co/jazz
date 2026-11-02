import { sveltekit } from "@sveltejs/kit/vite";
import { jazzSvelteKit } from "jazz-tools/dev/sveltekit";
import { defineConfig } from "vite";

export default defineConfig({
  // Jazz runs inside this app (src/lib/server/jazz.ts), so the plugin only
  // wires up the browser runtime: no separate dev sync server.
  plugins: [sveltekit(), jazzSvelteKit({ server: false })],
});
