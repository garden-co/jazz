import { sveltekit } from "@sveltejs/kit/vite";
import { defineConfig } from "vite";

export default defineConfig({
  // Jazz runs inside this app (src/lib/server/jazz.ts): no separate sync server.
  plugins: [sveltekit()],
  // jazz-wasm is wasm-bindgen output, which Vite's dependency pre-bundler cannot handle.
  optimizeDeps: { exclude: ["jazz-wasm"] },
  // jazz-napi is a native addon: Node loads it at runtime, the SSR bundle leaves it alone.
  ssr: { external: ["jazz-napi"] },
});
