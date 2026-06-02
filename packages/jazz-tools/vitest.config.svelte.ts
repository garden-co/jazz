import { defineConfig } from "vitest/config";
import { svelte } from "@sveltejs/vite-plugin-svelte";

export default defineConfig({
  plugins: [svelte({ hot: false })],
  // Resolve Svelte to its browser build so client APIs like `mount` work under
  // jsdom (without this, `svelte` resolves to the server build and `mount`
  // throws `lifecycle_function_unavailable`).
  resolve: { conditions: ["browser"] },
  test: {
    environment: "jsdom",
    include: ["src/**/*.svelte.test.ts"],
  },
});
