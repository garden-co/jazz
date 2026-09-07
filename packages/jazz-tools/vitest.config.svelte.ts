import { defineConfig } from "vitest/config";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import { resolve } from "node:path";

const sealedWasmPackage = process.env.JAZZ_CORRECTNESS_WASM_PACKAGE;
if (process.env.JAZZ_CORRECTNESS_ARTIFACT_RUN === "1" && !sealedWasmPackage)
  throw new Error("sealed correctness consumer is missing its admitted WASM package");

export default defineConfig({
  plugins: [svelte()],
  resolve: {
    conditions: ["browser"],
    alias: {
      ...(sealedWasmPackage ? { "jazz-wasm": resolve(sealedWasmPackage, "jazz_wasm.js") } : {}),
      $lib: resolve("../../starters/sveltekit-betterauth/src/lib"),
      "$env/dynamic/public": resolve("tests/svelte/starter-env.ts"),
    },
  },
  test: {
    environment: "jsdom",
    include: ["src/**/*.svelte.test.ts"],
  },
});
