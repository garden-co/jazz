import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";
import { jazzBackend } from "./jazz.server.js";

export default defineConfig({
  plugins: [
    vue(),
    jazzBackend({ dataDir: process.env.JAZZ_IN_MEMORY === "1" ? undefined : ".jazz" }),
  ],
  // jazz-wasm is wasm-bindgen output, which Vite's dependency pre-bundler cannot handle.
  optimizeDeps: { exclude: ["jazz-wasm"] },
});
