import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";
import { jazzPlugin } from "jazz-tools/dev";

export default defineConfig({
  // Walkthrough screenshots start from an empty in-memory server, so the demo tour is always
  // fresh, and hide the dev inspector toggle.
  plugins: [
    vue(),
    jazzPlugin(process.env.VITE_E2E ? { server: { inMemory: true }, inspector: false } : {}),
  ],
});
