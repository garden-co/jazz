import { resolve } from "node:path";
import { defineConfig } from "vite";

export default defineConfig({
  build: {
    emptyOutDir: true,
    outDir: "dist/package-root-import-gate",
    rollupOptions: {
      input: resolve(__dirname, "src/package-root-browser-import.fixture.ts"),
    },
  },
});
