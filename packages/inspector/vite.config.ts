import { resolve } from "node:path";
import { defineConfig, type UserConfig } from "vite";
import react from "@vitejs/plugin-react-swc";

export default defineConfig(({ mode }): UserConfig => {
  if (mode === "embedded") {
    return {
      plugins: [react()],
      base: "./",
      worker: { format: "es" },
      build: {
        outDir: "dist-embedded",
        emptyOutDir: true,
        rollupOptions: { input: { index: resolve(__dirname, "embedded.html") } },
      },
    };
  }

  // The standalone "web" build (the default).
  return {
    plugins: [react()],
    base: "/",
    publicDir: "public",
    worker: { format: "es" },
    build: {
      outDir: "dist",
      emptyOutDir: true,
    },
  };
});
