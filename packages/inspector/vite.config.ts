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

  const isExtensionBuild = mode === "extension";

  return {
    plugins: [react()],
    base: isExtensionBuild ? "./" : "/",
    publicDir: isExtensionBuild ? "chrome-extension" : "public",
    worker: {
      format: "es",
    },
    build: isExtensionBuild
      ? {
          outDir: "dist-extension",
          emptyOutDir: true,
          rollupOptions: {
            input: {
              index: resolve(__dirname, "devtools-tab.html"),
              devtools: resolve(__dirname, "devtools.html"),
            },
          },
        }
      : {
          outDir: "dist",
          emptyOutDir: true,
        },
  };
});
