import { resolve } from "node:path";
import { tanstackRouter } from "@tanstack/router-plugin/vite";
import react from "@vitejs/plugin-react-swc";
import { defineConfig } from "vite";

export default defineConfig(({ mode }) => {
  const isExtensionBuild = mode === "extension";

  return {
    resolve: {
      alias: {
        "#contexts": resolve(__dirname, "src/contexts"),
        "#data-explorer": resolve(__dirname, "src/components/data-explorer"),
        "#db-config-form": resolve(__dirname, "src/components/db-config-form"),
        "#inspector-layout": resolve(__dirname, "src/components/inspector-layout"),
        "#lib": resolve(__dirname, "src/lib"),
        "#pages": resolve(__dirname, "src/pages"),
      },
      tsconfigPaths: true,
    },
    plugins: [
      tanstackRouter({
        target: "react",
      }),
      react(),
    ],
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
