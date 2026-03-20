import { copyFileSync, mkdirSync } from "node:fs";
import { resolve } from "node:path";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react-swc";

function copyExtensionManifestPlugin() {
  return {
    name: "copy-extension-manifest",
    writeBundle(options: { dir?: string }) {
      const outputDirectory = options.dir ?? resolve(__dirname, "dist-extension");
      mkdirSync(outputDirectory, { recursive: true });
      copyFileSync(
        resolve(__dirname, "chrome-extension/manifest.json"),
        resolve(outputDirectory, "manifest.json"),
      );
    },
  };
}

export default defineConfig(({ mode }) => {
  const isExtensionBuild = mode === "extension";

  return {
    plugins: isExtensionBuild ? [react(), copyExtensionManifestPlugin()] : [react()],
    base: isExtensionBuild ? "./" : "/",
    publicDir: isExtensionBuild ? false : "public",
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
              "content-script": resolve(__dirname, "src/chrome-extension/content-script.ts"),
            },
            output: {
              entryFileNames: (chunkInfo) =>
                chunkInfo.name === "content-script"
                  ? "content-script.js"
                  : "assets/[name]-[hash].js",
            },
          },
        }
      : {
          outDir: "dist",
          emptyOutDir: true,
        },
  };
});
