import { resolve } from "node:path";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react-swc";

export default defineConfig(({ mode }) => {
  const isExtensionBuild = mode === "extension";

  return {
    plugins: [react()],
    resolve: {
      alias: [
        {
          find: "jazz-tools/react",
          replacement: resolve(__dirname, "../jazz-tools/src/react/index.ts"),
        },
        {
          find: "jazz-tools/testing",
          replacement: resolve(__dirname, "../jazz-tools/src/testing/index.ts"),
        },
        {
          find: "jazz-tools",
          replacement: resolve(__dirname, "../jazz-tools/src/index.ts"),
        },
      ],
    },
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
