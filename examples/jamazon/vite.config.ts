import { defineConfig } from "vite";
import { devtools } from "@tanstack/devtools-vite";
import tsconfigPaths from "vite-tsconfig-paths";

import { tanstackStart } from "@tanstack/react-start/plugin/vite";

import viteReact from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { nitro } from "nitro/vite";
import wasm from "vite-plugin-wasm";

const config = defineConfig({
  plugins: [
    devtools(),
    nitro({
      rollupConfig: {
        external: [/^@sentry\//, /^jazz-napi/, /^jazz-tools\/backend/],
      },
    }),
    tsconfigPaths({ projects: ["./tsconfig.json"] }),
    tailwindcss(),
    tanstackStart(),
    viteReact(),
    wasm(),
  ],
  ssr: {
    external: ["jazz-napi", "jazz-tools"],
  },
  optimizeDeps: {
    exclude: ["jazz-wasm"],
  },
  build: {
    target: "es2020",
  },
  worker: {
    format: "es",
  },
});

export default config;
