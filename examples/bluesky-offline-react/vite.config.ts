import { defineConfig, loadEnv } from "vite";
import babel from "@rolldown/plugin-babel";
import react, { reactCompilerPreset } from "@vitejs/plugin-react";
import { jazzPlugin } from "jazz-tools/dev/vite";
import { jazzAppId } from "./shared/identifiers.js";
import { pwaPlugin } from "./vite/pwa.js";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, ".", "");
  // ManagedDevRuntime supports this option, although JazzPluginOptions does
  // not currently expose it. Keeping it in a named object avoids a cast while
  // ensuring the local server and BFF share backend authority.
  const jazzOptions = {
    appId: jazzAppId,
    adminSecret: env.JAZZ_ADMIN_SECRET,
    backendSecret: env.BACKEND_SECRET,
    inspector: false,
    server: {
      port: 4200,
      jwksUrl: "http://127.0.0.1:3001/.well-known/jazz-jwks.json",
    },
  };
  const proxy = {
    "/api": "http://127.0.0.1:3001",
    "/xrpc": "http://127.0.0.1:3001",
  };
  return {
    plugins: [
      jazzPlugin(jazzOptions),
      react(),
      babel({ presets: [reactCompilerPreset()] }),
      pwaPlugin(),
    ],
    server: {
      host: "127.0.0.1",
      watch: { ignored: ["**/data/**"] },
      proxy,
    },
    preview: {
      host: "127.0.0.1",
      proxy,
    },
    build: {
      outDir: "dist/client",
    },
  };
});
