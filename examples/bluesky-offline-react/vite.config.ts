import { defineConfig, loadEnv } from "vite";
import babel from "@rolldown/plugin-babel";
import react, { reactCompilerPreset } from "@vitejs/plugin-react";
import { jazzPlugin } from "jazz-tools/dev/vite";
import { pwaPlugin } from "./pwa.js";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, ".", "");
  const proxy = {
    "/api": "http://127.0.0.1:3001",
    "/xrpc": "http://127.0.0.1:3001",
  };
  return {
    plugins: [
      jazzPlugin({
        adminSecret: env.JAZZ_ADMIN_SECRET,
        // The Vite plugin's managed runtime accepts this option, but its public
        // Vite options type does not expose it yet. A spread keeps this example
        // scoped to the public plugin while giving the separate BFF process the
        // same explicit backend identity as the local Jazz server.
        ...{ backendSecret: env.BACKEND_SECRET },
        server: {
          port: 4200,
          // Offline clients can reconnect with queued writes from an older
          // schema, so the local sync server must retain its catalogue across
          // Vite restarts.
          jwksUrl: "http://127.0.0.1:3001/.well-known/jazz-jwks.json",
        },
      }),
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
  };
});
