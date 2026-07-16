import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";
import { jazzPlugin } from "jazz-tools/dev/vite";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, ".", "");
  return {
    plugins: [
      jazzPlugin({
        adminSecret: env.JAZZ_ADMIN_SECRET,
        server: {
          port: 4200,
          inMemory: true,
          jwksUrl: "http://127.0.0.1:3001/.well-known/jazz-jwks.json",
        },
      }),
      react(),
    ],
    server: {
      host: "127.0.0.1",
      proxy: {
        "/api": "http://127.0.0.1:3001",
        "/xrpc": "http://127.0.0.1:3001",
      },
    },
  };
});
