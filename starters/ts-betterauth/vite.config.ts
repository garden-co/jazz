import { defineConfig } from "vite";
import { jazzPlugin } from "jazz-tools/dev/vite";

export default defineConfig({
  plugins: [jazzPlugin({ server: { jwksUrl: "http://localhost:3001/api/auth/jwks" } })],
  worker: { format: "es" },
  server: {
    proxy: {
      "/api": {
        target: "http://localhost:3001",
        changeOrigin: true,
      },
    },
  },
});
