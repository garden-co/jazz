import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";
import { jazzPlugin } from "jazz-tools/dev/vite";

// `/api` must reach the Hono server in both `vite` (dev) and `vite preview`
// (prod-build smoke). The two configs mirror each other on purpose.
const apiProxy = {
  "/api": {
    target: "http://localhost:3001",
    changeOrigin: true,
  },
};

export default defineConfig(({ mode }) => {
  const appOrigin =
    loadEnv(mode, process.cwd(), "APP_ORIGIN").APP_ORIGIN ?? "http://localhost:3001";

  return {
    plugins: [
      react(),
      jazzPlugin({
        server: {
          jwksUrl: `${appOrigin}/api/auth/jwks`,
          jwtIssuer: appOrigin,
          jwtAudience: appOrigin,
        },
      }),
    ],
    worker: { format: "es" },
    server: { proxy: apiProxy },
    preview: { proxy: apiProxy },
  };
});
