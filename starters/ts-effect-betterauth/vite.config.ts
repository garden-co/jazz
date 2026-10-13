import { defineConfig, loadEnv } from "vite";
import { jazzPlugin } from "jazz-tools/dev/vite";

// `pnpm dev` runs Vite inside the API server (server/dev.ts), so the client,
// Better Auth and the Effect API share one origin in dev and in production.
export default defineConfig(({ mode }) => {
  const appOrigin =
    loadEnv(mode, process.cwd(), "APP_ORIGIN").APP_ORIGIN ?? "http://localhost:3001";

  return {
    plugins: [
      jazzPlugin({
        server: {
          jwksUrl: `${appOrigin}/api/auth/jwks`,
          jwtIssuer: appOrigin,
          jwtAudience: appOrigin,
        },
      }),
    ],
    worker: { format: "es" },
    // `vite build --ssr server/index.ts` bundles only this app's own server
    // code (including schema.ts and permissions.ts) and imports every package.
    ssr: { external: true },
  };
});
