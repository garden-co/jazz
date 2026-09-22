import { sveltekit } from "@sveltejs/kit/vite";
import { jazzSvelteKit } from "jazz-tools/dev/sveltekit";
import { defineConfig, loadEnv } from "vite";

export default defineConfig(({ mode }) => {
  const appOrigin =
    loadEnv(mode, process.cwd(), "APP_ORIGIN").APP_ORIGIN ?? "http://localhost:5173";

  return {
    plugins: [
      sveltekit(),
      jazzSvelteKit({
        server: {
          jwksUrl: `${appOrigin}/api/auth/jwks`,
          jwtIssuer: appOrigin,
          jwtAudience: appOrigin,
        },
      }),
    ],
    server: {
      fs: {
        allow: ["../.."],
      },
    },
  };
});
