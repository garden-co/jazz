import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";
import { jazzPlugin } from "jazz-tools/dev/vite";
import { WORKOS_CLIENT_ID } from "./constants.js";

export default defineConfig(({ mode }) => {
  const { WORKOS_JWT_ISSUER } = loadEnv(mode, process.cwd(), "");

  if (!WORKOS_JWT_ISSUER) {
    throw new Error(
      "WORKOS_JWT_ISSUER must exactly match the iss claim in WorkOS access tokens, including any trailing slash or custom AuthKit domain.",
    );
  }

  return {
    plugins: [
      react(),
      jazzPlugin({
        server: {
          jwksUrl: `https://api.workos.com/sso/jwks/${WORKOS_CLIENT_ID}`,
          jwtIssuer: WORKOS_JWT_ISSUER,
          jwtAudience: WORKOS_CLIENT_ID,
        },
      }),
    ],
  };
});
