import type { NextConfig } from "next";
import { withJazz } from "jazz-tools/dev/next";

const appOrigin = process.env.NEXT_PUBLIC_APP_ORIGIN ?? "http://127.0.0.1:3000";

export default withJazz(
  {
    reactStrictMode: true,
    serverExternalPackages: ["jazz-napi", "jazz-tools/backend"],
  } satisfies NextConfig,
  {
    server: {
      backendSecret: process.env.BACKEND_SECRET ?? "poster-shop-development-backend-secret",
      // The Jazz server requires issuer and audience on external JWTs; both
      // match the Better Auth jwt plugin in src/lib/auth.ts.
      jwksUrl: `${appOrigin}/api/auth/jwks`,
      jwtIssuer: appOrigin,
      jwtAudience: appOrigin,
    },
  },
);
