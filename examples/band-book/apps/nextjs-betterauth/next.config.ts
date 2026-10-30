import type { NextConfig } from "next";
import { withJazz } from "jazz-tools/dev/next";
import { appOrigin, jwtAudience, jwtIssuer } from "./src/lib/config";

export default withJazz(
  {
    reactStrictMode: true,
    // The checked-in local origin is 127.0.0.1, which Next's dev server
    // otherwise treats as cross-origin and refuses its scripts.
    allowedDevOrigins: ["127.0.0.1"],
    serverExternalPackages: ["jazz-napi", "jazz-tools/backend"],
  } satisfies NextConfig,
  {
    server: {
      backendSecret: process.env.BACKEND_SECRET ?? "band-book-development-backend-secret",
      jwksUrl: `${appOrigin}/api/auth/jwks`,
      jwtIssuer,
      jwtAudience,
    },
  },
);
