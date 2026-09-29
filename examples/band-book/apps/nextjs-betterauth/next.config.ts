import type { NextConfig } from "next";
import { withJazz } from "jazz-tools/dev/next";
import { appOrigin } from "./src/lib/config";

export default withJazz(
  {
    reactStrictMode: true,
    serverExternalPackages: ["jazz-napi", "jazz-tools/backend"],
  } satisfies NextConfig,
  {
    server: {
      backendSecret: process.env.BACKEND_SECRET ?? "band-book-development-backend-secret",
      jwksUrl: `${appOrigin}/api/auth/jwks`,
    },
  },
);
