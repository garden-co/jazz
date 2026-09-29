import type { NextConfig } from "next";
import { withJazz } from "jazz-tools/dev/next";
import { appOrigin, serverSecret } from "./src/lib/config";

export default withJazz(
  {
    reactStrictMode: true,
    serverExternalPackages: ["jazz-napi", "jazz-tools/backend"],
  } satisfies NextConfig,
  {
    server: {
      backendSecret: serverSecret("BACKEND_SECRET"),
      jwksUrl: `${appOrigin}/api/auth/jwks`,
      jwtIssuer: appOrigin,
      jwtAudience: appOrigin,
    },
  },
);
