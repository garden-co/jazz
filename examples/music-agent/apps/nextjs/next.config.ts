import type { NextConfig } from "next";
import { withJazz } from "jazz-tools/dev/next";
import { appOrigin } from "./src/lib/app-origin";
import { serverSecret } from "./src/lib/server-secret";

export default withJazz(
  {
    reactStrictMode: true,
    serverExternalPackages: ["jazz-napi", "jazz-tools/backend"],
  } satisfies NextConfig,
  {
    server: {
      backendSecret: serverSecret("BACKEND_SECRET", "music-agent-development-backend-secret"),
      jwksUrl: `${appOrigin}/api/auth/jwks`,
      // The Jazz server only accepts app JWTs whose issuer and audience match.
      jwtIssuer: appOrigin,
      jwtAudience: appOrigin,
    },
  },
);
