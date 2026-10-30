import type { NextConfig } from "next";
import { withJazz } from "jazz-tools/dev/next";
import { appOrigin, assertServerConfiguration, serverSecret } from "./src/lib/config";

// Fail at startup, not on the first request, when a deployment lacks its secrets.
assertServerConfiguration();

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
