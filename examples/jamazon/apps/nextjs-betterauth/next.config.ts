import type { NextConfig } from "next";
import { withJazz } from "jazz-tools/dev/next";
import { paymentProvider, readBuildConfig } from "./src/lib/build-config.mjs";

const config = readBuildConfig();

export default withJazz(
  {
    reactStrictMode: true,
    serverExternalPackages: ["jazz-napi", "jazz-tools/backend"],
    // The browser needs to know which payment UI to show; the server enforces it.
    env: { NEXT_PUBLIC_PAYMENT_PROVIDER: paymentProvider(config) },
  } satisfies NextConfig,
  {
    server: {
      backendSecret: process.env.BACKEND_SECRET ?? "jamazon-development-backend-secret",
      jwksUrl: `${config.origin}/api/auth/jwks`,
    },
  },
);
