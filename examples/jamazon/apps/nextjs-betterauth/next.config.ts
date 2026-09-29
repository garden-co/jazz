import type { NextConfig } from "next";
import { withJazz } from "jazz-tools/dev/next";
import {
  assertBuildConfiguration,
  paymentProvider,
  readBuildConfig,
} from "./src/lib/build-config.mjs";

// Fails closed: a deployment without its origin, secrets and payment provider
// does not start. `pnpm dev` generates the local secrets first.
const config = assertBuildConfiguration(readBuildConfig());

export default withJazz(
  {
    reactStrictMode: true,
    serverExternalPackages: ["jazz-napi", "jazz-tools/backend"],
    // The browser needs to know which payment UI to show; the server enforces it.
    env: { NEXT_PUBLIC_PAYMENT_PROVIDER: paymentProvider(config) },
  } satisfies NextConfig,
  {
    server: {
      backendSecret: config.backendSecret,
      jwksUrl: `${config.origin}/api/auth/jwks`,
    },
  },
);
