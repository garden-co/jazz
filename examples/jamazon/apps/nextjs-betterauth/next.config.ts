import type { NextConfig } from "next";
import { withJazz } from "jazz-tools/dev/next";
import {
  assertBuildConfiguration,
  paymentProvider,
  readBuildConfig,
} from "./src/lib/build-config.mjs";

// Fails closed: a deployment without its origin, secrets and payment provider
// does not start. `pnpm dev` generates the local secret first.
const config = assertBuildConfiguration(readBuildConfig());

// Jazz runs inside this app (src/server/backend.ts): no separate sync server.
export default withJazz(
  {
    reactStrictMode: true,
    // The local origin is 127.0.0.1, which Next's dev server blocks by default.
    allowedDevOrigins: ["127.0.0.1"],
    serverExternalPackages: ["jazz-napi", "jazz-tools/backend"],
    env: {
      // The browser and the backend name the same Jazz app (a local default
      // in development).
      NEXT_PUBLIC_JAZZ_APP_ID: config.appId,
      // The browser needs to know which payment UI to show; the server enforces it.
      NEXT_PUBLIC_PAYMENT_PROVIDER: paymentProvider(config),
    },
  } satisfies NextConfig,
  { server: false },
);
