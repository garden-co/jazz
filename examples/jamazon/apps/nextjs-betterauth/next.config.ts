import type { NextConfig } from "next";
import {
  assertBuildConfiguration,
  paymentProvider,
  readBuildConfig,
} from "./src/lib/build-config.mjs";

// Fails closed: a deployment without its origin, secrets and payment provider
// does not start. `pnpm dev` generates the local secret first.
const config = assertBuildConfiguration(readBuildConfig());

// Jazz runs inside this app (src/server/backend.ts): no separate sync server.
export default {
  reactStrictMode: true,
  // The local origin is 127.0.0.1, which Next's dev server blocks by default.
  allowedDevOrigins: ["127.0.0.1"],
  // jazz-napi is a native addon: Node loads it at runtime, the bundler leaves it alone.
  serverExternalPackages: ["jazz-napi", "jazz-tools/backend"],
  env: {
    // The browser needs to know which payment UI to show; the server enforces it.
    NEXT_PUBLIC_PAYMENT_PROVIDER: paymentProvider(config),
  },
} satisfies NextConfig;
