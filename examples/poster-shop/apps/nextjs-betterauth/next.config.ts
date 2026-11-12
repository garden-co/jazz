import type { NextConfig } from "next";
import { assertBuildConfiguration } from "./src/lib/build-config.mjs";

// Rejects a partial deployment configuration before any route runs.
assertBuildConfiguration();

// Jazz runs inside this app (src/lib/jazz.server.ts): no separate sync server.
export default {
  reactStrictMode: true,
  // The local origin is 127.0.0.1, which Next's dev server blocks by default.
  allowedDevOrigins: ["127.0.0.1"],
  // jazz-napi is a native addon: Node loads it at runtime, the bundler leaves it alone.
  serverExternalPackages: ["jazz-napi", "jazz-tools/backend"],
} satisfies NextConfig;
