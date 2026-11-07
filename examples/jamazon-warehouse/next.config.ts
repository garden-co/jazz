import type { NextConfig } from "next";
import { assertServerConfiguration } from "./src/lib/config";

// Fail at startup, not on the first request, when a deployment lacks its secrets.
assertServerConfiguration();

// Jazz runs inside this app (src/lib/jazz.server.ts): no separate sync server.
export default {
  reactStrictMode: true,
  // The local origin may be 127.0.0.1, which Next's dev server blocks by default.
  allowedDevOrigins: ["127.0.0.1"],
  // jazz-napi is a native addon: Node loads it at runtime, the bundler leaves it alone.
  serverExternalPackages: ["jazz-napi", "jazz-tools/backend"],
} satisfies NextConfig;
