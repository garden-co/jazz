import type { NextConfig } from "next";
import { assertBuildConfiguration } from "./src/lib/build-config.mjs";

// Rejects a partial deployment configuration before any route runs.
const config = assertBuildConfiguration();

// Jazz runs inside this app (src/lib/jazz.server.ts): no separate sync server.
export default {
  reactStrictMode: true,
  // The local origin is 127.0.0.1, which Next's dev server blocks by default.
  allowedDevOrigins: ["127.0.0.1"],
  // jazz-napi is a native addon: Node loads it at runtime, the bundler leaves it alone.
  serverExternalPackages: ["jazz-napi", "jazz-tools/backend"],
  // The browser and the backend name the same Jazz app (a local default in
  // development).
  env: { NEXT_PUBLIC_JAZZ_APP_ID: config.appId },
} satisfies NextConfig;
