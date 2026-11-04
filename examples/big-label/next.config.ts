import type { NextConfig } from "next";
import { jazzConfig } from "./src/lib/jazz-config";

// Fails closed: a production build must name its Jazz app.
jazzConfig();

// Jazz runs inside this app (src/lib/jazz.server.ts): no separate sync server.
export default {
  reactStrictMode: true,
  // The local origin is 127.0.0.1, which Next's dev server blocks by default.
  allowedDevOrigins: ["127.0.0.1"],
  // jazz-napi is a native addon: Node loads it at runtime, the bundler leaves it alone.
  serverExternalPackages: ["jazz-napi", "jazz-tools/backend"],
} satisfies NextConfig;
