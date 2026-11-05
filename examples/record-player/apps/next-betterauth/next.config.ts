import { readConfig } from "./src/lib/config.mjs";

// Fails closed: a production build needs its own app id and secrets.
readConfig();

// Jazz runs inside this app (src/lib/jazz.server.ts): no separate sync server.
export default {
  reactStrictMode: true,
  // The local origin is 127.0.0.1, which Next's dev server blocks by default.
  allowedDevOrigins: ["127.0.0.1"],
  // jazz-napi is a native addon: Node loads it at runtime, the bundler leaves it alone.
  serverExternalPackages: ["jazz-napi", "jazz-tools/backend"],
};
