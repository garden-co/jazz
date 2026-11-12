import { assertConfiguration } from "./src/lib/config.mjs";

// Fails closed: a deployment without its origin, Jazz app and secrets does not
// start, and there are no checked-in secrets. `pnpm dev` generates local ones.
assertConfiguration();

// Jazz runs inside this app (src/lib/jazz.server.ts): no separate sync server.
export default {
  reactStrictMode: true,
  // The local origin is 127.0.0.1, which Next's dev server blocks by default.
  allowedDevOrigins: ["127.0.0.1"],
  // jazz-napi is a native addon: Node loads it at runtime, the bundler leaves it alone.
  serverExternalPackages: ["jazz-napi", "jazz-tools/backend"],
};
