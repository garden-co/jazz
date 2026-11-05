import { jazzConfig } from "./lib/jazz-config";

// Fails closed: a production build must name its Jazz app.
jazzConfig();

// Jazz runs inside this app (lib/jazz.server.ts): no separate sync server.
export default {
  // The local origin is 127.0.0.1, which Next's dev server blocks by default.
  allowedDevOrigins: ["127.0.0.1"],
  // jazz-napi is a native addon: Node loads it at runtime, the bundler leaves it alone.
  serverExternalPackages: ["jazz-napi", "jazz-tools/backend"],
};
