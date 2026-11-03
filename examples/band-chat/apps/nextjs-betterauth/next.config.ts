import { withJazz } from "jazz-tools/dev/next";
import { assertConfiguration } from "./src/lib/config.mjs";

// Fails closed: a deployment without its origin, Jazz app and secrets does not
// start, and there are no checked-in secrets. `pnpm dev` generates local ones.
const config = assertConfiguration();

// Jazz runs inside this app (src/lib/jazz.server.ts): no separate sync server.
export default withJazz(
  {
    reactStrictMode: true,
    // The local origin is 127.0.0.1, which Next's dev server blocks by default.
    allowedDevOrigins: ["127.0.0.1"],
    serverExternalPackages: ["jazz-napi", "jazz-tools/backend"],
    // The browser and the backend name the same Jazz app (a local default in
    // development).
    env: { NEXT_PUBLIC_JAZZ_APP_ID: config.appId },
  },
  { server: false },
);
