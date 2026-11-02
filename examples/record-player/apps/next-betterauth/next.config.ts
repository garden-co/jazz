import { withJazz } from "jazz-tools/dev/next";
import { readConfig } from "./src/lib/config.mjs";

// Fails closed: a production build needs its own app id and secrets.
const config = readConfig();

// Jazz runs inside this app (src/lib/jazz.server.ts): no separate sync server.
export default withJazz(
  {
    reactStrictMode: true,
    // The local origin is 127.0.0.1, which Next's dev server blocks by default.
    allowedDevOrigins: ["127.0.0.1"],
    serverExternalPackages: ["jazz-napi", "jazz-tools/backend"],
    // The browser and the backend name the same Jazz app.
    env: { NEXT_PUBLIC_JAZZ_APP_ID: config.appId },
  },
  { server: false },
);
