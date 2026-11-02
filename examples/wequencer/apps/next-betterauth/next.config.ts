import { withJazz } from "jazz-tools/dev/next";
import { jazzConfig } from "./lib/jazz-config";

// Fails closed: a production build must name its Jazz app.
const config = jazzConfig();

// Jazz runs inside this app (lib/jazz.server.ts): no separate sync server.
export default withJazz(
  {
    // The local origin is 127.0.0.1, which Next's dev server blocks by default.
    allowedDevOrigins: ["127.0.0.1"],
    serverExternalPackages: ["jazz-napi", "jazz-tools/backend"],
    // The browser and the backend name the same Jazz app.
    env: { NEXT_PUBLIC_JAZZ_APP_ID: config.appId },
  },
  { server: false },
);
