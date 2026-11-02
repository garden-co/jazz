import type { NextConfig } from "next";
import { withJazz } from "jazz-tools/dev/next";
import { assertServerConfiguration, jazzAppId } from "./src/lib/config";

// Fail at startup, not on the first request, when a deployment lacks its secrets.
assertServerConfiguration();

// Jazz runs inside this app (src/lib/jazz.server.ts): no separate sync server.
export default withJazz(
  {
    reactStrictMode: true,
    // The local origin may be 127.0.0.1, which Next's dev server blocks by default.
    allowedDevOrigins: ["127.0.0.1"],
    serverExternalPackages: ["jazz-napi", "jazz-tools/backend"],
    // The browser and the backend name the same Jazz app.
    env: { NEXT_PUBLIC_JAZZ_APP_ID: jazzAppId },
  } satisfies NextConfig,
  { server: false },
);
