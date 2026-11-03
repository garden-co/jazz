import type { NextConfig } from "next";

// `pnpm dev` starts a local Jazz core (dev/examples/with-local-core.mjs) and
// passes its URL and app id in NEXT_PUBLIC_JAZZ_SERVER_URL / NEXT_PUBLIC_JAZZ_APP_ID.
export default {
  // jazz-napi is a native addon: Node loads it at runtime, the bundler leaves it alone.
  serverExternalPackages: ["jazz-napi"],
} satisfies NextConfig;
