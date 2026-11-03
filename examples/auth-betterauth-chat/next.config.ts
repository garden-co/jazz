import { withJazz } from "jazz-tools/dev/next";

export const baseNextConfig = {
  reactStrictMode: true,
  serverExternalPackages: ["jazz-napi", "jazz-tools/backend"],
};

// Jazz runs inside this app (src/lib/jazz.server.ts): no separate sync server.
export default withJazz(baseNextConfig, { server: false });
