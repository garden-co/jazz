export const baseNextConfig = {
  reactStrictMode: true,
  // jazz-napi is a native addon: Node loads it at runtime, the bundler leaves it alone.
  serverExternalPackages: ["jazz-napi", "jazz-tools/backend"],
};

// Jazz runs inside this app (src/lib/jazz.server.ts): no separate sync server.
export default baseNextConfig;
