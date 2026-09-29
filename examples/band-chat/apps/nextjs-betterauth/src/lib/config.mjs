// BandChat's server configuration, shared by next.config.ts, the auth routes
// and the pre-build check. It fails closed: local defaults apply only to a
// non-production process on a loopback origin, and no secret is checked in.

export const LOCAL_ORIGIN = "http://127.0.0.1:3000";

/** @param {Record<string, string | undefined>} env */
export function readConfig(env = process.env) {
  return {
    nodeEnv: env.NODE_ENV,
    /** Public settings that were not set explicitly. */
    unsetPublic: [
      "NEXT_PUBLIC_APP_ORIGIN",
      "NEXT_PUBLIC_JAZZ_APP_ID",
      "NEXT_PUBLIC_JAZZ_SERVER_URL",
    ].filter((name) => !env[name]),
    origin: env.NEXT_PUBLIC_APP_ORIGIN || LOCAL_ORIGIN,
    appId: env.NEXT_PUBLIC_JAZZ_APP_ID || undefined,
    serverUrl: env.NEXT_PUBLIC_JAZZ_SERVER_URL || undefined,
    backendSecret: env.BACKEND_SECRET || undefined,
    betterAuthSecret: env.BETTER_AUTH_SECRET || undefined,
  };
}

/**
 * A local run is a non-production process (`next dev`, tests) on a loopback
 * origin. Anything else, including a production build that forgot to set its
 * origin, is a deployment and must be configured explicitly. In development
 * `withJazz` supplies the Jazz app id and server URL.
 * @param {ReturnType<typeof readConfig>} config
 */
export function usesLocalDefaults(config = readConfig()) {
  if (config.nodeEnv === "production") return false;
  const { hostname } = new URL(config.origin);
  return hostname === "127.0.0.1" || hostname === "localhost";
}

/**
 * Rejects incomplete configurations before any route runs. Both secrets are
 * always required (`pnpm dev` generates local ones, see dev-secrets.mjs);
 * deployments must also name their origin, Jazz app and sync server.
 * @param {ReturnType<typeof readConfig>} config
 */
export function assertConfiguration(config = readConfig()) {
  const local = usesLocalDefaults(config);
  const missing = [
    ...(local ? [] : config.unsetPublic),
    ...(config.backendSecret ? [] : ["BACKEND_SECRET"]),
    ...(config.betterAuthSecret ? [] : ["BETTER_AUTH_SECRET"]),
  ];
  if (missing.length) {
    throw new Error(
      local
        ? `BandChat is missing ${missing.join(", ")}. Start it with \`pnpm dev\`, which generates local secrets.`
        : `BandChat deployments must set ${missing.join(", ")}.`,
    );
  }
  return config;
}

/**
 * The Jazz app id and sync server for the auth backend. `withJazz` sets both
 * in development; deployments set them explicitly.
 * @param {ReturnType<typeof readConfig>} config
 */
export function jazzServer(config = readConfig()) {
  assertConfiguration(config);
  if (!config.appId || !config.serverUrl) {
    throw new Error("NEXT_PUBLIC_JAZZ_APP_ID and NEXT_PUBLIC_JAZZ_SERVER_URL are not configured");
  }
  return { appId: config.appId, serverUrl: config.serverUrl };
}
