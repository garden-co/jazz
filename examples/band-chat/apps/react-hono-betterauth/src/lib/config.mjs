// BandChat's server configuration, shared by the Hono server, vite.config.ts
// and the pre-build check. It fails closed: local defaults apply only to a
// non-production process on a loopback origin, and no secret is checked in.

export const LOCAL_ORIGIN = "http://127.0.0.1:3000";
/** The Jazz app id a local run uses when none is set. */
export const LOCAL_APP_ID = "00000000-0000-0000-0000-0000000b0c4a";

/** @param {Record<string, string | undefined>} env */
export function readConfig(env = process.env) {
  return {
    nodeEnv: env.NODE_ENV,
    /** Public settings that were not set explicitly. */
    unsetPublic: ["APP_ORIGIN", "VITE_JAZZ_APP_ID"].filter((name) => !env[name]),
    origin: env.APP_ORIGIN || LOCAL_ORIGIN,
    appId: env.VITE_JAZZ_APP_ID || LOCAL_APP_ID,
    betterAuthSecret: env.BETTER_AUTH_SECRET || undefined,
    /**
     * Where the data lives. With JAZZ_SERVER_URL set, the backend connects to
     * that sync server (such as Jazz Cloud) with the app's secrets; otherwise
     * it keeps the data in a local SQLite directory.
     */
    remote: env.JAZZ_SERVER_URL
      ? {
          url: env.JAZZ_SERVER_URL,
          adminSecret: env.JAZZ_ADMIN_SECRET || undefined,
          backendSecret: env.BACKEND_SECRET || undefined,
        }
      : undefined,
    dataPath: env.JAZZ_DATA_PATH || ".jazz",
  };
}

/**
 * A local run is a non-production process (`pnpm dev`, tests) on a loopback
 * origin. Anything else, including a production build that forgot to set its
 * origin, is a deployment and must be configured explicitly.
 * @param {ReturnType<typeof readConfig>} config
 */
export function usesLocalDefaults(config = readConfig()) {
  if (config.nodeEnv === "production") return false;
  const { hostname } = new URL(config.origin);
  return hostname === "127.0.0.1" || hostname === "localhost";
}

/**
 * Rejects incomplete configurations before any route runs. The Better Auth
 * secret is always required (`pnpm dev` generates a local one, see
 * dev-secrets.mjs); deployments must also name their origin and Jazz app, and
 * a remote sync server needs both of the app's secrets.
 * @param {ReturnType<typeof readConfig>} config
 */
export function assertConfiguration(config = readConfig()) {
  const local = usesLocalDefaults(config);
  const missing = [
    ...(local ? [] : config.unsetPublic),
    ...(config.betterAuthSecret ? [] : ["BETTER_AUTH_SECRET"]),
    ...(config.remote && !config.remote.adminSecret ? ["JAZZ_ADMIN_SECRET"] : []),
    ...(config.remote && !config.remote.backendSecret ? ["BACKEND_SECRET"] : []),
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
