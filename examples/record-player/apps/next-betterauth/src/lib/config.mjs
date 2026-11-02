// RecordPlayer's server configuration, shared by next.config.ts and the
// backend. It fails closed: local defaults apply only outside production.

/** The Jazz app id a local run uses when none is set. */
export const LOCAL_APP_ID = "00000000-0000-0000-0000-00000000a1b0";

/** @param {Record<string, string | undefined>} env */
export function readConfig(env = process.env) {
  const production = env.NODE_ENV === "production";
  const appId = env.NEXT_PUBLIC_JAZZ_APP_ID || (production ? undefined : LOCAL_APP_ID);
  const betterAuthSecret =
    env.BETTER_AUTH_SECRET || (production ? undefined : "record-player-development-secret");
  /**
   * Where the data lives. With JAZZ_SERVER_URL set, the backend connects to
   * that sync server (such as Jazz Cloud) with the app's secrets; otherwise
   * it keeps the data in a local SQLite directory.
   */
  const remote = env.JAZZ_SERVER_URL
    ? {
        url: env.JAZZ_SERVER_URL,
        adminSecret: env.JAZZ_ADMIN_SECRET || undefined,
        backendSecret: env.BACKEND_SECRET || undefined,
      }
    : undefined;
  const missing = [
    !appId && "NEXT_PUBLIC_JAZZ_APP_ID",
    !betterAuthSecret && "BETTER_AUTH_SECRET",
    remote && !remote.adminSecret && "JAZZ_ADMIN_SECRET",
    remote && !remote.backendSecret && "BACKEND_SECRET",
  ].filter(Boolean);
  if (missing.length) throw new Error(`RecordPlayer must be configured with ${missing.join(", ")}`);
  return {
    origin: env.NEXT_PUBLIC_APP_ORIGIN || "http://127.0.0.1:3000",
    appId: /** @type {string} */ (appId),
    betterAuthSecret: /** @type {string} */ (betterAuthSecret),
    remote: remote && {
      url: remote.url,
      adminSecret: /** @type {string} */ (remote.adminSecret),
      backendSecret: /** @type {string} */ (remote.backendSecret),
    },
    dataPath: env.JAZZ_DATA_PATH || ".jazz",
  };
}
