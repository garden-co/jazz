export const LOCAL_DEFAULTS = Object.freeze({
  origin: "http://127.0.0.1:3000",
  /** The Jazz app id a local run uses when none is set. */
  appId: "00000000-0000-0000-0000-00000000b057",
});

/** @param {Record<string, string | undefined>} env */
export function readBuildConfig(env = process.env) {
  return {
    origin: env.NEXT_PUBLIC_APP_ORIGIN || LOCAL_DEFAULTS.origin,
    appId: env.NEXT_PUBLIC_JAZZ_APP_ID || LOCAL_DEFAULTS.appId,
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
    betterAuthSecret: env.BETTER_AUTH_SECRET || undefined,
  };
}

/**
 * The checked-in local setup: the default origin and app, with data in a
 * local SQLite directory. Only this setup may use the fallback secret.
 * @param {ReturnType<typeof readBuildConfig>} config
 */
export function usesLocalDefaults(config = readBuildConfig()) {
  return (
    config.origin === LOCAL_DEFAULTS.origin &&
    config.appId === LOCAL_DEFAULTS.appId &&
    !config.remote
  );
}

/**
 * Reject partial or nonlocal configurations before Next evaluates any route:
 * a deployment needs its own Better Auth secret, and a remote sync server
 * needs both of the app's secrets.
 * @param {ReturnType<typeof readBuildConfig>} config
 */
export function assertBuildConfiguration(config = readBuildConfig()) {
  if (usesLocalDefaults(config)) return config;
  const missing = [
    !config.betterAuthSecret && "BETTER_AUTH_SECRET",
    config.remote && !config.remote.adminSecret && "JAZZ_ADMIN_SECRET",
    config.remote && !config.remote.backendSecret && "BACKEND_SECRET",
  ].filter(Boolean);
  if (missing.length) {
    throw new Error(`PosterShop deployments must set ${missing.join(", ")}`);
  }
  return config;
}

if (import.meta.url === new URL(process.argv[1], "file:").href) {
  const config = assertBuildConfiguration();
  console.log(
    usesLocalDefaults(config)
      ? "PosterShop build config: checked-in local defaults"
      : "PosterShop build config: configured nonlocal deployment",
  );
}
