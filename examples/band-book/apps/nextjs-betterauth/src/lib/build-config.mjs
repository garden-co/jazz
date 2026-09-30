export const LOCAL_DEFAULTS = Object.freeze({
  origin: "http://127.0.0.1:3000",
  appId: "band-book-local",
  serverUrl: "http://127.0.0.1:4200",
});

/** @param {Record<string, string | undefined>} env */
export function readBuildConfig(env = process.env) {
  return {
    origin: env.NEXT_PUBLIC_APP_ORIGIN ?? LOCAL_DEFAULTS.origin,
    appId: env.NEXT_PUBLIC_JAZZ_APP_ID ?? LOCAL_DEFAULTS.appId,
    serverUrl: env.NEXT_PUBLIC_JAZZ_SERVER_URL ?? LOCAL_DEFAULTS.serverUrl,
    backendSecret: env.BACKEND_SECRET,
    betterAuthSecret: env.BETTER_AUTH_SECRET,
    nodeEnv: env.NODE_ENV,
    /** Which deployment settings were set explicitly rather than defaulted. */
    explicit: {
      origin: Boolean(env.NEXT_PUBLIC_APP_ORIGIN),
      appId: Boolean(env.NEXT_PUBLIC_JAZZ_APP_ID),
      serverUrl: Boolean(env.NEXT_PUBLIC_JAZZ_SERVER_URL),
    },
  };
}

/**
 * Whether the checked-in development values (and their dev secrets) may be
 * used. Never in production: a deploy that forgets its env vars would
 * otherwise look exactly like local development, so it fails closed instead.
 * @param {ReturnType<typeof readBuildConfig>} config
 */
export function usesLocalDefaults(config = readBuildConfig()) {
  if (config.nodeEnv === "production") return false;
  return (
    config.origin === LOCAL_DEFAULTS.origin &&
    config.appId === LOCAL_DEFAULTS.appId &&
    config.serverUrl === LOCAL_DEFAULTS.serverUrl
  );
}

/** Reject partial/nonlocal configurations before Next evaluates any route. */
/** @param {ReturnType<typeof readBuildConfig>} config */
export function assertBuildConfiguration(config = readBuildConfig()) {
  if (usesLocalDefaults(config)) return config;
  // In production the local defaults would silently become the JWT issuer,
  // audience and JWKS URL, so the deployment settings must be explicit too.
  const production = config.nodeEnv === "production";
  const missing = [
    production && !config.explicit.origin && "NEXT_PUBLIC_APP_ORIGIN",
    production && !config.explicit.appId && "NEXT_PUBLIC_JAZZ_APP_ID",
    production && !config.explicit.serverUrl && "NEXT_PUBLIC_JAZZ_SERVER_URL",
    !config.backendSecret && "BACKEND_SECRET",
    !config.betterAuthSecret && "BETTER_AUTH_SECRET",
  ].filter(Boolean);
  if (missing.length) {
    throw new Error(
      `BandBook production or nonlocal configuration is incomplete; missing: ${missing.join(", ")}`,
    );
  }
  return config;
}

// Run as a script (`node src/lib/build-config.mjs`), not when a bundle imports it.
const entry = typeof process !== "undefined" ? process.argv?.[1] : undefined;
if (entry && import.meta.url === new URL(entry, "file:").href) {
  const config = assertBuildConfiguration();
  console.log(
    usesLocalDefaults(config)
      ? "BandBook build config: checked-in local defaults"
      : "BandBook build config: configured nonlocal deployment",
  );
}
