export const LOCAL_DEFAULTS = Object.freeze({
  origin: "http://127.0.0.1:3000",
  appId: "poster-shop-local",
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
  };
}

/** @param {ReturnType<typeof readBuildConfig>} config */
export function usesLocalDefaults(config = readBuildConfig()) {
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
  const missing = [
    !config.backendSecret && "BACKEND_SECRET",
    !config.betterAuthSecret && "BETTER_AUTH_SECRET",
  ].filter(Boolean);
  if (missing.length) {
    throw new Error(
      `PosterShop nonlocal configuration requires BACKEND_SECRET and BETTER_AUTH_SECRET; missing: ${missing.join(", ")}`,
    );
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
