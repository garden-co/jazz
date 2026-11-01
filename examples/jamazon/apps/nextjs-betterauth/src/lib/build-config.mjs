export const LOCAL_DEFAULTS = Object.freeze({
  origin: "http://127.0.0.1:3000",
  appId: "00000000-0000-0000-0000-0000000a3a20",
  // Local runs take card payments from the in-app sandbox provider, and say so
  // on every payment screen. It is chosen here, never as a fallback.
  paymentProvider: "sandbox",
});

const PROVIDERS = ["sandbox", "stripe"];

/** @param {Record<string, string | undefined>} env */
export function readBuildConfig(env = process.env) {
  return {
    nodeEnv: env.NODE_ENV,
    /** Public settings set explicitly rather than defaulted (deployments must set all). */
    unsetPublic: ["NEXT_PUBLIC_APP_ORIGIN", "NEXT_PUBLIC_JAZZ_APP_ID"].filter((name) => !env[name]),
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
    paymentProvider: env.PAYMENT_PROVIDER || undefined,
    stripeSecretKey: env.STRIPE_SECRET_KEY || undefined,
    stripePublishableKey: env.NEXT_PUBLIC_STRIPE_PUBLISHABLE_KEY || undefined,
  };
}

/**
 * A local run is a non-production process (`next dev`, tests) on a loopback
 * origin. Anything else, including a production build that forgot to set
 * NEXT_PUBLIC_APP_ORIGIN, is a deployment and must be configured explicitly.
 * @param {ReturnType<typeof readBuildConfig>} config
 */
export function usesLocalDefaults(config = readBuildConfig()) {
  if (config.nodeEnv === "production") return false;
  const { hostname } = new URL(config.origin);
  return hostname === "127.0.0.1" || hostname === "localhost";
}

/**
 * The payment provider this deployment uses: explicit `PAYMENT_PROVIDER`, or
 * the documented local default. Stripe runs in test mode only.
 * @param {ReturnType<typeof readBuildConfig>} config
 * @returns {"sandbox" | "stripe"}
 */
export function paymentProvider(config = readBuildConfig()) {
  const provider =
    config.paymentProvider ??
    (usesLocalDefaults(config) ? LOCAL_DEFAULTS.paymentProvider : undefined);
  if (!provider) throw new Error("Jamazon deployments must set PAYMENT_PROVIDER");
  if (!PROVIDERS.includes(provider))
    throw new Error(`PAYMENT_PROVIDER must be one of ${PROVIDERS.join(", ")}; got ${provider}`);
  if (provider === "stripe") {
    if (!config.stripeSecretKey?.startsWith("sk_test_"))
      throw new Error("PAYMENT_PROVIDER=stripe requires a test-mode STRIPE_SECRET_KEY (sk_test_…)");
    if (!config.stripePublishableKey?.startsWith("pk_test_"))
      throw new Error(
        "PAYMENT_PROVIDER=stripe requires a test-mode NEXT_PUBLIC_STRIPE_PUBLISHABLE_KEY (pk_test_…)",
      );
  }
  return /** @type {"sandbox" | "stripe"} */ (provider);
}

/**
 * Reject incomplete configurations before any route runs. The Better Auth
 * secret is always required: there are no checked-in secrets, and `pnpm dev`
 * generates a local one (see dev-secrets.mjs). Deployments must also name
 * their origin, Jazz app and payment provider, and a remote sync server needs
 * both of the app's secrets.
 * @param {ReturnType<typeof readBuildConfig>} config
 */
export function assertBuildConfiguration(config = readBuildConfig()) {
  const local = usesLocalDefaults(config);
  const missing = [
    ...(local ? [] : config.unsetPublic),
    !local && !config.paymentProvider && "PAYMENT_PROVIDER",
    !config.betterAuthSecret && "BETTER_AUTH_SECRET",
    config.remote && !config.remote.adminSecret && "JAZZ_ADMIN_SECRET",
    config.remote && !config.remote.backendSecret && "BACKEND_SECRET",
  ].filter(Boolean);
  if (missing.length) {
    throw new Error(
      local
        ? `Jamazon is missing ${missing.join(", ")}. Start it with \`pnpm dev\`, which generates local secrets.`
        : `Jamazon deployments must set ${missing.join(", ")}.`,
    );
  }
  paymentProvider(config);
  return config;
}
