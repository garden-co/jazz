export const LOCAL_DEFAULTS = Object.freeze({
  origin: "http://127.0.0.1:3000",
  appId: "jamazon-local",
  serverUrl: "http://127.0.0.1:4200",
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
    unsetPublic: [
      "NEXT_PUBLIC_APP_ORIGIN",
      "NEXT_PUBLIC_JAZZ_APP_ID",
      "NEXT_PUBLIC_JAZZ_SERVER_URL",
    ].filter((name) => !env[name]),
    origin: env.NEXT_PUBLIC_APP_ORIGIN || LOCAL_DEFAULTS.origin,
    appId: env.NEXT_PUBLIC_JAZZ_APP_ID || LOCAL_DEFAULTS.appId,
    serverUrl: env.NEXT_PUBLIC_JAZZ_SERVER_URL || LOCAL_DEFAULTS.serverUrl,
    backendSecret: env.BACKEND_SECRET || undefined,
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
 * The app id and server URL are not part of the test: `withJazz` generates
 * both for `next dev`.
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
 * Reject incomplete configurations before any route runs. Both secrets are
 * always required: there are no checked-in secrets, and `pnpm dev` generates
 * local ones (see dev-secrets.mjs). Deployments must also name their origin,
 * Jazz app and server, and payment provider.
 * @param {ReturnType<typeof readBuildConfig>} config
 */
export function assertBuildConfiguration(config = readBuildConfig()) {
  const local = usesLocalDefaults(config);
  const missing = [
    ...(local ? [] : config.unsetPublic),
    !local && !config.paymentProvider && "PAYMENT_PROVIDER",
    !config.backendSecret && "BACKEND_SECRET",
    !config.betterAuthSecret && "BETTER_AUTH_SECRET",
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
