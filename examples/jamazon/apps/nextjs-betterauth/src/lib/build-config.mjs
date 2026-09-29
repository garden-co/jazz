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
    origin: env.NEXT_PUBLIC_APP_ORIGIN ?? LOCAL_DEFAULTS.origin,
    appId: env.NEXT_PUBLIC_JAZZ_APP_ID ?? LOCAL_DEFAULTS.appId,
    serverUrl: env.NEXT_PUBLIC_JAZZ_SERVER_URL ?? LOCAL_DEFAULTS.serverUrl,
    backendSecret: env.BACKEND_SECRET,
    betterAuthSecret: env.BETTER_AUTH_SECRET,
    paymentProvider: env.PAYMENT_PROVIDER,
    stripeSecretKey: env.STRIPE_SECRET_KEY,
    stripePublishableKey: env.NEXT_PUBLIC_STRIPE_PUBLISHABLE_KEY,
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

/**
 * The payment provider this deployment uses: explicit `PAYMENT_PROVIDER`, or
 * the documented local default. Stripe runs in test mode only.
 * @param {ReturnType<typeof readBuildConfig>} config
 * @returns {"sandbox" | "stripe"}
 */
export function paymentProvider(config = readBuildConfig()) {
  const provider =
    config.paymentProvider ?? (usesLocalDefaults(config) ? LOCAL_DEFAULTS.paymentProvider : undefined);
  if (!provider) throw new Error("Jamazon nonlocal configuration requires PAYMENT_PROVIDER");
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

/** Reject partial/nonlocal configurations before Next evaluates any route. */
/** @param {ReturnType<typeof readBuildConfig>} config */
export function assertBuildConfiguration(config = readBuildConfig()) {
  paymentProvider(config);
  if (usesLocalDefaults(config)) return config;
  const missing = [
    !config.backendSecret && "BACKEND_SECRET",
    !config.betterAuthSecret && "BETTER_AUTH_SECRET",
  ].filter(Boolean);
  if (missing.length) {
    throw new Error(
      `Jamazon nonlocal configuration requires BACKEND_SECRET and BETTER_AUTH_SECRET; missing: ${missing.join(", ")}`,
    );
  }
  return config;
}

if (import.meta.url === new URL(process.argv[1], "file:").href) {
  const config = assertBuildConfiguration();
  console.log(
    `Jamazon build config: ${usesLocalDefaults(config) ? "checked-in local defaults" : "configured nonlocal deployment"}, ${paymentProvider(config)} payments`,
  );
}
