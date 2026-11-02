import {
  assertBuildConfiguration,
  paymentProvider,
  readBuildConfig,
} from "@/src/lib/build-config.mjs";

// Next inlines NEXT_PUBLIC_* values only where the code spells out
// `process.env.NAME`, so read them that way rather than through a variable.
const build = assertBuildConfiguration(
  readBuildConfig({
    ...process.env,
    NEXT_PUBLIC_APP_ORIGIN: process.env.NEXT_PUBLIC_APP_ORIGIN,
    NEXT_PUBLIC_JAZZ_APP_ID: process.env.NEXT_PUBLIC_JAZZ_APP_ID,
    NEXT_PUBLIC_STRIPE_PUBLISHABLE_KEY: process.env.NEXT_PUBLIC_STRIPE_PUBLISHABLE_KEY,
  }),
);

/**
 * Server-side configuration, resolved and validated once. There are no
 * fallback secrets: `assertBuildConfiguration` refuses to start without them.
 */
export const serverConfig = {
  origin: build.origin,
  appId: build.appId,
  /** A remote sync server (Jazz Cloud), or undefined for local SQLite. */
  remote: build.remote as { url: string; adminSecret: string; backendSecret: string } | undefined,
  dataPath: build.dataPath,
  paymentProvider: paymentProvider(build),
  stripeSecretKey: build.stripeSecretKey,
  betterAuthSecret: build.betterAuthSecret!,
};
