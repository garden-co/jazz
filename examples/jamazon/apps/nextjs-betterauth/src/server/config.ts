import { paymentProvider, readBuildConfig, usesLocalDefaults } from "@/src/lib/build-config.mjs";

const build = readBuildConfig();

/** Server-side configuration, resolved once. */
export const serverConfig = {
  origin: build.origin,
  appId: build.appId,
  serverUrl: build.serverUrl,
  isLocal: usesLocalDefaults(build),
  paymentProvider: paymentProvider(build),
  stripeSecretKey: build.stripeSecretKey,
};
