import { DefaultRuntimeSource } from "../runtime/default-runtime-source.js";
import { authSecretSeedForMinting } from "../runtime/auth-secret-codec.js";
import type { RuntimeSourcesConfig } from "../runtime/context.js";
import { clientScope, DEFAULT_SERVER_URL } from "../runtime/client-scope.js";
import type { AccountStore } from "./persistence.js";
import { createAccountManagerWithMinter } from "./account-manager-core.js";

export interface AccountManagerConfig {
  /**
   * Backend and test code only; a browser client leaves it out and is scoped
   * by `serverUrl` (see `clientScope`).
   */
  appId?: string;
  /** The app id an existing local-first app's users were created under. */
  scope?: string;
  /** The app's Jazz endpoint (default `"/jazz"`, its own backend). */
  serverUrl?: string;
  env?: string;
  runtimeSources?: RuntimeSourcesConfig;
  /**
   * Supply durable storage on native/server hosts; browser defaults to
   * localStorage. Only that browser default reopens the last external account
   * before its provider confirms it.
   */
  store?: AccountStore;
}

/** Prepare crypto and restore selection; createLocalFirst() is synchronous afterward. */
export async function createAccountManager(config: AccountManagerConfig) {
  const scope = clientScope({ ...config, serverUrl: config.serverUrl ?? DEFAULT_SERVER_URL });
  const source = new DefaultRuntimeSource();
  await source.load({ appId: scope, runtimeSources: config.runtimeSources });
  return createAccountManagerWithMinter(config, (secret, audience) =>
    source.mintLocalFirstToken({
      secret: authSecretSeedForMinting(secret),
      audience,
      ttlSeconds: 3600,
      nowSeconds: BigInt(Math.floor(Date.now() / 1000)),
    }),
  );
}
