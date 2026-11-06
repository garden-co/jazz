import { DefaultRuntimeSource } from "../runtime/default-runtime-source.js";
import { authSecretSeedForMinting } from "../runtime/auth-secret-codec.js";
import type { RuntimeSourcesConfig } from "../runtime/context.js";
import { clientScope } from "../runtime/client-scope.js";
import type { AccountStore } from "./persistence.js";
import { createAccountManagerWithMinter, resolvedServerUrl } from "./account-manager-core.js";
import type { AppBearer } from "./enrollment.js";

export interface AccountManagerConfig {
  /**
   * Backend and test code only; a browser client leaves it out and is scoped
   * by `serverUrl` (see `clientScope`).
   */
  appId?: string;
  /** The app id an existing local-first app's users were created under. */
  scope?: string;
  /**
   * The app's Jazz endpoint (default `"/jazz"`, its own backend), or `null`
   * to keep Jazz on this device only, with no server and no sign-in.
   */
  serverUrl?: string | null;
  env?: string;
  runtimeSources?: RuntimeSourcesConfig;
  /**
   * Supply durable storage on native/server hosts; browser defaults to
   * localStorage. Only that browser default reopens the last external account
   * before its provider confirms it.
   */
  store?: AccountStore;
  /**
   * Browser hosts only: the `fetch` sign-in requests to the app's backend go
   * through (default: the global one), as for a test or a proxy. The backend
   * names the user by the cookie a browser sends on the sync connection too,
   * so a bearer this adds would sign in but never sync: pass `bearer`
   * instead. Outside a browser page or worker (Node, Deno, a CLI) without
   * `bearer`, `loginWithBackend` and `linkWithBackend` refuse without
   * calling it.
   */
  fetch?: typeof fetch;
  /**
   * Hosts without cookies only (Node, Deno, Bun, a CLI): the app's own token
   * for its backend, read whenever it is needed. It is sent as
   * `Authorization: Bearer` when signing in through the backend
   * (`loginWithBackend`, `linkWithBackend`) and on every sync connection,
   * thin and local-first, where the backend's `authenticate` reads who the
   * user is. Return null while no one is signed in.
   *
   * Not supported yet in a browser page or worker, whose sync connection
   * can't carry a header: there the backend's cookie names the user. On
   * React Native, pass `bearer` to `createAccountManager` from
   * `jazz-tools/react-native` or `jazz-tools/expo`.
   */
  bearer?: AppBearer;
}

/** Prepare crypto and restore selection; createLocalFirst() is synchronous afterward. */
export async function createAccountManager(config: AccountManagerConfig) {
  const scope = clientScope({ ...config, serverUrl: resolvedServerUrl(config) });
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
