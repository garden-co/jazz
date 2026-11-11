import { installNativeRuntimeEntropy } from "../runtime/runtime-entropy.js";
import { clientRegistryUrl, deviceOnlyFetch } from "../accounts/account-manager-core.js";
import { clientScope } from "../runtime/client-scope.js";
import { prepareAccountManager, type AccountStore } from "../accounts/persistence.js";
import type { AppBearer } from "../accounts/enrollment.js";
import { formatAuthSecret, parseAuthSecret } from "../runtime/auth-secret-codec.js";
import { importNativeRelay } from "./native-relay.js";

export interface AccountManagerConfig {
  /**
   * Backend and test code only; a browser client leaves it out and is scoped
   * by `serverUrl` (see `clientScope`).
   */
  appId?: string;
  /** The app id an existing local-first app's users were created under. */
  scope?: string;
  /**
   * The app's Jazz endpoint, its backend's `createJazz` mount. `null` keeps
   * Jazz on this device only: no server, no sign-in, one local-first account.
   */
  serverUrl: string | null;
  /** Atomic OS-protected storage scoped to this application, registry, and profile. */
  store: AccountStore;
  /**
   * The app's own token for its backend (an app has no cookie to send), read
   * whenever it is needed: sent as `Authorization: Bearer` when signing in
   * through the backend (`loginWithBackend`) and on the device's sync
   * connection, where the backend's `authenticate` reads who the user is.
   */
  bearer?: AppBearer;
}

/** Prepare native crypto and retained selection without opening a Jazz context. */
export async function createAccountManager(config: AccountManagerConfig) {
  const registry = clientRegistryUrl(config);
  const native = await importNativeRelay();
  const crypto = native.installNativeForegroundRuntime();
  const generateSecret = crypto.accountSecret;
  const mintToken = crypto.mintLocalFirstToken;
  if (typeof generateSecret !== "function" || typeof mintToken !== "function") {
    throw new Error(
      "React Native accounts require a matching native build with account crypto support",
    );
  }
  if (!config.store)
    throw new Error("React Native accounts require an atomic OS-protected AccountStore");
  installNativeRuntimeEntropy(generateSecret);
  return prepareAccountManager({
    appId: clientScope(config),
    registry,
    store: config.store,
    bearer: config.bearer,
    ...(config.serverUrl === null ? { fetch: deviceOnlyFetch } : {}),
    generateSecret: () => formatAuthSecret(generateSecret()),
    mintToken: (secret, audience) =>
      mintToken(parseAuthSecret(secret), audience, 3600, Math.floor(Date.now() / 1000)),
  });
}
