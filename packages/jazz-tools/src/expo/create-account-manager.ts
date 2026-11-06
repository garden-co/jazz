import { CryptoDigestAlgorithm, digestStringAsync } from "expo-crypto";
import { getItem, getItemAsync, setItem } from "expo-secure-store";
import { clientRegistryUrl } from "../accounts/account-manager-core.js";
import type { AccountStore } from "../accounts/persistence.js";
import type { AppBearer } from "../accounts/enrollment.js";
import { createAccountManager as createNativeAccountManager } from "../react-native/create-account-manager.js";

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
  /** Separate retained account selections within the same application. */
  profile?: string;
  env?: string;
  /**
   * The app's own token for its backend (an app has no cookie to send), read
   * whenever it is needed: sent as `Authorization: Bearer` when signing in
   * through the backend (`loginWithBackend`) and on the device's sync
   * connection, where the backend's `authenticate` reads who the user is.
   */
  bearer?: AppBearer;
}

/** Prepare retained accounts without opening a database or a Jazz context. */
export async function createAccountManager(config: AccountManagerConfig) {
  const registry = clientRegistryUrl(config);
  const native = await import("jazz-rn/relay");
  const factory = native.installNativeForegroundRuntime();
  if (typeof factory.withAccountStoreLock !== "function") {
    throw new Error("Expo accounts require a matching native build with atomic account storage");
  }
  const scope = JSON.stringify([
    "jazz-account-selection-v1",
    registry,
    config.env ?? "dev",
    config.profile ?? "default",
  ]);
  const digest = await digestStringAsync(CryptoDigestAlgorithm.SHA256, scope);
  const key = `jazz.account-selection-v1.${digest}`;
  const store: AccountStore = {
    read: () => getItemAsync(key),
    async update(transform) {
      factory.withAccountStoreLock!(() => {
        // SecureStore's synchronous operations keep the entire read/modify/write
        // inside the native lock. No biometric prompts or asynchronous work.
        const replacement = transform(getItem(key));
        setItem(key, replacement);
      });
    },
  };
  return createNativeAccountManager({ ...config, store });
}
