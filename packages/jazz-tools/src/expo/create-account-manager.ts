import { CryptoDigestAlgorithm, digestStringAsync } from "expo-crypto";
import { getItem, getItemAsync, setItem } from "expo-secure-store";
import { accountRegistryUrl } from "../accounts/context.js";
import type { AccountStore } from "../accounts/persistence.js";
import { createAccountManager as createNativeAccountManager } from "../react-native/create-account-manager.js";

export interface AccountManagerConfig {
  appId: string;
  serverUrl: string;
  /** Separate retained account selections within the same application. */
  profile?: string;
  env?: string;
}

/** Prepare retained accounts without opening a database or a Jazz context. */
export async function createAccountManager(config: AccountManagerConfig) {
  const registry = accountRegistryUrl(config.serverUrl, config.appId);
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
