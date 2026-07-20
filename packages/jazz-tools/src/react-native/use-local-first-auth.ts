import { useLocalFirstAuthWithStore } from "../react-core/use-local-first-auth.js";
import {
  BrowserAuthSecretStore,
  type BrowserAuthSecretStoreOptions,
} from "../runtime/auth-secret-store.js";
import type { AuthSecretStore } from "../runtime/auth-secret-store.js";

export type UseLocalFirstAuthOptions = Pick<
  BrowserAuthSecretStoreOptions,
  "key" | "authSecretStorageKey" | "appId" | "userId" | "sessionId"
> & {
  store?: AuthSecretStore;
};

export function useLocalFirstAuth(options: UseLocalFirstAuthOptions = {}) {
  const { store, ...storeOptions } = options;
  const resolvedStore = store ?? BrowserAuthSecretStore.getDefault(storeOptions);
  return useLocalFirstAuthWithStore(resolvedStore);
}

export type { LocalFirstAuth } from "../react-core/use-local-first-auth.js";
