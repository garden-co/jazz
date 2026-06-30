import { useLocalFirstAuthWithStore } from "../react-core/use-local-first-auth.js";
import {
  BrowserAuthSecretStore,
  browserAuthSecretStore,
  type BrowserAuthSecretStoreOptions,
} from "../runtime/auth-secret-store.js";
import type { LocalFirstAuth } from "../react-core/use-local-first-auth.js";

export type UseLocalFirstAuthOptions = Pick<BrowserAuthSecretStoreOptions, "authSecretStorageKey">;

export function useLocalFirstAuth(options?: UseLocalFirstAuthOptions): LocalFirstAuth {
  const store = options?.authSecretStorageKey
    ? BrowserAuthSecretStore.getDefault({ authSecretStorageKey: options.authSecretStorageKey })
    : browserAuthSecretStore;
  return useLocalFirstAuthWithStore(store);
}

export type { LocalFirstAuth } from "../react-core/use-local-first-auth.js";
