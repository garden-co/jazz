import { useLocalFirstAuthWithStore } from "../react-core/use-local-first-auth.js";
import {
  BrowserAuthSecretStore,
  browserAuthSecretStore,
  type BrowserAuthSecretStoreOptions,
} from "../runtime/auth-secret-store.js";

export type UseLocalFirstAuthOptions = Pick<
  BrowserAuthSecretStoreOptions,
  "key" | "authSecretStorageKey" | "appId" | "userId" | "sessionId"
>;

export function useLocalFirstAuth(options: UseLocalFirstAuthOptions = {}) {
  const hasCustomOptions = Object.values(options).some((value) => value !== undefined);
  const store = hasCustomOptions
    ? BrowserAuthSecretStore.getDefault(options)
    : browserAuthSecretStore;
  return useLocalFirstAuthWithStore(store);
}

export type { LocalFirstAuth } from "../react-core/use-local-first-auth.js";
