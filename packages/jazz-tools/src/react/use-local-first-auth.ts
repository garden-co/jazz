import { useLocalFirstAuthWithStore } from "../react-core/use-local-first-auth.js";
import {
  BrowserAuthSecretStore,
  browserAuthSecretStore,
  type BrowserAuthSecretStoreOptions,
} from "../runtime/auth-secret-store.js";

export type UseLocalFirstAuthOptions = Pick<BrowserAuthSecretStoreOptions, "key" | "profile"> & {
  /**
   * For apps that already have local-first users: the app id their secrets
   * were stored under, so they keep them. New apps leave it out.
   */
  scope?: string;
};

export function useLocalFirstAuth(options: UseLocalFirstAuthOptions = {}) {
  const hasCustomOptions = Object.values(options).some((value) => value !== undefined);
  const store = hasCustomOptions
    ? BrowserAuthSecretStore.getDefault(secretStoreOptions(options))
    : browserAuthSecretStore;
  return useLocalFirstAuthWithStore(store);
}

function secretStoreOptions({ scope, ...options }: UseLocalFirstAuthOptions) {
  return scope === undefined ? options : { ...options, appId: scope };
}

export type { LocalFirstAuth } from "../react-core/use-local-first-auth.js";
