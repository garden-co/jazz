import { useLocalFirstAuthWithStore } from "../react-core/use-local-first-auth.js";
import type { LocalFirstAuth } from "../react-core/use-local-first-auth.js";
import {
  ExpoAuthSecretStore,
  expoAuthSecretStore,
  type ExpoAuthSecretStoreOptions,
} from "./auth-secret-store.js";

export type UseLocalFirstAuthOptions = Pick<ExpoAuthSecretStoreOptions, "key" | "profile"> & {
  /**
   * For apps that already have local-first users: the app id their secrets
   * were stored under, so they keep them. New apps leave it out.
   */
  scope?: string;
};

export function useLocalFirstAuth(options: UseLocalFirstAuthOptions = {}): LocalFirstAuth {
  const hasCustomOptions = Object.values(options).some((value) => value !== undefined);
  const store = hasCustomOptions
    ? ExpoAuthSecretStore.getDefault(secretStoreOptions(options))
    : expoAuthSecretStore;
  return useLocalFirstAuthWithStore(store);
}

function secretStoreOptions({ scope, ...options }: UseLocalFirstAuthOptions) {
  return scope === undefined ? options : { ...options, appId: scope };
}

export type { LocalFirstAuth } from "../react-core/use-local-first-auth.js";
