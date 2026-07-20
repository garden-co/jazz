import { useLocalFirstAuthWithStore } from "../react-core/use-local-first-auth.js";
import type { LocalFirstAuth } from "../react-core/use-local-first-auth.js";
import {
  ExpoAuthSecretStore,
  expoAuthSecretStore,
  type ExpoAuthSecretStoreOptions,
} from "./auth-secret-store.js";

export type UseLocalFirstAuthOptions = Pick<
  ExpoAuthSecretStoreOptions,
  "key" | "authSecretStorageKey" | "appId" | "userId" | "sessionId"
>;

export function useLocalFirstAuth(options: UseLocalFirstAuthOptions = {}): LocalFirstAuth {
  const hasCustomOptions = Object.values(options).some((value) => value !== undefined);
  const store = hasCustomOptions ? ExpoAuthSecretStore.getDefault(options) : expoAuthSecretStore;
  return useLocalFirstAuthWithStore(store);
}

export type { LocalFirstAuth } from "../react-core/use-local-first-auth.js";
