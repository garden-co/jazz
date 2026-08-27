import { useLocalFirstAuthWithStore } from "../react-core/use-local-first-auth.js";
import type { BrowserAuthSecretStoreOptions } from "../runtime/auth-secret-store.js";
import type { AuthSecretStore } from "../runtime/auth-secret-store.js";

export type UseLocalFirstAuthOptions = Pick<
  BrowserAuthSecretStoreOptions,
  "key" | "appId" | "profile"
> & {
  /** Native secure-store adapter; required because React Native has no localStorage. */
  store: AuthSecretStore;
};

export const REACT_NATIVE_AUTH_SECRET_STORE_REQUIRED_ERROR =
  "React Native local-first auth requires options.store backed by native secure storage. " +
  "For Expo, pass new ExpoAuthSecretStore() from jazz-tools/expo; do not use browser localStorage.";

/**
 * React Native has no portable built-in secure store. Inject the platform
 * adapter explicitly; Expo users can pass `new ExpoAuthSecretStore()`.
 */
export function useLocalFirstAuth(
  options: UseLocalFirstAuthOptions,
): ReturnType<typeof useLocalFirstAuthWithStore> {
  if (!options.store) {
    throw new Error(REACT_NATIVE_AUTH_SECRET_STORE_REQUIRED_ERROR);
  }
  return useLocalFirstAuthWithStore(options.store);
}

export type { LocalFirstAuth } from "../react-core/use-local-first-auth.js";
