import { ID, SecretURLAuth, SecretURLAuthTransfer } from "jazz-tools";
import { useCallback, useMemo, useSyncExternalStore } from "react";
import {
  useAuthSecretStorage,
  useIsAuthenticated,
  useJazzContext,
} from "../hooks.js";

/**
 * `useSecretURLAuth` hook provides a `JazzAuth` object for secret URL authentication.
 * Allows you to create a pairing URL from an authenticated session and log in from another device.
 *
 * @param origin - The origin URL to use when generating the secret URL.
 *
 * @example
 * ```ts
 * const auth = useSecretURLAuth(window.location.origin);
 * ```
 *
 * @category Auth Providers
 */
export function useSecretURLAuth(origin: string) {
  const context = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();

  if ("guest" in context) {
    throw new Error("Secret URL auth is not supported in guest mode");
  }

  const authMethod = useMemo(() => {
    return new SecretURLAuth(
      context.node.crypto,
      context.authenticate,
      authSecretStorage,
      origin,
    );
  }, [origin]);

  const role = useSyncExternalStore(
    useCallback((callback) => authMethod.subscribe(callback), [authMethod]),
    () => authMethod.role,
  );

  const createUrl = async (expiresAt?: Date) => {
    const result = await authMethod.createAuthTransferURL(expiresAt);
    return result.url;
  };

  const isAuthenticated = useIsAuthenticated();

  return {
    state: isAuthenticated ? "signedIn" : "anonymous",
    logIn: authMethod.logIn,
    createUrl,
    role,
  } as const;
}
