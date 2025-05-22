import { PassphraseAuth } from "jazz-tools";
import { useCallback, useMemo, useSyncExternalStore } from "react";
import { useAuthSecretStorage, useJazzContext } from "../hooks.js";
import { useIsAuthenticated } from "../hooks.js";

/**
 * `usePassphraseAuth` hook provides a `JazzAuth` object for passphrase authentication.
 *
 * @example
 * ```ts
 * const auth = usePassphraseAuth({ appName, appHostname, wordlist });
 * ```
 *
 * @category Auth Providers
 */
export function usePassphraseAuth({
  wordlist,
}: {
  wordlist: string[];
}) {
  const context = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();

  if ("guest" in context) {
    throw new Error("Passphrase auth is not supported in guest mode");
  }

  const authMethod = useMemo(() => {
    return new PassphraseAuth(
      context.node.crypto,
      context.authenticate,
      context.register,
      authSecretStorage,
      wordlist,
    );
  }, [wordlist]);

  const passphrase = useSyncExternalStore(
    useCallback(
      (callback) => {
        authMethod.loadCurrentAccountPassphrase();
        return authMethod.subscribe(callback);
      },
      [authMethod],
    ),
    () => authMethod.passphrase,
  );

  const isAuthenticated = useIsAuthenticated();
  return {
    state: isAuthenticated ? "signedIn" : "anonymous",
    logIn: authMethod.logIn,
    signUp: authMethod.signUp,
    registerNewAccount: authMethod.registerNewAccount,
    generateRandomPassphrase: authMethod.generateRandomPassphrase,
    passphrase,
  } as const;
}
