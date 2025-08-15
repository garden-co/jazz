import { PassphraseAuth } from "jazz-tools";
import { computed, markRaw, ref, watchEffect } from "vue";
import { useAuthSecretStorage, useJazzContext } from "../composables.js";
import { useIsAuthenticated } from "./useIsAuthenticated.js";

/**
 * `usePassphraseAuth` composable provides a `JazzAuth` object for passphrase authentication.
 *
 * @example
 * ```ts
 * const auth = usePassphraseAuth({ wordlist });
 * ```
 *
 * @category Auth Providers
 */
export function usePassphraseAuth({ wordlist }: { wordlist: string[] }) {
  const context = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();
  const isAuthenticated = useIsAuthenticated();

  if ("guest" in context.value) {
    throw new Error("Passphrase auth is not supported in guest mode");
  }

  const authMethod = computed(() => {
    return markRaw(
      new PassphraseAuth(
        context.value.node.crypto,
        context.value.authenticate,
        context.value.register,
        authSecretStorage,
        wordlist,
      ),
    );
  });

  const passphrase = ref(authMethod.value.passphrase);

  watchEffect((onCleanup) => {
    authMethod.value.loadCurrentAccountPassphrase();

    const unsubscribe = authMethod.value.subscribe(() => {
      passphrase.value = authMethod.value.passphrase;
    });

    onCleanup(unsubscribe);
  });

  return computed(() => ({
    state: isAuthenticated.value ? "signedIn" : "anonymous",
    logIn: authMethod.value.logIn,
    signUp: authMethod.value.signUp,
    registerNewAccount: authMethod.value.registerNewAccount,
    generateRandomPassphrase: authMethod.value.generateRandomPassphrase,
    passphrase: passphrase.value,
  }));
}
