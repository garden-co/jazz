import { BrowserPasskeyAuth } from "jazz-tools/browser";
import { computed, markRaw } from "vue";
import { useAuthSecretStorage, useJazzContext } from "../composables.js";
import { useIsAuthenticated } from "./useIsAuthenticated.js";

/**
 * `usePasskeyAuth` composable provides a `JazzAuth` object for passkey authentication.
 *
 * @example
 * ```ts
 * const auth = usePasskeyAuth({ appName, appHostname });
 * ```
 *
 * @category Auth Providers
 */
export function usePasskeyAuth({
  appName,
  appHostname,
}: {
  appName: string;
  appHostname?: string;
}) {
  const context = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();
  const isAuthenticated = useIsAuthenticated();

  if ("guest" in context.value) {
    throw new Error("Passkey auth is not supported in guest mode");
  }

  const authMethod = computed(() => {
    return markRaw(
      new BrowserPasskeyAuth(
        context.value.node.crypto,
        context.value.authenticate,
        authSecretStorage,
        appName,
        appHostname,
      ),
    );
  });

  return computed(() => ({
    state: isAuthenticated.value ? "signedIn" : "anonymous",
    logIn: authMethod.value.logIn,
    signUp: authMethod.value.signUp,
  }));
}
