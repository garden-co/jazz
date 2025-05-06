import { BetterAuth, newAuthClient } from "jazz-auth-betterauth";
import {
  useAuthSecretStorage,
  useIsAuthenticated,
  useJazzContext,
} from "jazz-react";
import { useEffect, useMemo } from "react";

/**
 * @category Auth Providers
 */
export function useBetterAuth<T extends ReturnType<typeof newAuthClient>>(
  authClient: T,
) {
  const context = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();

  if ("guest" in context) {
    throw new Error("Better Auth is not supported in guest mode");
  }

  const authMethod = useMemo(() => {
    return new BetterAuth(context.authenticate, authSecretStorage, authClient);
  }, [context.authenticate, authSecretStorage, authClient]);

  const isAuthenticated = useIsAuthenticated();

  useEffect(() => {
    authMethod.authClient.useSession.subscribe((value) => {
      authMethod.onUserChange(value.data ?? undefined);
    });
  }, [isAuthenticated]);

  return {
    state: isAuthenticated
      ? "signedIn"
      : ("anonymous" as "signedIn" | "anonymous"),
    logIn: authMethod.logIn as () => Promise<void>,
    signIn: authMethod.signIn as () => Promise<void>,
    authClient: authMethod.authClient,
  } as const;
}
