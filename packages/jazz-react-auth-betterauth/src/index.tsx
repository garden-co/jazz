import type { ClientOptions } from "better-auth";
import { BetterAuth } from "jazz-auth-betterauth";
import {
  useAuthSecretStorage,
  useIsAuthenticated,
  useJazzContext,
} from "jazz-react";
import { useEffect, useMemo } from "react";

// biome-ignore lint/correctness/useImportExtensions: <explanation>
export * from "./contexts/Auth";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
export * from "./types/auth";
// biome-ignore lint/correctness/useImportExtensions: <explanation>
export * from "./lib/social";

/**
 * @category Auth Providers
 */
export function useBetterAuth<T extends ClientOptions>(options?: T) {
  const context = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();

  if ("guest" in context) {
    throw new Error("Better Auth is not supported in guest mode");
  }

  const authMethod = useMemo(() => {
    return new BetterAuth(context.authenticate, authSecretStorage, options);
  }, [context.authenticate, authSecretStorage, options]);

  const isAuthenticated = useIsAuthenticated();

  useEffect(() => {
    return authMethod.authClient.useSession.subscribe((value) => {
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
