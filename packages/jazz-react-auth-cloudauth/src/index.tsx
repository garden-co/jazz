import {
  type AuthClient,
  CloudAuth,
  type Session,
  newAuthClient,
} from "jazz-auth-cloudauth";
import {
  useAuthSecretStorage,
  useIsAuthenticated,
  useJazzContext,
} from "jazz-react";
import { useEffect, useMemo } from "react";

/**
 * @category Auth Providers
 */
export function useCloudAuth(baseUrl: string): {
  readonly state: "signedIn" | "anonymous";
  readonly logIn: () => Promise<void>;
  readonly signIn: () => Promise<void>;
  readonly authClient: AuthClient;
} {
  const context = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();
  const authClient: AuthClient = newAuthClient(baseUrl);

  if ("guest" in context) {
    throw new Error("Cloud auth is not supported in guest mode");
  }

  const authMethod = useMemo(() => {
    return new CloudAuth(context.authenticate, authSecretStorage, authClient);
  }, []);

  const isAuthenticated = useIsAuthenticated();

  useEffect(() => {
    authClient.useSession.subscribe((value) => {
      authMethod.onUserChange(value.data as Pick<Session, "user">);
    });
  }, []);

  return {
    state: isAuthenticated ? "signedIn" : "anonymous",
    logIn: authMethod.logIn,
    signIn: authMethod.signIn,
    authClient,
  } as const;
}
