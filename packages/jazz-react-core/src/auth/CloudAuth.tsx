import {
  type AuthClient,
  CloudAuth,
  type Session,
  newAuthClient,
} from "jazz-tools";
import { useEffect, useMemo } from "react";
import { useAuthSecretStorage, useJazzContext } from "../hooks.js";
import { useIsAuthenticated } from "../hooks.js";

/**
 * @category Auth Providers
 */
export function useCloudAuth(): {
  readonly state: "signedIn" | "anonymous";
  readonly logIn: (session: Pick<Session, "user">) => Promise<void>;
  readonly signIn: (session: Pick<Session, "user">) => Promise<void>;
  readonly authClient: AuthClient;
} {
  const context = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();
  const authClient: AuthClient = newAuthClient("http://localhost:3000");
  const keyserver = "http://localhost:6189";

  if ("guest" in context) {
    throw new Error("Cloud auth is not supported in guest mode");
  }

  const authMethod = useMemo(() => {
    return new CloudAuth(
      context.authenticate,
      authSecretStorage,
      authClient,
      keyserver,
    );
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
