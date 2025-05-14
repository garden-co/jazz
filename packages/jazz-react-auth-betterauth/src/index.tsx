import type { ClientOptions } from "better-auth";
import { BetterAuth } from "jazz-auth-betterauth";
import {
  useAuthSecretStorage,
  useIsAuthenticated,
  useJazzContext,
} from "jazz-react";
import { useEffect, useMemo, useState } from "react";

export * from "./contexts/Auth.js";
export * from "./types/auth.js";
export * from "./lib/social.js";

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
  type Data = Awaited<
    ReturnType<typeof authMethod.authClient.getSession<{}>>
  >["data"];
  type User = NonNullable<Data>["user"];
  const [account, setAccount] = useState<User | undefined>(undefined);

  useEffect(() => {
    return authMethod.authClient.useSession.subscribe(
      async ({
        data,
      }: {
        data: Data;
      }) => {
        if (!data || !data.user) return;
        if (data.user.encryptedCredentials === account?.encryptedCredentials)
          return;
        if (!data.user.encryptedCredentials) {
          await authMethod.signIn();
        } else if (!isAuthenticated) {
          await authMethod.logIn();
        }
        setAccount(data.user);
      },
    );
  }, [authMethod.authClient, account, isAuthenticated]);

  return {
    state: isAuthenticated
      ? "signedIn"
      : ("anonymous" as "signedIn" | "anonymous"),
    logIn: authMethod.logIn as () => Promise<void>,
    signIn: authMethod.signIn as () => Promise<void>,
    authClient: authMethod.authClient,
    account: account,
  } as const;
}
