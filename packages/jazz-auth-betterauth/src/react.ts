import type { ClientOptions } from "better-auth";
import type {
  AuthSecretStorage,
  AuthenticateAccountFunction,
} from "jazz-tools";
import * as React from "react";
import { useEffect, useMemo, useState } from "react";
import { BetterAuth } from "./index.js";
import type { AuthClient } from "./index.js";

export interface JazzBetterAuthUser {
  id: string;
  name: string;
  encryptedCredentials?: unknown;
  [key: string]: any;
}

export type BetterAuthHookReturn<T extends ClientOptions> = {
  readonly state: "signedIn" | "anonymous";
  readonly logIn: () => Promise<void>;
  readonly signIn: () => Promise<void>;
  readonly authClient: AuthClient<T>;
  readonly account: JazzBetterAuthUser | undefined;
};

/**
 * Factory function to create a useBetterAuth hook with platform-specific hooks
 *
 * @param hooks - Object containing platform-specific hooks
 * @returns A useBetterAuth hook implementation
 */
export function createUseBetterAuthHook<T extends ClientOptions>(hooks: {
  useJazzContext: () => {
    authenticate: AuthenticateAccountFunction;
    [key: string]: any;
  };
  useAuthSecretStorage: () => AuthSecretStorage;
  useIsAuthenticated: () => boolean;
}) {
  const { useJazzContext, useAuthSecretStorage, useIsAuthenticated } = hooks;

  /**
   * Platform-agnostic implementation of useBetterAuth
   * @category Auth Providers
   */
  return function useBetterAuth<U extends ClientOptions = T>(
    options?: U,
  ): BetterAuthHookReturn<U> {
    const context = useJazzContext();
    const authSecretStorage = useAuthSecretStorage();

    if ("guest" in context) {
      throw new Error("Better Auth is not supported in guest mode");
    }

    const authMethod = useMemo(() => {
      return new BetterAuth(context.authenticate, authSecretStorage, options);
    }, [context.authenticate, authSecretStorage, options]);

    const isAuthenticated = useIsAuthenticated();
    const [account, setAccount] = useState<JazzBetterAuthUser | undefined>(
      undefined,
    );

    useEffect(() => {
      type Data = Awaited<
        ReturnType<typeof authMethod.authClient.getSession>
      >["data"];
      return authMethod.authClient.useSession.subscribe(
        async ({
          data,
        }: {
          data: Data;
        }) => {
          if (!data || !data.user) return;
          const user = data.user as JazzBetterAuthUser;
          if (user.encryptedCredentials === account?.encryptedCredentials)
            return;
          if (!user.encryptedCredentials) {
            await authMethod.signIn();
          } else if (!isAuthenticated) {
            await authMethod.logIn();
          }
          setAccount(user);
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
    };
  };
}

/**
 * Factory function to create an AuthContext and related hooks/providers
 *
 * @param useBetterAuth - The useBetterAuth hook implementation
 * @returns AuthContext, AuthProvider, and useAuth
 */
export function createAuthContext<T extends (options?: any) => any>(
  useBetterAuth: T,
) {
  type HookReturn = ReturnType<T>;
  const AuthContext = React.createContext<HookReturn | null>(null);

  function AuthProvider({
    children,
    options,
  }: {
    children: React.ReactNode;
    options?: Parameters<T>[0];
  }) {
    const auth = useBetterAuth(options);
    return React.createElement(AuthContext.Provider, { value: auth }, children);
  }

  function useAuth(): HookReturn {
    const context = React.useContext(AuthContext);
    if (!context) {
      throw new Error("useAuth must be used within an AuthProvider");
    }
    return context;
  }

  return { AuthContext, AuthProvider, useAuth };
}
