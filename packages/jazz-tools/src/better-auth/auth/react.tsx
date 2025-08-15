"use client";

import type { ClientOptions } from "better-auth";
import { createAuthClient } from "better-auth/client";
import type {
  Account,
  AccountClass,
  AnyAccountSchema,
  CoValueFromRaw,
} from "jazz-tools";
import {
  type JazzProviderProps,
  JazzReactProvider,
  useAuthSecretStorage,
  useJazzContext,
} from "jazz-tools/react";
import { useEffect } from "react";
import { type PropsWithChildren, createContext } from "react";
import { jazzPluginClient } from "./client.js";

type AuthClient = ReturnType<
  typeof createAuthClient<{
    plugins: [ReturnType<typeof jazzPluginClient>];
  }>
>;

export const AuthContext = createContext<AuthClient | null>(null);

/**
 * @param props.children - The children to render.
 * @param props.betterAuthClient - The BetterAuth client with the Jazz plugin.
 *
 * @example
 * ```ts
 * const betterAuthClient = createAuthClient({
 *   plugins: [
 *     jazzPluginClient(),
 *   ],
 * });
 *
 * <AuthProvider betterAuthClient={betterAuthClient}>
 *   <App />
 * </AuthProvider>
 * ```
 */
export function AuthProvider({
  children,
  betterAuthClient,
}: PropsWithChildren<{
  betterAuthClient: AuthClient;
}>) {
  const context = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();

  if (betterAuthClient.jazz === undefined) {
    throw new Error(
      "Better Auth client has been initialized without the jazzPluginClient",
    );
  }

  useEffect(() => {
    betterAuthClient.jazz.setJazzContext(context);
    betterAuthClient.jazz.setAuthSecretStorage(authSecretStorage);

    // We need to subscribe to the session to let the plugin keep sync Jazz's and BetterAuth's session
    return betterAuthClient.useSession.subscribe(() => {});
  }, [betterAuthClient, context, authSecretStorage]);

  return children;
}

/**
 * @param props - The props for the JazzReactProvider.
 * @param props.betterAuth - The options for the BetterAuth client.
 * @returns The JazzReactProvider with the BetterAuth plugin.
 *
 * @example
 * ```ts
 * <JazzReactProviderWithBetterAuth
 *  betterAuth={{
 *    baseURL: "http://localhost:3000",
 *  }}
 *  sync={{
 *    peer: "ws://localhost:4200",
 *  }}
 * >
 *   <App />
 * </JazzReactProviderWithBetterAuth>
 * ```
 */
export const JazzReactProviderWithBetterAuth = <
  S extends
    | (AccountClass<Account> & CoValueFromRaw<Account>)
    | AnyAccountSchema,
>(
  props: { betterAuthClient: AuthClient } & JazzProviderProps<S>,
) => {
  return (
    <JazzReactProvider {...props}>
      <AuthProvider betterAuthClient={props.betterAuthClient}>
        {props.children}
      </AuthProvider>
    </JazzReactProvider>
  );
};
