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
import { useMemo } from "react";
import { type PropsWithChildren, createContext, useContext } from "react";
import { jazzPluginClient } from "./client.js";

type AuthClient = ReturnType<typeof createAuthClient>;

export const AuthContext = createContext<AuthClient | null>(null);

function AuthProvider({
  children,
  options,
}: PropsWithChildren<{
  options?: ClientOptions;
}>) {
  const context = useJazzContext();
  const authSecretStorage = useAuthSecretStorage();

  const authClient = useMemo(
    () =>
      createAuthClient({
        ...options,
        plugins: [
          ...(options?.plugins || []),
          jazzPluginClient(
            context.authenticate,
            context.logOut,
            authSecretStorage,
          ),
        ],
      }),
    [options],
  );

  return (
    <AuthContext.Provider value={authClient}>{children}</AuthContext.Provider>
  );
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
  props: { betterAuth?: ClientOptions } & JazzProviderProps<S>,
) => {
  return (
    <JazzReactProvider {...props}>
      <AuthProvider options={props.betterAuth}>{props.children}</AuthProvider>
    </JazzReactProvider>
  );
};

export function useBetterAuth<T extends ClientOptions>() {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used within an AuthProvider");
  }
  return context as unknown as ReturnType<typeof createAuthClient<T>>;
}
