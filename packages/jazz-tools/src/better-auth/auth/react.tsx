"use client";

import { createAuthClient } from "better-auth/client";
import {
  useAuthSecretStorage,
  useJazzContextValue,
} from "jazz-tools/react-core";
import { useEffect } from "react";
import { type PropsWithChildren } from "react";
import { jazzPluginClient } from "./client.js";

type AuthClient = ReturnType<
  typeof createAuthClient<{
    plugins: [ReturnType<typeof jazzPluginClient>];
  }>
>;

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
  const context = useJazzContextValue();
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
