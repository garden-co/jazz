import type { BetterAuthClientPlugin } from "better-auth";
import type {
  AuthSecretStorage,
  AuthenticateAccountFunction,
} from "jazz-tools";

/**
 * @param authenticationFunction - The function to authenticate the user, usually JazzContextManager.authenticate
 * @param logOutFunction - The function to log out the user, usually JazzContextManager.logOut
 * @param authSecretStorage - The storage to store the auth secret, usually JazzContextManager.authSecretStorage
 * @returns The BetterAuth client plugin.
 *
 * @example
 * ```ts
 * const context = useJazzContext();
 * const authSecretStorage = useAuthSecretStorage();
 * const auth = betterAuth({
 *   plugins: [jazzPluginClient(context.authenticate, context.logOut, authSecretStorage)],
 * });
 * ```
 */
export const jazzPluginClient = (
  authenticationFunction: AuthenticateAccountFunction,
  logOutFunction: () => Promise<void> | void,
  authSecretStorage: AuthSecretStorage,
) => {
  return {
    id: "jazz-plugin",
    // $InferServerPlugin: {} as ReturnType<typeof jazzPlugin>,
    fetchPlugins: [
      {
        id: "jazz-plugin",
        name: "jazz-plugin",
        hooks: {
          async onRequest(context) {
            if (context.url.toString().includes("/sign-up")) {
              const oldBody = JSON.parse(context.body);

              const credentials = await authSecretStorage.get();
              context.body = JSON.stringify({
                ...oldBody,
                jazzAuth: {
                  ...credentials,
                  // If the provider remains 'anonymous', Jazz will not consider us authenticated later.
                  provider: "better-auth",
                },
              });
            }
          },
          async onSuccess(context) {
            if (context.request.url.toString().includes("/sign-up")) {
              const jazzAuth = {
                ...context.data.jazzAuth,
                secretSeed: Uint8Array.from(context.data.jazzAuth.secretSeed),
              };

              await authenticationFunction(jazzAuth);
              await authSecretStorage.set(jazzAuth);

              return;
            }

            if (context.request.url.toString().includes("/sign-in")) {
              const jazzAuth = {
                ...context.data.jazzAuth,
                secretSeed: Uint8Array.from(context.data.jazzAuth.secretSeed),
              };

              await authenticationFunction(jazzAuth);
              await authSecretStorage.set(jazzAuth);

              return;
            }

            if (context.request.url.toString().includes("/sign-out")) {
              await logOutFunction();
              return;
            }

            if (context.request.url.toString().includes("/delete-user")) {
              await logOutFunction();
              return;
            }
          },
        },
      },
    ],
  } satisfies BetterAuthClientPlugin;
};
