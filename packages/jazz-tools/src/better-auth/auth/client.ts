import type { BetterAuthClientPlugin } from "better-auth";
import type {
  Account,
  AuthSecretStorage,
  AuthSetPayload,
  JazzContextType,
} from "jazz-tools";
import type { jazzPlugin } from "./server.js";

const SIGNUP_URLS = [
  "/sign-up",
  "/sign-in/social",
  "/sign-in/oauth2",
  "/email-otp/send-verification-otp",
];

/**
 * @example
 * ```ts
 * const auth = betterAuth({
 *   plugins: [jazzPluginClient()],
 * });
 * ```
 */
export const jazzPluginClient = () => {
  let jazzContext: JazzContextType<Account>;
  let authSecretStorage: AuthSecretStorage;
  let signOutUnsubscription: () => void;

  const authenticateOnJazz = async (jazzAuth: AuthSetPayload) => {
    const parsedJazzAuth = {
      ...jazzAuth,
      secretSeed: jazzAuth.secretSeed
        ? Uint8Array.from(jazzAuth.secretSeed)
        : undefined,
    };

    await jazzContext.authenticate(parsedJazzAuth);
    await authSecretStorage.set(parsedJazzAuth);
  };

  return {
    id: "jazz-plugin",
    $InferServerPlugin: {} as ReturnType<typeof jazzPlugin>,
    getActions: ($fetch, $store) => {
      return {
        jazz: {
          setJazzContext: (context: JazzContextType<Account>) => {
            jazzContext = context;
          },
          setAuthSecretStorage: (storage: AuthSecretStorage) => {
            authSecretStorage = storage;
            if (signOutUnsubscription) signOutUnsubscription();

            // This is a workaround to logout from Better Auth when user logs out directly from Jazz
            signOutUnsubscription = authSecretStorage.onUpdate(
              (isAuthenticated) => {
                if (isAuthenticated === false) {
                  const session = $store.atoms.session?.get();
                  if (!session) return;

                  // if the user logs out from Better Auth, the get session is immediately called
                  // so we must wait the next fetched session to understand if we need to call sign-out
                  if (session.isPending || session.isRefetching) {
                    // listen once for next session's data
                    const unsub = $store.atoms.session?.listen((session) => {
                      unsub?.();
                      // if the session is null, user has been already logged out from Better Auth
                      if (session.data !== null) {
                        $fetch("/sign-out", { method: "POST" });
                      }
                    });
                  }
                  // if the session is not pending, it means user logged out from Jazz only
                  // so we call the sign-out api
                  else {
                    $fetch("/sign-out", { method: "POST" });
                  }
                }
              },
            );
          },
        },
      };
    },
    fetchPlugins: [
      {
        id: "jazz-plugin",
        name: "jazz-plugin",
        hooks: {
          async onRequest(context) {
            if (
              SIGNUP_URLS.some((url) => context.url.toString().includes(url))
            ) {
              const credentials = await authSecretStorage.get();

              if (!credentials) {
                throw new Error("Jazz credentials not found");
              }

              context.headers.set(
                "x-jazz-auth",
                JSON.stringify({
                  accountID: credentials.accountID,
                  secretSeed: credentials.secretSeed,
                  accountSecret: credentials.accountSecret,
                }),
              );
            }
          },
          async onSuccess(context) {
            if (context.request.url.toString().includes("/sign-up")) {
              await authenticateOnJazz(context.data.jazzAuth);
              return;
            }

            if (context.request.url.toString().includes("/sign-in/email")) {
              await authenticateOnJazz(context.data.jazzAuth);
              return;
            }

            if (context.request.url.toString().includes("/get-session")) {
              if (context.data === null) {
                if (authSecretStorage.isAuthenticated === true) {
                  console.info(
                    "Jazz is authenticated, but the session is null. Logging out",
                  );
                  await jazzContext.logOut();
                }
                return;
              }

              if (!context.data?.user) {
                return;
              }

              if (authSecretStorage.isAuthenticated === false) {
                console.info(
                  "Jazz is not authenticated, using Better Auth stored credentials",
                );
                await authenticateOnJazz(context.data.jazzAuth);
                return;
              }

              const sessionAccountID = context.data.user.accountID;

              const credentials = await authSecretStorage.get();

              if (!credentials) {
                throw new Error("Jazz credentials not found");
              }

              if (credentials.accountID !== sessionAccountID) {
                console.info(
                  "Jazz credentials mismatch, using Better Auth stored credentials",
                );
                await authenticateOnJazz(context.data.jazzAuth);
              }
              return;
            }

            if (context.request.url.toString().includes("/sign-out")) {
              await jazzContext.logOut();
              return;
            }

            if (context.request.url.toString().includes("/delete-user")) {
              await jazzContext.logOut();
              return;
            }
          },
        },
      },
    ],
  } satisfies BetterAuthClientPlugin;
};
