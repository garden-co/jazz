import type { ClientOptions } from "better-auth";
import { createAuthClient } from "better-auth/client";
import { jazzClientPlugin } from "jazz-betterauth-client-plugin";
import {
  Account,
  type AuthCredentials,
  type AuthSecretStorage,
  type AuthenticateAccountFunction,
} from "jazz-tools";
import type { AuthSetPayload } from "jazz-tools/dist/auth/AuthSecretStorage.js";

export const newAuthClient = <T extends ClientOptions>(options?: T) => {
  type Plugins = Array<
    | NonNullable<NonNullable<T>["plugins"]>[number]
    | ReturnType<typeof jazzClientPlugin>
  >;
  type Options<T> = {
    plugins: Plugins;
  } & T;
  type AuthClient<T> = ReturnType<typeof createAuthClient<Options<T>>> &
    ReturnType<
      typeof createAuthClient<{
        plugins: [ReturnType<typeof jazzClientPlugin>];
      }>
    >;
  return createAuthClient<Options<T>>({
    ...options,
    plugins: [...(options?.plugins ?? []), ...[jazzClientPlugin()]],
  } as Options<T>) as AuthClient<T>;
};

export type AuthClient<T extends ClientOptions> = ReturnType<
  typeof newAuthClient<T>
>;
export type InferredSession<T extends ClientOptions> =
  AuthClient<T>["$Infer"]["Session"];
export type Session<T extends ClientOptions> =
  AuthClient<T>["$Infer"]["Session"]["session"];
export type User<T extends ClientOptions> =
  AuthClient<T>["$Infer"]["Session"]["user"];

export class BetterAuth<T extends ClientOptions> {
  public authClient: AuthClient<T>;
  constructor(
    private authenticate: AuthenticateAccountFunction,
    private authSecretStorage: AuthSecretStorage,
    options?: T,
  ) {
    this.authClient = newAuthClient(options);
  }

  static async loadAuthData(
    storage: AuthSecretStorage,
    credentials: AuthCredentials,
  ) {
    return storage.set({
      ...credentials,
      provider: "betterauth",
    } satisfies AuthSetPayload);
  }

  /**
   * After first authentication.\
   * Retrieves decrypted Jazz credentials from the authentication server.
   */
  logIn = async () => {
    const session = await this.authClient.getSession();
    if (!session) throw new Error("Not authenticated");
    const credentials: AuthCredentials | null = (
      await this.authClient.jazzPlugin.decryptCredentials()
    ).data;
    if (credentials) {
      await this.authenticate(credentials);
      await BetterAuth.loadAuthData(this.authSecretStorage, credentials);
    }
  };

  /**
   * On first authentication.\
   * Sends Jazz credentials to the authentication server.
   */
  signIn = async () => {
    const session = (await this.authClient.getSession()).data;
    if (!session || !session.user) throw new Error("Not authenticated");
    var credentials = await this.authSecretStorage.get();
    if (!credentials) throw new Error("No credentials found");

    credentials.provider = "betterauth"; // If the provider remains 'anonymous', Jazz will not consider us authenticated later.
    await this.authClient.jazzPlugin.encryptCredentials({
      ...credentials,
      secretSeed: credentials.secretSeed
        ? Array.from(credentials.secretSeed)
        : undefined,
    }); // Sends the credentials to the authentication server.

    const currentAccount = await Account.getMe().ensureLoaded({
      resolve: {
        profile: true,
      },
    });
    currentAccount.profile.name = session.user.name;
    await BetterAuth.loadAuthData(this.authSecretStorage, credentials);
  };
}
