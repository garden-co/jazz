import { ClientOptions } from "better-auth";
import { createAuthClient } from "better-auth/client";
import { jazzClientPlugin } from "jazz-betterauth-client-plugin";
import {
  Account,
  type AuthCredentials,
  type AuthSecretStorage,
  type AuthenticateAccountFunction,
} from "jazz-tools";
import type { AuthSetPayload } from "jazz-tools/dist/auth/AuthSecretStorage.js";

export const newAuthClient = (options?: ClientOptions) =>
  createAuthClient({
    ...options,
    plugins: [...(options?.plugins ?? []), ...[jazzClientPlugin()]],
  });

export class BetterAuth<T extends ReturnType<typeof newAuthClient>> {
  constructor(
    private authenticate: AuthenticateAccountFunction,
    private authSecretStorage: AuthSecretStorage,
    public authClient: T,
  ) {}

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
   * Called when the authentication session changes.
   * @param session The authentication session.
   */
  onUserChange = async (
    session?: (typeof this.authClient)["$Infer"]["Session"],
  ) => {
    if (!session || !session.user) return;
    const isAuthenticated = this.authSecretStorage.isAuthenticated;
    if (!isAuthenticated) return;
  };

  /**
   * After first authentication.\
   * Retrieves decrypted Jazz credentials from the authentication server.
   */
  logIn = async () => {
    const session = await this.authClient.getSession();
    if (!session) throw new Error("Not authenticated");
    const credentials = (await this.authClient.jazzPlugin.decryptCredentials())
      .data as AuthCredentials;
    await this.authenticate(credentials);
    await BetterAuth.loadAuthData(this.authSecretStorage, credentials);
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
