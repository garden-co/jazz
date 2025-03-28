import { createAuthClient } from "better-auth/client";
import { inferAdditionalFields } from "better-auth/client/plugins";
import { jazzClientPlugin } from "jazz-betterauth-client-plugin";
import {
  Account,
  AuthCredentials,
  AuthSecretStorage,
  AuthenticateAccountFunction,
} from "jazz-tools";
import { AuthSetPayload } from "jazz-tools/dist/auth/AuthSecretStorage.js";

export const newAuthClient = (baseUrl: string) =>
  createAuthClient({
    baseURL: baseUrl,
    plugins: [
      jazzClientPlugin(),
      inferAdditionalFields({
        user: {
          encryptedCredentials: {
            type: "string",
            required: false,
          },
          salt: {
            type: "string",
            required: false,
          },
        },
      }),
    ],
  });

export type AuthClient = ReturnType<typeof newAuthClient>;
export type Session = AuthClient["$Infer"]["Session"];

export class CloudAuth {
  constructor(
    private authenticate: AuthenticateAccountFunction,
    private authSecretStorage: AuthSecretStorage,
    private authClient: AuthClient,
  ) {}

  static async loadAuthData(
    storage: AuthSecretStorage,
    credentials: AuthCredentials,
  ) {
    return storage.set({
      ...credentials,
      provider: "cloudauth",
    } satisfies AuthSetPayload);
  }

  /**
   * Called when the authentication session changes.
   * @param session The authentication session.
   */
  onUserChange = async (session: Pick<Session, "user">) => {
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
    await CloudAuth.loadAuthData(this.authSecretStorage, credentials);
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

    credentials.provider = "cloudauth"; // If the provider remains 'anonymous', Jazz will not consider us authenticated later.
    await this.authClient.jazzPlugin.encryptCredentials({
      ...credentials,
      secretSeed: credentials.secretSeed
        ? Array.from(credentials.secretSeed)
        : undefined,
    }); // Sends the credentials to the authentication server.

    const currentAccount = await Account.getMe().ensureLoaded({
      profile: {},
    });
    currentAccount.profile.name = session.user.name;
    await CloudAuth.loadAuthData(this.authSecretStorage, credentials);
  };
}
