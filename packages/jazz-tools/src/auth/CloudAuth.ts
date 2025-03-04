import { createAuthClient } from "better-auth/client";
import { inferAdditionalFields } from "better-auth/client/plugins";
import { AgentSecret } from "cojson";
import { Account } from "../coValues/account.js";
import { ID } from "../internal.js";
import { AuthCredentials, AuthenticateAccountFunction } from "../types.js";
import { AuthSecretStorage } from "./AuthSecretStorage.js";

export const authClient = createAuthClient({
  plugins: [
    inferAdditionalFields({
      user: {
        accountID: {
          type: "string",
          required: true,
        },
        secretSeed: {
          type: "number[]",
          required: false,
        },
        accountSecret: {
          type: "string",
          required: true,
        },
        provider: {
          type: "string",
          required: false,
        },
      },
    }),
  ],
});

export type Session = typeof authClient.$Infer.Session;

export class CloudAuth {
  constructor(
    private authenticate: AuthenticateAccountFunction,
    private authSecretStorage: AuthSecretStorage,
  ) {}

  static loadAuthData(
    session: Pick<Session, "user">,
    storage: AuthSecretStorage,
  ) {
    return storage.set({
      accountID: session.user.accountID as ID<Account>,
      accountSecret: session.user.accountSecret as AgentSecret,
      secretSeed: session.user.secretSeed
        ? Uint8Array.from(session.user.secretSeed)
        : undefined,
      provider: session.user.provider ? session.user.provider : "",
    });
  }

  onUserChange = async (session: Pick<Session, "user">) => {
    if (!session.user) return;
    const isAuthenticated = this.authSecretStorage.isAuthenticated;
    if (!isAuthenticated) return;
  };

  logIn = async (session: Pick<Session, "user">) => {
    if (!session.user) throw new Error("Not signed in");
    const credentials = {
      accountID: session.user.accountID as ID<Account>,
      accountSecret: session.user.accountSecret as AgentSecret,
      secretSeed: session.user.secretSeed
        ? Uint8Array.from(session.user.secretSeed)
        : undefined,
      provider: session.user.provider ? session.user.provider : "",
    } satisfies AuthCredentials;
    await this.authenticate(credentials);
    await CloudAuth.loadAuthData(session, this.authSecretStorage);
  };

  signIn = async (session: Pick<Session, "user">) => {
    const credentials = await this.authSecretStorage.get();
    if (!credentials) throw new Error("No credentials found");
    const jazzAccountSeed = credentials.secretSeed
      ? Array.from(credentials.secretSeed)
      : undefined;
    await authClient.updateUser({
      accountID: credentials.accountID,
      accountSecret: credentials.accountSecret,
      secretSeed: jazzAccountSeed,
      provider: credentials.provider,
    });
    const currentAccount = await Account.getMe().ensureLoaded({
      profile: {},
    });
    currentAccount.profile.name = session.user.name;
    await CloudAuth.loadAuthData(session, this.authSecretStorage);
  };
}
