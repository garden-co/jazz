import { AgentSecret } from "cojson";
import { AuthSecretStorage } from "jazz-tools";
import {
  Account,
  AuthCredentials,
  AuthenticateAccountFunction,
  ID,
} from "jazz-tools";
import { getClerkUsername } from "./getClerkUsername.js";
import { MinimalClerkClient } from "./types.js";

type ClerkCredentials = {
  jazzAccountID: ID<Account>;
  jazzAccountSecret: AgentSecret;
  jazzAccountSeed?: number[];
};

export type { MinimalClerkClient };

export class JazzClerkAuth {
  constructor(
    private authenticate: AuthenticateAccountFunction,
    private authSecretStorage: AuthSecretStorage,
  ) {}

  onClerkUserChange = async (clerkClient: Pick<MinimalClerkClient, "user">) => {
    if (!clerkClient.user) return;

    const isAuthenticated = this.authSecretStorage.isAuthenticated;

    if (isAuthenticated) return;

    const clerkCredentials = clerkClient.user
      .unsafeMetadata as ClerkCredentials;

    if (!clerkCredentials.jazzAccountID) {
      await this.signIn(clerkClient);
    } else {
      await this.logIn(clerkClient);
    }
  };

  logIn = async (clerkClient: Pick<MinimalClerkClient, "user">) => {
    if (!clerkClient.user) {
      throw new Error("Not signed in on Clerk");
    }

    const clerkCredentials = clerkClient.user
      .unsafeMetadata as ClerkCredentials;

    if (
      !clerkCredentials.jazzAccountID ||
      !clerkCredentials.jazzAccountSecret
    ) {
      throw new Error("No credentials found on Clerk");
    }

    const credentials = {
      accountID: clerkCredentials.jazzAccountID,
      accountSecret: clerkCredentials.jazzAccountSecret,
      secretSeed: clerkCredentials.jazzAccountSeed
        ? Uint8Array.from(clerkCredentials.jazzAccountSeed)
        : undefined,
      provider: "clerk",
    } satisfies AuthCredentials;

    await this.authenticate(credentials);

    await this.authSecretStorage.set(credentials);
  };

  signIn = async (clerkClient: Pick<MinimalClerkClient, "user">) => {
    const credentials = await this.authSecretStorage.get();

    if (!credentials) {
      throw new Error("No credentials found");
    }

    await clerkClient.user?.update({
      unsafeMetadata: {
        jazzAccountID: credentials.accountID,
        jazzAccountSecret: credentials.accountSecret,
        jazzAccountSeed: credentials.secretSeed
          ? Array.from(credentials.secretSeed)
          : undefined,
      } satisfies ClerkCredentials,
    });

    const currentAccount = await Account.getMe().ensureLoaded({
      resolve: {
        profile: true,
      },
    });

    const username = getClerkUsername(clerkClient);

    if (username) {
      currentAccount.profile.name = username;
    }

    await this.authSecretStorage.set({
      accountID: credentials.accountID,
      accountSecret: credentials.accountSecret,
      secretSeed: credentials.secretSeed,
      provider: "clerk",
    });
  };
}

// eslint-disable-next-line @typescript-eslint/no-namespace
export namespace BrowserClerkAuth {
  export interface Driver {
    onError: (error: string | Error) => void;
  }
}
