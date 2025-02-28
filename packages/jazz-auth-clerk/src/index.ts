import {
  Account,
  AuthCredentials,
  AuthSecretStorage,
  AuthenticateAccountFunction,
} from "jazz-tools";
import { getClerkUsername } from "./getClerkUsername.js";
import {
  ClerkCredentials,
  MinimalClerkClient,
  isClerkCredentials,
} from "./types.js";

export type { MinimalClerkClient };
export { isClerkCredentials };

export class JazzClerkAuth {
  constructor(
    private authenticate: AuthenticateAccountFunction,
    private authSecretStorage: AuthSecretStorage,
  ) {}

  /**
   * Loads the Jazz auth data from the Clerk user and sets it in the auth secret storage.
   */
  static loadClerkAuthData(
    credentials: ClerkCredentials,
    storage: AuthSecretStorage,
  ) {
    return storage.set({
      accountID: credentials.jazzAccountID,
      accountSecret: credentials.jazzAccountSecret,
      secretSeed: credentials.jazzAccountSeed
        ? Uint8Array.from(credentials.jazzAccountSeed)
        : undefined,
      provider: "clerk",
    });
  }

  onClerkUserChange = async (clerkClient: Pick<MinimalClerkClient, "user">) => {
    if (!clerkClient.user) {
      return;
    }

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

    const clerkCredentials = clerkClient.user.unsafeMetadata;
    if (!isClerkCredentials(clerkCredentials)) {
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

    await JazzClerkAuth.loadClerkAuthData(
      {
        jazzAccountID: credentials.accountID,
        jazzAccountSecret: credentials.accountSecret,
        jazzAccountSeed: clerkCredentials.jazzAccountSeed,
      },
      this.authSecretStorage,
    );
  };

  signIn = async (clerkClient: Pick<MinimalClerkClient, "user">) => {
    const credentials = await this.authSecretStorage.get();

    if (!credentials) {
      throw new Error("No credentials found");
    }

    const jazzAccountSeed = credentials.secretSeed
      ? Array.from(credentials.secretSeed)
      : undefined;

    await clerkClient.user?.update({
      unsafeMetadata: {
        jazzAccountID: credentials.accountID,
        jazzAccountSecret: credentials.accountSecret,
        jazzAccountSeed,
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

    await JazzClerkAuth.loadClerkAuthData(
      {
        jazzAccountID: credentials.accountID,
        jazzAccountSecret: credentials.accountSecret,
        jazzAccountSeed,
      },
      this.authSecretStorage,
    );
  };
}

// eslint-disable-next-line @typescript-eslint/no-namespace
export namespace BrowserClerkAuth {
  export interface Driver {
    onError: (error: string | Error) => void;
  }
}
