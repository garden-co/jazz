import {
  Account,
  AuthCredentials,
  AuthSecretStorage,
  AuthenticateAccountFunction,
} from "jazz-tools";
import { getClerkUsername } from "./getClerkUsername.js";
import {
  ClerkCredentials,
  ClerkEventSchema,
  ClerkUser,
  MinimalClerkClient,
  isClerkAuthStateEqual,
  isClerkCredentials,
} from "./types.js";

export type { MinimalClerkClient };
export { isClerkCredentials };

export class JazzClerkAuth {
  constructor(
    private authenticate: AuthenticateAccountFunction,
    private logOut: () => Promise<void> | void,
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

  static async initializeAuth(clerk: MinimalClerkClient) {
    const secretStorage = new AuthSecretStorage();

    if (!isClerkCredentials(clerk.user?.unsafeMetadata)) {
      return;
    }

    await JazzClerkAuth.loadClerkAuthData(
      clerk.user.unsafeMetadata,
      secretStorage,
    );
  }

  private isFirstCall = true;
  private previousUser: Pick<ClerkUser, "unsafeMetadata"> | null = null;

  registerListener(clerkClient: MinimalClerkClient) {
    this.previousUser = ClerkEventSchema.parse(clerkClient).user ?? null;

    // Need to use addListener because the clerk user object is not updated when the user logs in
    return clerkClient.addListener((event) => {
      const user =
        (ClerkEventSchema.parse(event).user as ClerkUser | null) ?? null;

      if (!isClerkAuthStateEqual(this.previousUser, user) || this.isFirstCall) {
        this.previousUser = user;
        this.onClerkUserChange(user);
        this.isFirstCall = false;
      }
    });
  }

  onClerkUserChange = async (clerkUser: ClerkUser | null | undefined) => {
    const isAuthenticated = this.authSecretStorage.isAuthenticated;

    // LogOut is driven by Clerk. The framework adapters will need to pass `logOutReplacement` to the `JazzProvider`
    // to make the logOut work correctly.
    if (!clerkUser) {
      if (isAuthenticated) {
        this.authSecretStorage.clear();
        await this.logOut();
      }
      return;
    }

    if (isAuthenticated) return;

    if (!clerkUser.unsafeMetadata.jazzAccountID) {
      await this.signIn(clerkUser);
    } else {
      await this.logIn(clerkUser);
    }
  };

  logIn = async (clerkUser: ClerkUser) => {
    const clerkCredentials = clerkUser.unsafeMetadata;
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

  signIn = async (clerkUser: ClerkUser) => {
    const credentials = await this.authSecretStorage.get();

    if (!credentials) {
      throw new Error("No credentials found");
    }

    const jazzAccountSeed = credentials.secretSeed
      ? Array.from(credentials.secretSeed)
      : undefined;

    const clerkCredentials = {
      jazzAccountID: credentials.accountID,
      jazzAccountSecret: credentials.accountSecret,
      jazzAccountSeed,
    };
    // user.update will cause the Clerk user change listener to fire; updating this.previousUser beforehand
    // ensures the listener sees the new credentials and does not trigger an unnecessary logIn operation
    this.previousUser = { unsafeMetadata: clerkCredentials };

    await clerkUser.update({
      unsafeMetadata: clerkCredentials,
    });

    const currentAccount = await Account.getMe().$jazz.ensureLoaded({
      resolve: {
        profile: true,
      },
    });

    const username = getClerkUsername({ user: clerkUser });

    if (username) {
      currentAccount.profile.$jazz.set("name", username);
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
