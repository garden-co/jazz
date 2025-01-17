import { AgentSecret, CryptoProvider } from "cojson";
import { AuthSecretStorage } from "jazz-browser";
import { Account, AuthMethod, AuthResult, Credentials, ID } from "jazz-tools";

export type MinimalClerkClient = {
  user:
    | {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        unsafeMetadata: Record<string, any>;
        fullName: string | null;
        username: string | null;
        id: string;
        update: (args: {
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          unsafeMetadata: Record<string, any>;
        }) => Promise<unknown>;
      }
    | null
    | undefined;
  signOut: () => Promise<void>;
};

type ClerkCredentials = {
  jazzAccountID: ID<Account>;
  jazzAccountSecret: AgentSecret;
  jazzAccountSeed?: number[];
};

export class BrowserClerkAuth implements AuthMethod {
  constructor(
    public driver: BrowserClerkAuth.Driver,
    private readonly clerkClient: MinimalClerkClient,
  ) {}

  async start(crypto: CryptoProvider): Promise<AuthResult> {
    AuthSecretStorage.migrate();

    // Check local storage for credentials
    const credentials = AuthSecretStorage.get();

    if (credentials && !credentials.isAnonymous) {
      try {
        return {
          type: "existing",
          credentials: {
            accountID: credentials.accountID,
            secret: credentials.accountSecret,
          },
          saveCredentials: async () => {}, // No need to save credentials when recovering from local storage
          onSuccess: () => {},
          onError: (error: string | Error) => {
            this.driver.onError(error);
          },
          logOut: () => {
            AuthSecretStorage.clear();
            void this.clerkClient.signOut();
          },
        };
      } catch (e) {
        console.error("Error parsing local storage credentials", e);
      }
    }

    if (this.clerkClient.user) {
      const username =
        this.clerkClient.user.fullName ||
        this.clerkClient.user.username ||
        this.clerkClient.user.id;
      // Check clerk user metadata for credentials
      const clerkCredentials = this.clerkClient.user
        .unsafeMetadata as ClerkCredentials;
      if (clerkCredentials.jazzAccountID) {
        if (!clerkCredentials.jazzAccountSecret) {
          this.driver.onError("No secret for existing user");
          throw new Error("No secret for existing user");
        }
        return {
          type: "existing",
          credentials: {
            accountID: clerkCredentials.jazzAccountID as ID<Account>,
            secret: clerkCredentials.jazzAccountSecret as AgentSecret,
          },
          saveCredentials: async ({ accountID, secret }: Credentials) => {
            AuthSecretStorage.set({
              accountID,
              accountSecret: secret,
              secretSeed: clerkCredentials.jazzAccountSeed
                ? Uint8Array.from(clerkCredentials.jazzAccountSeed)
                : undefined,
            });
          },
          onSuccess: () => {},
          onError: (error: string | Error) => {
            this.driver.onError(error);
          },
          logOut: () => {
            void this.clerkClient.signOut();
          },
        };
      } else if (credentials?.isAnonymous) {
        return {
          type: "existing",
          username,
          credentials: {
            accountID: credentials.accountID,
            secret: credentials.accountSecret,
          },
          saveCredentials: async ({ accountID, secret }: Credentials) => {
            AuthSecretStorage.set({
              accountID,
              accountSecret: secret,
              secretSeed: credentials.secretSeed,
            });
            await this.clerkClient.user?.update({
              unsafeMetadata: {
                jazzAccountID: accountID,
                jazzAccountSecret: secret,
                jazzAccountSeed: Array.from(credentials.secretSeed),
              } satisfies ClerkCredentials,
            });
          },
          onSuccess: () => {},
          onError: (error: string | Error) => {
            this.driver.onError(error);
          },
          logOut: () => {
            void this.clerkClient.signOut();
          },
        };
      } else {
        const secretSeed = crypto.newRandomSecretSeed();
        // No credentials found, so we need to create new credentials
        return {
          type: "new",
          creationProps: {
            name: username,
          },
          initialSecret: crypto.agentSecretFromSecretSeed(secretSeed),
          saveCredentials: async ({ accountID, secret }: Credentials) => {
            AuthSecretStorage.set({
              accountID,
              secretSeed,
              accountSecret: secret,
            });
            await this.clerkClient.user?.update({
              unsafeMetadata: {
                jazzAccountID: accountID,
                jazzAccountSecret: secret,
                jazzAccountSeed: Array.from(secretSeed),
              } satisfies ClerkCredentials,
            });
          },
          onSuccess: () => {},
          onError: (error: string | Error) => {
            this.driver.onError(error);
          },
          logOut: () => {
            void this.clerkClient.signOut();
          },
        };
      }
    } else {
      // Clerk user not found, so we can't authenticate
      throw new Error("Not signed in");
    }
  }
}

// eslint-disable-next-line @typescript-eslint/no-namespace
export namespace BrowserClerkAuth {
  export interface Driver {
    onError: (error: string | Error) => void;
  }
}
