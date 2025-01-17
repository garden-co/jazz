import { AgentSecret, CryptoProvider } from "cojson";
import { Account, AuthMethod, AuthResult, ID } from "jazz-tools";
import { AuthSecretStorage } from "./AuthSecretStorage.js";

type StorageData = {
  accountID: ID<Account>;
  accountSecret: AgentSecret;
};

/**
 * `BrowserDemoAuth` provides a `JazzAuth` object for demo authentication.
 *
 * Demo authentication is useful for quickly testing your app, as it allows you to create new accounts and log in as existing ones. The authentication persists across page reloads, with the credentials stored in `localStorage`.
 *
 * ```
 * import { BrowserDemoAuth } from "jazz-browser";
 *
 * const auth = new BrowserDemoAuth(driver);
 * ```
 *
 * @category Auth Providers
 */
export class BrowserDemoAuth implements AuthMethod {
  constructor(
    public driver: BrowserDemoAuth.Driver,
    seedAccounts?: {
      [name: string]: {
        accountID: ID<Account>;
        accountSecret: AgentSecret;
      };
    },
  ) {
    for (const [name, credentials] of Object.entries(seedAccounts || {})) {
      const storageData = JSON.stringify(credentials satisfies StorageData);
      if (
        !(
          localStorage["demo-auth-existing-users"]?.split(",") as
            | string[]
            | undefined
        )?.includes(name)
      ) {
        localStorage["demo-auth-existing-users"] = localStorage[
          "demo-auth-existing-users"
        ]
          ? localStorage["demo-auth-existing-users"] + "," + name
          : name;
      }
      localStorage["demo-auth-existing-users-" + name] = storageData;
    }
  }

  /**
   * @returns A `JazzAuth` object
   */
  async start(crypto: CryptoProvider) {
    AuthSecretStorage.migrate();

    const credentials = AuthSecretStorage.get();

    if (credentials) {
      const accountID = credentials.accountID;
      const secret = credentials.accountSecret;

      return {
        type: "existing",
        credentials: { accountID, secret },
        onSuccess: () => {
          this.driver.onSignedIn({ logOut, isSignUp: false });
        },
        onError: (error: string | Error) => {
          this.driver.onError(error);
        },
        logOut: () => {
          AuthSecretStorage.clear();
        },
      } satisfies AuthResult;
    } else {
      return new Promise<AuthResult>((resolve) => {
        this.driver.onReady({
          signUp: async (username) => {
            const secretSeed = crypto.newRandomSecretSeed();
            const accountSecret = crypto.agentSecretFromSecretSeed(secretSeed);

            resolve({
              type: "new",
              creationProps: { name: username },
              initialSecret: accountSecret,
              saveCredentials: async (credentials: {
                accountID: ID<Account>;
                secret: AgentSecret;
              }) => {
                const storageData = JSON.stringify({
                  accountID: credentials.accountID,
                  accountSecret: credentials.secret,
                } satisfies StorageData);

                AuthSecretStorage.set({
                  accountID: credentials.accountID,
                  secretSeed,
                  accountSecret,
                });

                localStorage["demo-auth-existing-users-" + username] =
                  storageData;

                localStorage["demo-auth-existing-users"] = localStorage[
                  "demo-auth-existing-users"
                ]
                  ? localStorage["demo-auth-existing-users"] + "," + username
                  : username;
              },
              onSuccess: () => {
                this.driver.onSignedIn({ logOut, isSignUp: true });
              },
              onError: (error: string | Error) => {
                this.driver.onError(error);
              },
              logOut: () => {
                AuthSecretStorage.clear();
              },
            });
          },
          existingUsers:
            localStorage["demo-auth-existing-users"]?.split(",") ?? [],
          logInAs: async (existingUser) => {
            const storageData = JSON.parse(
              localStorage["demo-auth-existing-users-" + existingUser],
            ) as StorageData;

            AuthSecretStorage.set({
              accountID: storageData.accountID,
              accountSecret: storageData.accountSecret,
            });

            resolve({
              type: "existing",
              credentials: {
                accountID: storageData.accountID,
                secret: storageData.accountSecret,
              },
              onSuccess: () => {
                this.driver.onSignedIn({ logOut, isSignUp: false });
              },
              onError: (error: string | Error) => {
                this.driver.onError(error);
              },
              logOut: () => {
                AuthSecretStorage.clear();
              },
            });
          },
        });
      });
    }
  }
}

/** @internal */
// eslint-disable-next-line @typescript-eslint/no-namespace
export namespace BrowserDemoAuth {
  export interface Driver {
    onReady: (next: {
      signUp: (username: string) => Promise<void>;
      existingUsers: string[];
      logInAs: (existingUser: string) => Promise<void>;
    }) => void;
    onSignedIn: (next: {
      logOut: () => void;
      isSignUp: boolean;
    }) => void;
    onError: (error: string | Error) => void;
  }
}

function logOut() {
  AuthSecretStorage.clear();
}
