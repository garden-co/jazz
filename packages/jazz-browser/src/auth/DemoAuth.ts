import { AgentSecret } from "cojson";
import { Account, AuthMethod, AuthResult, ID } from "jazz-tools";

type StorageData = {
  accountID: ID<Account>;
  accountSecret: AgentSecret;
};

const localStorageKey = "jazz-logged-in-secret";

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
  async start() {
    // migrate old localStorage key to new one
    if (localStorage["demo-auth-logged-in-secret"]) {
      if (!localStorage[localStorageKey]) {
        localStorage[localStorageKey] =
          localStorage["demo-auth-logged-in-secret"];
      }
      delete localStorage["demo-auth-logged-in-secret"];
    }

    if (localStorage[localStorageKey]) {
      const localStorageData = JSON.parse(
        localStorage[localStorageKey],
      ) as StorageData;

      const accountID = localStorageData.accountID as ID<Account>;
      const secret = localStorageData.accountSecret;

      return {
        type: "existing",
        credentials: { accountID, secret },
        onSuccess: () => {
          this.driver.onSignedIn({ logOut });
        },
        onError: (error: string | Error) => {
          this.driver.onError(error);
        },
        logOut: () => {
          delete localStorage[localStorageKey];
        },
      } satisfies AuthResult;
    } else {
      return new Promise<AuthResult>((resolve) => {
        this.driver.onReady({
          signUp: async (username) => {
            resolve({
              type: "new",
              creationProps: { name: username },
              saveCredentials: async (credentials: {
                accountID: ID<Account>;
                secret: AgentSecret;
              }) => {
                const storageData = JSON.stringify({
                  accountID: credentials.accountID,
                  accountSecret: credentials.secret,
                } satisfies StorageData);

                localStorage[localStorageKey] = storageData;
                localStorage["demo-auth-existing-users-" + username] =
                  storageData;

                localStorage["demo-auth-existing-users"] = localStorage[
                  "demo-auth-existing-users"
                ]
                  ? localStorage["demo-auth-existing-users"] + "," + username
                  : username;
              },
              onSuccess: () => {
                this.driver.onSignedIn({ logOut });
              },
              onError: (error: string | Error) => {
                this.driver.onError(error);
              },
              logOut: () => {
                delete localStorage[localStorageKey];
              },
            });
          },
          existingUsers:
            localStorage["demo-auth-existing-users"]?.split(",") ?? [],
          logInAs: async (existingUser) => {
            const storageData = JSON.parse(
              localStorage["demo-auth-existing-users-" + existingUser],
            ) as StorageData;

            localStorage[localStorageKey] = JSON.stringify(storageData);

            resolve({
              type: "existing",
              credentials: {
                accountID: storageData.accountID,
                secret: storageData.accountSecret,
              },
              onSuccess: () => {
                this.driver.onSignedIn({ logOut });
              },
              onError: (error: string | Error) => {
                this.driver.onError(error);
              },
              logOut: () => {
                delete localStorage[localStorageKey];
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
    onSignedIn: (next: { logOut: () => void }) => void;
    onError: (error: string | Error) => void;
  }
}

function logOut() {
  delete localStorage[localStorageKey];
}
