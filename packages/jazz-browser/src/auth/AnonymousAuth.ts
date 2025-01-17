import { AgentSecret, CryptoProvider } from "cojson";
import { Account, AuthMethod, AuthResult, ID } from "jazz-tools";
import { AuthSecretStorage } from "./AuthSecretStorage.js";

/**
 * `BrowserAnonymousAuth` provides a `JazzAuth` object for demo authentication.
 *
 * Demo authentication is useful for quickly testing your app, as it allows you to create new accounts and log in as existing ones. The authentication persists across page reloads, with the credentials stored in `localStorage`.
 *
 * ```
 * import { BrowserAnonymousAuth } from "jazz-browser";
 *
 * const auth = new BrowserAnonymousAuth(driver);
 * ```
 *
 * @category Auth Providers
 */
export class BrowserAnonymousAuth implements AuthMethod {
  constructor(
    public defaultUserName: string,
    public driver: BrowserAnonymousAuth.Driver,
  ) {}

  /**
   * @returns A `JazzAuth` object
   */
  async start(crypto: CryptoProvider) {
    AuthSecretStorage.migrate();

    const existingUser = AuthSecretStorage.get();

    if (existingUser) {
      const accountID = existingUser.accountID;
      const secret = existingUser.accountSecret;

      return {
        type: "existing",
        credentials: { accountID, secret },
        onSuccess: () => {
          this.driver.onSignedIn({ logOut });
        },
        onError: (error: string | Error) => {
          this.driver.onError(error);
        },
        logOut,
      } satisfies AuthResult;
    } else {
      const secretSeed = crypto.newRandomSecretSeed();

      return {
        type: "new",
        creationProps: { name: this.defaultUserName, anonymous: true },
        initialSecret: crypto.agentSecretFromSecretSeed(secretSeed),
        saveCredentials: async (credentials: {
          accountID: ID<Account>;
          secret: AgentSecret;
        }) => {
          AuthSecretStorage.set({
            accountID: credentials.accountID,
            secretSeed,
            accountSecret: credentials.secret,
            isAnonymous: true,
          });
        },
        onSuccess: () => {
          this.driver.onSignedIn({ logOut });
        },
        onError: (error: string | Error) => {
          this.driver.onError(error);
        },
        logOut,
      } satisfies AuthResult;
    }
  }
}

/** @internal */
// eslint-disable-next-line @typescript-eslint/no-namespace
export namespace BrowserAnonymousAuth {
  export interface Driver {
    onSignedIn: (next: { logOut: () => void }) => void;
    onError: (error: string | Error) => void;
  }
}

function logOut() {
  AuthSecretStorage.clear();
}
