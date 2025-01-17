import * as bip39 from "@scure/bip39";
import { CryptoProvider, cojsonInternals } from "cojson";
import { Account, AuthMethod, AuthResult, ID } from "jazz-tools";
import { AuthSecretStorage } from "./AuthSecretStorage.js";

/**
 * `BrowserPassphraseAuth` provides a `JazzAuth` object for passphrase authentication.
 *
 * ```ts
 * import { BrowserPassphraseAuth } from "jazz-browser";
 *
 * const auth = new BrowserPassphraseAuth(driver, wordlist);
 * ```
 *
 * @category Auth Providers
 */
export class BrowserPassphraseAuth implements AuthMethod {
  constructor(
    public driver: BrowserPassphraseAuth.Driver,
    public wordlist: string[],
  ) {}

  /**
   * @returns A `JazzAuth` object
   */
  async start(crypto: CryptoProvider): Promise<AuthResult> {
    AuthSecretStorage.migrate();

    const credentials = AuthSecretStorage.get();

    if (credentials && !credentials.isAnonymous) {
      const accountID = credentials.accountID;
      const secret = credentials.accountSecret;

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
          AuthSecretStorage.clear();
        },
      } satisfies AuthResult;
    } else {
      return new Promise<AuthResult>((resolve) => {
        this.driver.onReady({
          signUp: async (username, passphrase) => {
            if (credentials?.isAnonymous) {
              console.warn(
                "Anonymous user upgrade is currently not supported on passphrase auth",
              );
            }

            const secretSeed = bip39.mnemonicToEntropy(
              passphrase,
              this.wordlist,
            );
            const accountSecret = crypto.agentSecretFromSecretSeed(secretSeed);
            if (!accountSecret) {
              this.driver.onError("Invalid passphrase");
              return;
            }

            resolve({
              type: "new",
              creationProps: { name: username },
              initialSecret: accountSecret,
              saveCredentials: async (credentials) => {
                AuthSecretStorage.set({
                  accountID: credentials.accountID,
                  secretSeed,
                  accountSecret,
                });
              },
              onSuccess: () => {
                this.driver.onSignedIn({ logOut });
              },
              onError: (error: string | Error) => {
                this.driver.onError(error);
              },
              logOut: () => {
                AuthSecretStorage.clear();
              },
            });
          },
          logIn: async (passphrase: string) => {
            const secretSeed = bip39.mnemonicToEntropy(
              passphrase,
              this.wordlist,
            );
            const accountSecret = crypto.agentSecretFromSecretSeed(secretSeed);

            if (!accountSecret) {
              this.driver.onError("Invalid passphrase");
              return;
            }

            const accountID = cojsonInternals.idforHeader(
              cojsonInternals.accountHeaderForInitialAgentSecret(
                accountSecret,
                crypto,
              ),
              crypto,
            ) as ID<Account>;

            resolve({
              type: "existing",
              credentials: { accountID, secret: accountSecret },
              saveCredentials: async ({ accountID }) => {
                AuthSecretStorage.set({
                  accountID,
                  secretSeed,
                  accountSecret,
                });
              },
              onSuccess: () => {
                this.driver.onSignedIn({ logOut });
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
export namespace BrowserPassphraseAuth {
  export interface Driver {
    onReady: (next: {
      signUp: (username: string, passphrase: string) => Promise<void>;
      logIn: (passphrase: string) => Promise<void>;
    }) => void;
    onSignedIn: (next: { logOut: () => void }) => void;
    onError: (error: string | Error) => void;
  }
}

function logOut() {
  AuthSecretStorage.clear();
}
