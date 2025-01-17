import { CryptoProvider, RawAccountID, cojsonInternals } from "cojson";
import { Account, AuthMethod, AuthResult, ID } from "jazz-tools";
import { AuthSecretStorage } from "./AuthSecretStorage.js";

/**
 * `BrowserPasskeyAuth` provides a `JazzAuth` object for passkey authentication.
 *
 * ```ts
 * import { BrowserPasskeyAuth } from "jazz-browser";
 *
 * const auth = new BrowserPasskeyAuth(driver, appName);
 * ```
 *
 * @category Auth Providers
 */
export class BrowserPasskeyAuth implements AuthMethod {
  constructor(
    public driver: BrowserPasskeyAuth.Driver,
    public appName: string,
    // TODO: is this a safe default?
    public appHostname: string = window.location.hostname,
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
          signUp: async (username) => {
            if (credentials?.isAnonymous) {
              resolve({
                type: "existing",
                username,
                credentials: {
                  accountID: credentials.accountID,
                  secret: credentials.accountSecret,
                },
                saveCredentials: async ({ accountID, secret }) => {
                  await this.createPasskeyCredentials({
                    accountID,
                    secretSeed: credentials.secretSeed,
                    username,
                  });

                  AuthSecretStorage.set({
                    accountID,
                    secretSeed: credentials.secretSeed,
                    accountSecret: secret,
                  });
                },
                onSuccess: () => {
                  this.driver.onSignedIn({ logOut });
                },
                onError: (error: string | Error) => {
                  this.driver.onError(error);
                },
                logOut,
              });
              return;
            } else {
              const secretSeed = crypto.newRandomSecretSeed();

              resolve({
                type: "new",
                creationProps: { name: username },
                initialSecret: crypto.agentSecretFromSecretSeed(secretSeed),
                saveCredentials: async ({ accountID, secret }) => {
                  await this.createPasskeyCredentials({
                    accountID,
                    secretSeed,
                    username,
                  });

                  AuthSecretStorage.set({
                    accountID,
                    secretSeed,
                    accountSecret: secret,
                  });
                },
                onSuccess: () => {
                  this.driver.onSignedIn({ logOut });
                },
                onError: (error: string | Error) => {
                  this.driver.onError(error);
                },
                logOut,
              });
            }
          },
          logIn: async () => {
            const webAuthNCredential = await this.getPasskeyCredentials().catch(
              () => {
                this.driver.onError(
                  "Error while accessing the passkey credentials",
                );
                return "rejected" as const;
              },
            );

            if (webAuthNCredential === "rejected") {
              return;
            }

            if (!webAuthNCredential) {
              this.driver.onError(
                "Error while accessing the passkey credentials",
              );
              return;
            }

            const webAuthNCredentialPayload = new Uint8Array(
              webAuthNCredential.response.userHandle,
            );
            const accountSecretSeed = webAuthNCredentialPayload.slice(
              0,
              cojsonInternals.secretSeedLength,
            );

            const secret = crypto.agentSecretFromSecretSeed(accountSecretSeed);

            const accountID = cojsonInternals.rawCoIDfromBytes(
              webAuthNCredentialPayload.slice(
                cojsonInternals.secretSeedLength,
                cojsonInternals.secretSeedLength +
                  cojsonInternals.shortHashLength,
              ),
            ) as ID<Account>;

            resolve({
              type: "existing",
              credentials: { accountID, secret },
              saveCredentials: async ({ accountID, secret }) => {
                AuthSecretStorage.set({
                  accountID,
                  accountSecret: secret,
                  secretSeed: accountSecretSeed,
                });
              },
              onSuccess: () => {
                this.driver.onSignedIn({ logOut });
              },
              onError: (error: string | Error) => {
                this.driver.onError(error);
              },
              logOut,
            });
          },
        });
      });
    }
  }

  private async createPasskeyCredentials({
    accountID,
    secretSeed,
    username,
  }: {
    accountID: ID<Account>;
    secretSeed: Uint8Array;
    username: string;
  }) {
    const webAuthNCredentialPayload = new Uint8Array(
      cojsonInternals.secretSeedLength + cojsonInternals.shortHashLength,
    );

    webAuthNCredentialPayload.set(secretSeed);
    webAuthNCredentialPayload.set(
      cojsonInternals.rawCoIDtoBytes(accountID as unknown as RawAccountID),
      cojsonInternals.secretSeedLength,
    );

    try {
      await navigator.credentials.create({
        publicKey: {
          challenge: Uint8Array.from([0, 1, 2]),
          rp: {
            name: this.appName,
            id: this.appHostname,
          },
          user: {
            id: webAuthNCredentialPayload,
            name: username + ` (${new Date().toLocaleString()})`,
            displayName: username,
          },
          pubKeyCredParams: [{ alg: -7, type: "public-key" }],
          authenticatorSelection: {
            authenticatorAttachment: "platform",
            requireResidentKey: true,
            residentKey: "required",
          },
          timeout: 60000,
          attestation: "direct",
        },
      });
    } catch (error) {
      if (error instanceof DOMException && error.name === "NotAllowedError") {
        throw new Error("Passkey creation not allowed");
      }

      throw error;
    }
  }

  private async getPasskeyCredentials() {
    const value = await navigator.credentials.get({
      publicKey: {
        challenge: Uint8Array.from([0, 1, 2]),
        rpId: this.appHostname,
        allowCredentials: [],
        timeout: 60000,
      },
    });

    return value as
      | (Credential & { response: { userHandle: ArrayBuffer } })
      | null;
  }
}

/** @internal */
// eslint-disable-next-line @typescript-eslint/no-namespace
export namespace BrowserPasskeyAuth {
  export interface Driver {
    onReady: (next: {
      signUp: (username: string) => Promise<void>;
      logIn: () => Promise<void>;
    }) => void;
    onSignedIn: (next: { logOut: () => void }) => void;
    onError: (error: string | Error) => void;
  }
}

function logOut() {
  AuthSecretStorage.clear();
}
