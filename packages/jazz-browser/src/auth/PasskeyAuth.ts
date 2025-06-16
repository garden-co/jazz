import { CryptoProvider, RawAccountID, cojsonInternals } from "cojson";
import {
  Account,
  AuthSecretStorage,
  AuthenticateAccountFunction,
  ID,
} from "jazz-tools";

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
export class BrowserPasskeyAuth {
  constructor(
    protected crypto: CryptoProvider,
    protected authenticate: AuthenticateAccountFunction,
    protected authSecretStorage: AuthSecretStorage,
    public appName: string,
    public appHostname: string = window.location.hostname,
  ) {}

  static readonly id = "passkey";

  logIn = async () => {
    const { crypto, authenticate } = this;

    const webAuthNCredential = await this.getPasskeyCredentials();

    if (!webAuthNCredential) {
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
        cojsonInternals.secretSeedLength + cojsonInternals.shortHashLength,
      ),
    ) as ID<Account>;

    await authenticate({
      accountID,
      accountSecret: secret,
    });

    await this.authSecretStorage.set({
      accountID,
      secretSeed: accountSecretSeed,
      accountSecret: secret,
      provider: "passkey",
    });
  };

  signUp = async (username: string) => {
    const credentials = await this.authSecretStorage.get();

    if (!credentials?.secretSeed) {
      throw new Error(
        "Not enough credentials to register the account with passkey",
      );
    }

    await this.createPasskeyCredentials({
      accountID: credentials.accountID,
      secretSeed: credentials.secretSeed,
      username,
    });

    const currentAccount = await Account.getMe().ensureLoaded({
      resolve: {
        profile: true,
      },
    });

    if (username.trim().length !== 0) {
      currentAccount.profile.name = username;
    }

    await this.authSecretStorage.set({
      accountID: credentials.accountID,
      secretSeed: credentials.secretSeed,
      accountSecret: credentials.accountSecret,
      provider: "passkey",
    });
  };

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
          challenge: this.crypto.randomBytes(20),
          rp: {
            name: this.appName,
            id: this.appHostname,
          },
          user: {
            id: webAuthNCredentialPayload,
            name: username + ` (${new Date().toLocaleString()})`,
            displayName: username,
          },
          pubKeyCredParams: [
            { alg: -7, type: "public-key" },
            { alg: -8, type: "public-key" },
            { alg: -37, type: "public-key" },
            { alg: -257, type: "public-key" },
          ],
          authenticatorSelection: {
            requireResidentKey: true,
            residentKey: "required",
            userVerification: "preferred",
          },
          timeout: 60000,
          attestation: "direct",
        },
      });
    } catch (error) {
      throw new Error("Passkey creation aborted", { cause: error });
    }
  }

  private async getPasskeyCredentials() {
    try {
      const value = await navigator.credentials.get({
        publicKey: {
          challenge: Uint8Array.from([0, 1, 2]),
          rpId: this.appHostname,
          allowCredentials: [],
          timeout: 60000,
          userVerification: "preferred",
        },
        mediation: "optional",
      });

      return value as
        | (Credential & { response: { userHandle: ArrayBuffer } })
        | null;
    } catch (error) {
      throw new Error("Passkey creation aborted", { cause: error });
    }
  }
}
