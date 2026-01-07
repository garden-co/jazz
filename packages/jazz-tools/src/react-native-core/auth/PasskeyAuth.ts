import { CryptoProvider, RawAccountID, cojsonInternals } from "cojson";
import {
  Account,
  AuthSecretStorage,
  AuthenticateAccountFunction,
  ID,
} from "jazz-tools";
import {
  base64UrlToUint8Array,
  uint8ArrayToBase64Url,
} from "./passkey-utils.js";

// Types for react-native-passkey library
// We define these here to avoid requiring the library as a direct dependency
interface PasskeyCreateRequest {
  challenge: string;
  rp: {
    id: string;
    name: string;
  };
  user: {
    id: string;
    name: string;
    displayName: string;
  };
  pubKeyCredParams: Array<{ alg: number; type: "public-key" }>;
  authenticatorSelection?: {
    authenticatorAttachment?: "platform" | "cross-platform";
    requireResidentKey?: boolean;
    residentKey?: "discouraged" | "preferred" | "required";
    userVerification?: "discouraged" | "preferred" | "required";
  };
  timeout?: number;
  attestation?: "none" | "indirect" | "direct" | "enterprise";
}

interface PasskeyGetRequest {
  challenge: string;
  rpId: string;
  allowCredentials?: Array<{
    id: string;
    type: "public-key";
    transports?: Array<"usb" | "nfc" | "ble" | "internal" | "hybrid">;
  }>;
  timeout?: number;
  userVerification?: "discouraged" | "preferred" | "required";
}

interface PasskeyGetResult {
  id: string;
  rawId: string;
  type: "public-key";
  response: {
    clientDataJSON: string;
    authenticatorData: string;
    signature: string;
    userHandle: string;
  };
}

/**
 * Interface for the react-native-passkey module.
 * @internal
 */
export interface PasskeyModule {
  create: (request: PasskeyCreateRequest) => Promise<unknown>;
  get: (request: PasskeyGetRequest) => Promise<PasskeyGetResult>;
  isSupported: () => Promise<boolean>;
}

let cachedPasskeyModule: PasskeyModule | null = null;

/**
 * Lazily loads the react-native-passkey module.
 * This allows the module to be an optional peer dependency.
 * @internal
 */
export function getPasskeyModule(): PasskeyModule {
  if (cachedPasskeyModule) {
    return cachedPasskeyModule;
  }

  try {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const module = require("react-native-passkey");
    const passkeyModule: PasskeyModule =
      module.Passkey || module.default || module;
    cachedPasskeyModule = passkeyModule;
    return passkeyModule;
  } catch {
    throw new Error(
      "react-native-passkey is not installed. Please install it to use passkey authentication: npm install react-native-passkey",
    );
  }
}

/**
 * Sets a custom passkey module (for testing purposes).
 * @internal
 */
export function setPasskeyModule(module: PasskeyModule | null): void {
  cachedPasskeyModule = module;
}

/**
 * Check if passkeys are supported on the current device.
 * Returns false if the react-native-passkey module is not available or if the device doesn't support passkeys.
 */
export async function isPasskeySupported(): Promise<boolean> {
  try {
    const module = getPasskeyModule();
    return await module.isSupported();
  } catch {
    return false;
  }
}

/**
 * `ReactNativePasskeyAuth` provides passkey (WebAuthn) authentication for React Native apps.
 *
 * This class uses the device's biometric authentication (FaceID/TouchID/fingerprint) to
 * securely store and retrieve Jazz account credentials.
 *
 * **Requirements:**
 * - Install `react-native-passkey` as a peer dependency
 * - Configure your app's associated domains (iOS) and asset links (Android)
 * - Passkeys require HTTPS domain verification
 *
 * ```ts
 * import { ReactNativePasskeyAuth } from "jazz-tools/react-native-core";
 *
 * const auth = new ReactNativePasskeyAuth(
 *   crypto,
 *   authenticate,
 *   authSecretStorage,
 *   "My App",
 *   "myapp.com"
 * );
 * ```
 *
 * @category Auth Providers
 */
export class ReactNativePasskeyAuth {
  constructor(
    protected crypto: CryptoProvider,
    protected authenticate: AuthenticateAccountFunction,
    protected authSecretStorage: AuthSecretStorage,
    public appName: string,
    public rpId: string,
  ) {}

  static readonly id = "passkey";

  /**
   * Log in using an existing passkey.
   * This will prompt the user to authenticate with their device biometrics.
   */
  logIn = async () => {
    const { crypto, authenticate } = this;

    const webAuthNCredential = await this.getPasskeyCredentials();

    if (!webAuthNCredential) {
      return;
    }

    const webAuthNCredentialPayload = base64UrlToUint8Array(
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

  /**
   * Register a new passkey for the current account.
   * This will create a passkey that stores the account credentials securely on the device.
   *
   * @param username - The display name for the passkey
   */
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

    const currentAccount = await Account.getMe().$jazz.ensureLoaded({
      resolve: {
        profile: true,
      },
    });

    if (username.trim().length !== 0) {
      currentAccount.profile.$jazz.set("name", username);
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

    const challenge = uint8ArrayToBase64Url(
      new Uint8Array(this.crypto.randomBytes(32)),
    );
    const userId = uint8ArrayToBase64Url(webAuthNCredentialPayload);

    const passkey = getPasskeyModule();

    try {
      await passkey.create({
        challenge,
        rp: {
          id: this.rpId,
          name: this.appName,
        },
        user: {
          id: userId,
          name: `${username} (${new Date().toLocaleString()})`,
          displayName: username,
        },
        pubKeyCredParams: [
          { alg: -7, type: "public-key" }, // ES256
          { alg: -257, type: "public-key" }, // RS256
        ],
        authenticatorSelection: {
          residentKey: "required",
          userVerification: "preferred",
        },
        timeout: 60000,
        attestation: "none",
      });
    } catch (error) {
      throw new Error("Passkey creation aborted", { cause: error });
    }
  }

  private async getPasskeyCredentials(): Promise<PasskeyGetResult | null> {
    const challenge = uint8ArrayToBase64Url(
      new Uint8Array(this.crypto.randomBytes(32)),
    );

    const passkey = getPasskeyModule();

    try {
      const result = await passkey.get({
        challenge,
        rpId: this.rpId,
        timeout: 60000,
        userVerification: "preferred",
      });

      return result;
    } catch (error) {
      throw new Error("Passkey authentication aborted", { cause: error });
    }
  }
}
