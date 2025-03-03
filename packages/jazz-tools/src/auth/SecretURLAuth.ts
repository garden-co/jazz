import {
  CryptoProvider,
  base64URLtoBytes,
  bytesToBase64url,
  cojsonInternals,
} from "cojson";
import { Account } from "../coValues/account.js";
import { ID } from "../internal.js";
import { AuthenticateAccountFunction } from "../types.js";
import { AuthSecretStorage } from "./AuthSecretStorage.js";

/**
 * `SecretURLAuth` provides a `JazzAuth` object for secret URL authentication. Good for use in a QR code.
 *
 * ```ts
 * import { SecretURLAuth } from "jazz-tools";
 *
 * const auth = new SecretURLAuth(crypto, jazzContext.authenticate, new AuthSecretStorage());
 * ```
 *
 * @category Auth Providers
 */
export class SecretURLAuth {
  constructor(
    private crypto: CryptoProvider,
    private authenticate: AuthenticateAccountFunction,
    private authSecretStorage: AuthSecretStorage,
  ) {}

  secretURL: string = "";

  /**
   * Logs in a user using a secret URL.
   *
   * @param url - The secret URL to log in with.
   */
  logIn = async (url: string) => {
    const parsed = parseAuthURL(url);
    if (!parsed) {
      throw new Error("Invalid authentication URL");
    }

    if (Date.now() > parsed.expiresAt) {
      throw new Error("This link has expired");
    }

    const { crypto, authenticate } = this;

    const secretSeed = this.decodeSecret(parsed.secret);
    const accountSecret = crypto.agentSecretFromSecretSeed(secretSeed);

    const accountID = cojsonInternals.idforHeader(
      cojsonInternals.accountHeaderForInitialAgentSecret(accountSecret, crypto),
      crypto,
    ) as ID<Account>;

    await authenticate({ accountID, accountSecret });

    await this.authSecretStorage.set({
      accountID,
      secretSeed,
      accountSecret,
      provider: "secretURL",
    });
  };

  /**
   * Creates a pairing URL for the given secret.
   *
   * @param expiresAt - The expiration time in milliseconds. Defaults to 15 minutes.
   * @returns The pairing URL.
   */
  createPairingURL = async (expiresAt?: number) => {
    const credentials = await this.authSecretStorage.get();
    if (!credentials?.secretSeed) {
      throw new Error("No existing authentication found");
    }

    const secret = this.encodeSecret(credentials.secretSeed);
    return createAuthURL(secret, expiresAt);
  };

  private encodeSecret = (secret: Uint8Array): string => {
    return bytesToBase64url(secret);
  };

  private decodeSecret = (encoded: string): Uint8Array => {
    return base64URLtoBytes(encoded);
  };

  getCurrentAccountSecretURL = async () => {
    const credentials = await this.authSecretStorage.get();
    if (!credentials?.secretSeed) throw new Error("No active session");
    return createAuthURL(this.encodeSecret(credentials.secretSeed));
  };

  loadCurrentAccountSecretURL = async () => {
    this.secretURL = await this.getCurrentAccountSecretURL();
    this.notify();
  };

  listeners = new Set<() => void>();
  subscribe = (callback: () => void) => {
    this.listeners.add(callback);

    return () => {
      this.listeners.delete(callback);
    };
  };

  notify() {
    for (const listener of this.listeners) {
      listener();
    }
  }
}

/**
 * Creates an authentication URL for the given secret.
 *
 * @param secret - The secret to encode.
 * @param expiresAtParam - The expiration time in milliseconds. Defaults to 15 minutes from now.
 * @returns The authentication URL.
 */
export function createAuthURL(secret: string, expiresAtParam?: number) {
  const expiresAt = expiresAtParam ?? Date.now() + 15 * 60 * 1000;
  const payload = JSON.stringify({ s: secret, t: expiresAt });
  const encoded = bytesToBase64url(new TextEncoder().encode(payload)).replace(
    /=/g,
    "",
  );
  return `${window.location.origin}/auth#${encoded}`;
}

/**
 * Parses an authentication URL to extract the secret and expiration time.
 *
 * @param url - The authentication URL to parse.
 * @returns The parsed authentication URL or null if the URL is invalid.
 */
export function parseAuthURL(
  url: string,
): { secret: string; expiresAt: number } | null {
  try {
    const hash = new URL(url).hash.substring(1);
    if (!hash) return null;

    // Add padding if needed
    const paddedHash = hash.padEnd(
      hash.length + ((4 - (hash.length % 4)) % 4),
      "=",
    );

    const decodedBytes = base64URLtoBytes(paddedHash);
    const decoded = new TextDecoder().decode(decodedBytes);
    const { s, t } = JSON.parse(decoded);

    if (typeof s === "string" && typeof t === "number") {
      return { secret: s, expiresAt: t };
    }
  } catch (e) {
    console.error("Invalid auth URL", e);
  }
  return null;
}
