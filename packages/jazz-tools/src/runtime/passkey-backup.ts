export type PasskeyBackupErrorCode =
  | "not-supported"
  | "invalid-secret"
  | "create-failed"
  | "get-failed"
  | "no-credential"
  | "invalid-credential";

const DEFAULT_MESSAGES: Record<PasskeyBackupErrorCode, string> = {
  "not-supported": "WebAuthn is not supported in this browser",
  "invalid-secret": "Secret must be a 32-byte base64url string",
  "create-failed": "Failed to create passkey credential",
  "get-failed": "Failed to retrieve passkey credential",
  "no-credential": "No passkey credential found",
  "invalid-credential": "Passkey credential does not contain a valid secret",
};

export class PasskeyBackupError extends Error {
  readonly name = "PasskeyBackupError";
  readonly code: PasskeyBackupErrorCode;

  constructor(code: PasskeyBackupErrorCode, cause?: unknown) {
    super(DEFAULT_MESSAGES[code]);
    this.code = code;
    if (cause !== undefined) {
      this.cause = cause;
    }
  }
}

export interface BrowserPasskeyBackupOptions {
  appName: string;
  /**
   * Relying-party ID for the passkey credential. Defaults to `location.hostname`.
   * Must be stable across environments for cross-device recovery to work.
   */
  appHostname?: string;
}

function base64urlToBytes(input: string): Uint8Array {
  const normalized = input.replace(/-/g, "+").replace(/_/g, "/");
  const remainder = normalized.length % 4;
  const padded = remainder === 0 ? normalized : normalized + "=".repeat(4 - remainder);
  const binary = atob(padded);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}

function bytesToBase64url(bytes: Uint8Array): string {
  let binary = "";
  for (const b of bytes) binary += String.fromCharCode(b);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

export class BrowserPasskeyBackup {
  private readonly appName: string;
  private readonly rpId: string;

  constructor(options: BrowserPasskeyBackupOptions) {
    this.appName = options.appName;
    this.rpId = options.appHostname ?? globalThis.location?.hostname ?? "localhost";
  }

  async backup(secret: string): Promise<void> {
    if (!globalThis.navigator?.credentials) {
      throw new PasskeyBackupError("not-supported");
    }

    let secretBytes: Uint8Array;
    try {
      secretBytes = base64urlToBytes(secret);
    } catch {
      throw new PasskeyBackupError("invalid-secret");
    }
    if (secretBytes.length !== 32) {
      throw new PasskeyBackupError("invalid-secret");
    }

    // credentials.create call will be added in Task 3
  }

  async restore(): Promise<string> {
    throw new Error("not implemented yet");
  }
}
