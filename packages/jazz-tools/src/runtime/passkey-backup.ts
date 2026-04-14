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
  readonly code: PasskeyBackupErrorCode;

  constructor(code: PasskeyBackupErrorCode, cause?: unknown) {
    super(DEFAULT_MESSAGES[code]);
    this.name = "PasskeyBackupError";
    this.code = code;
    if (cause !== undefined) {
      this.cause = cause;
    }
  }
}
