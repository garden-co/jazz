const messages = {
  "key-unavailable": "The encryption key is currently unavailable",
  "key-not-shared": "The encryption key has not been shared with this device",
  "maintenance-required": "Encryption key maintenance is required",
  "unsupported-format": "The encrypted data format is not supported",
  "invalid-ciphertext": "Encrypted data could not be authenticated or decoded",
  "encryption-failed": "Could not encrypt the value",
} as const;

export type E2eeDataErrorCode = keyof typeof messages;

/** Fixed diagnostics: adapter exceptions may contain plaintext or key material. */
export class E2eeDataError extends Error {
  constructor(readonly code: E2eeDataErrorCode) {
    super(messages[code]);
    this.name = "E2eeDataError";
  }
}
