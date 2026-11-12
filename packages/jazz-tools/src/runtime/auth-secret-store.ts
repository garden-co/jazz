import { formatAuthSecret } from "./auth-secret-codec.js";

export {
  authSecretStorageKey,
  AuthSecretFormatError,
  formatAuthSecret,
  parseAuthSecret,
} from "./auth-secret-codec.js";

/**
 * Generate a new 32-byte auth secret in canonical versioned form.
 * Uses the platform's native CSPRNG.
 */
export function generateAuthSecret(): string {
  const bytes = new Uint8Array(32);
  crypto.getRandomValues(bytes);
  return formatAuthSecret(bytes);
}
