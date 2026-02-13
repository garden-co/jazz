/**
 * Helpers for creating and validating sync client IDs.
 */

const UUID_REGEX = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

/**
 * Validate that a client ID is a UUID string.
 */
export function isValidClientId(clientId: string): boolean {
  return UUID_REGEX.test(clientId);
}

/**
 * Generate a UUIDv4 client ID.
 */
export function generateClientId(): string {
  if (typeof globalThis.crypto?.randomUUID === "function") {
    return globalThis.crypto.randomUUID();
  }

  // Fallback for environments without crypto.randomUUID.
  const bytes = new Uint8Array(16);
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = Math.floor(Math.random() * 256);
  }

  // UUIDv4 version/variant bits.
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;

  const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    hex.slice(12, 16),
    hex.slice(16, 20),
    hex.slice(20),
  ].join("-");
}

/**
 * Resolve a usable client ID from optional user input.
 */
export function resolveClientId(clientId?: string): string {
  if (!clientId) {
    return generateClientId();
  }

  if (!isValidClientId(clientId)) {
    throw new Error(`Invalid clientId "${clientId}" (expected UUID)`);
  }

  return clientId;
}
