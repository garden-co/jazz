/** Shared constants for browser tests -- no Node.js imports. */
export const TEST_PORT = 19880;
export const JWT_SECRET = "test-jwt-secret-for-chat-react-tests";
export const ADMIN_SECRET = "test-admin-secret-for-chat-react-tests";
export const APP_ID = "019d4349-24f1-7053-a5ae-b5fb5600f7a7";

/**
 * Derive a valid base64url-encoded 32-byte secret from a human-readable label.
 * Uses SHA-256 so the result is deterministic and always the right format
 * for `mintLocalFirstToken`.
 */
export async function testSecret(label: string): Promise<string> {
  const data = new TextEncoder().encode(label);
  const hash = await crypto.subtle.digest("SHA-256", data);
  const bytes = new Uint8Array(hash);
  let binary = "";
  for (const b of bytes) {
    binary += String.fromCharCode(b);
  }
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}
