/**
 * Utility functions for base64url encoding/decoding used by React Native passkey authentication.
 *
 * The react-native-passkey library uses base64url strings, while the browser WebAuthn API
 * uses raw ArrayBuffers. These utilities handle the conversion between formats.
 */

/**
 * Converts a Uint8Array to a base64url-encoded string.
 * Base64url uses '-' and '_' instead of '+' and '/', and omits padding '='.
 */
export function uint8ArrayToBase64Url(bytes: Uint8Array): string {
  // Convert to regular base64 first
  let binary = "";
  for (let i = 0; i < bytes.length; i++) {
    binary += String.fromCharCode(bytes[i]!);
  }
  const base64 = btoa(binary);

  // Convert to base64url: replace + with -, / with _, remove =
  return base64.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

/**
 * Converts a base64url-encoded string to a Uint8Array.
 */
export function base64UrlToUint8Array(base64url: string): Uint8Array {
  // Convert base64url to regular base64
  let base64 = base64url.replace(/-/g, "+").replace(/_/g, "/");

  // Add padding if needed
  const padding = base64.length % 4;
  if (padding > 0) {
    base64 += "=".repeat(4 - padding);
  }

  // Decode base64 to binary string
  const binary = atob(base64);

  // Convert binary string to Uint8Array
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }

  return bytes;
}
