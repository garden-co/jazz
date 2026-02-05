const encoder = new TextEncoder();
const decoder = new TextDecoder();

// Check for native base64 support (available in modern runtimes)
const hasNativeBase64 =
  typeof (Uint8Array.prototype as unknown as { toBase64?: unknown })
    .toBase64 === "function" &&
  typeof (Uint8Array as unknown as { fromBase64?: unknown }).fromBase64 ===
    "function";

// Native implementation hooks for React Native (set via setNativeBase64Implementation)
let nativeBytesToBase64url: ((bytes: ArrayBuffer) => string) | undefined;
let nativeBase64urlToBytes: ((base64: string) => ArrayBuffer) | undefined;
let nativeBytesToBase64: ((bytes: ArrayBuffer) => string) | undefined;

/**
 * Set native base64 implementation for React Native.
 * Called by RNCrypto.create() to register native Rust implementations.
 * @internal
 */
export function setNativeBase64Implementation(impl: {
  bytesToBase64url: (bytes: ArrayBuffer) => string;
  base64urlToBytes: (base64: string) => ArrayBuffer;
  bytesToBase64: (bytes: ArrayBuffer) => string;
}): void {
  nativeBytesToBase64url = impl.bytesToBase64url;
  nativeBase64urlToBytes = impl.base64urlToBytes;
  nativeBytesToBase64 = impl.bytesToBase64;
}

/**
 * Convert Uint8Array to ArrayBuffer, handling views correctly.
 */
function toArrayBuffer(view: Uint8Array): ArrayBuffer {
  if (
    view.byteOffset === 0 &&
    view.byteLength === view.buffer.byteLength &&
    view.buffer instanceof ArrayBuffer
  ) {
    return view.buffer;
  }
  const buffer = new ArrayBuffer(view.byteLength);
  new Uint8Array(buffer).set(view);
  return buffer;
}

export function base64URLtoBytes(base64: string): Uint8Array {
  // Use React Native native implementation if available
  if (nativeBase64urlToBytes) {
    return new Uint8Array(nativeBase64urlToBytes(base64));
  }
  // Use browser native implementation if available
  if (hasNativeBase64) {
    return (
      Uint8Array as unknown as {
        fromBase64: (s: string, opts: { alphabet: string }) => Uint8Array;
      }
    ).fromBase64(base64, { alphabet: "base64url" });
  }
  return base64URLtoBytesFallback(base64);
}

export function bytesToBase64url(bytes: Uint8Array): string {
  // Use React Native native implementation if available
  if (nativeBytesToBase64url) {
    return nativeBytesToBase64url(toArrayBuffer(bytes));
  }
  // Use browser native implementation if available
  if (hasNativeBase64) {
    return (
      bytes as unknown as {
        toBase64: (opts: { alphabet: string }) => string;
      }
    ).toBase64({ alphabet: "base64url" });
  }
  return bytesToBase64urlFallback(bytes);
}

/**
 * Encode bytes to standard base64 (not URL-safe).
 * Use this for data URLs and other contexts requiring standard base64.
 */
export function bytesToBase64(bytes: Uint8Array): string {
  // Use React Native native implementation if available
  if (nativeBytesToBase64) {
    return nativeBytesToBase64(toArrayBuffer(bytes));
  }
  // Use browser native implementation if available
  if (hasNativeBase64) {
    return (
      bytes as unknown as {
        toBase64: () => string;
      }
    ).toBase64();
  }
  return bytesToBase64Fallback(bytes);
}

// --- Fallback implementations ---

function base64URLtoBytesFallback(base64: string): Uint8Array {
  base64 = base64.replace(/=/g, "");
  const n = base64.length;
  const rem = n % 4;
  const k = rem && rem - 1; // how many bytes the last base64 chunk encodes
  const m = (n >> 2) * 3 + k; // total encoded bytes

  const encoded = new Uint8Array(n + 3);
  encoder.encodeInto(base64 + "===", encoded);

  for (let i = 0, j = 0; i < n; i += 4, j += 3) {
    const x =
      (lookup[encoded[i]!]! << 18) +
      (lookup[encoded[i + 1]!]! << 12) +
      (lookup[encoded[i + 2]!]! << 6) +
      lookup[encoded[i + 3]!]!;
    encoded[j] = x >> 16;
    encoded[j + 1] = (x >> 8) & 0xff;
    encoded[j + 2] = x & 0xff;
  }
  return new Uint8Array(encoded.buffer, 0, m);
}

function bytesToBase64urlFallback(bytes: Uint8Array): string {
  const m = bytes.length;
  const k = m % 3;
  const n = Math.floor(m / 3) * 4 + (k && k + 1);
  const N = Math.ceil(m / 3) * 4;
  const encoded = new Uint8Array(N);

  for (let i = 0, j = 0; j < m; i += 4, j += 3) {
    const y = (bytes[j]! << 16) + (bytes[j + 1]! << 8) + (bytes[j + 2]! | 0);
    encoded[i] = encodeLookup[y >> 18]!;
    encoded[i + 1] = encodeLookup[(y >> 12) & 0x3f]!;
    encoded[i + 2] = encodeLookup[(y >> 6) & 0x3f]!;
    encoded[i + 3] = encodeLookup[y & 0x3f]!;
  }

  let base64 = decoder.decode(new Uint8Array(encoded.buffer, 0, n));
  if (k === 1) base64 += "==";
  if (k === 2) base64 += "=";

  return base64;
}

function bytesToBase64Fallback(bytes: Uint8Array): string {
  const m = bytes.length;
  const k = m % 3;
  const n = Math.floor(m / 3) * 4 + (k && k + 1);
  const N = Math.ceil(m / 3) * 4;
  const encoded = new Uint8Array(N);

  for (let i = 0, j = 0; j < m; i += 4, j += 3) {
    const y = (bytes[j]! << 16) + (bytes[j + 1]! << 8) + (bytes[j + 2]! | 0);
    encoded[i] = encodeLookupStd[y >> 18]!;
    encoded[i + 1] = encodeLookupStd[(y >> 12) & 0x3f]!;
    encoded[i + 2] = encodeLookupStd[(y >> 6) & 0x3f]!;
    encoded[i + 3] = encodeLookupStd[y & 0x3f]!;
  }

  let base64 = decoder.decode(new Uint8Array(encoded.buffer, 0, n));
  if (k === 1) base64 += "==";
  if (k === 2) base64 += "=";

  return base64;
}

// base64url alphabet (RFC 4648 §5)
const alphabetUrl =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

// Standard base64 alphabet (RFC 4648 §4)
const alphabetStd =
  "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

const lookup = new Uint8Array(128);
for (const [i, a] of Array.from(alphabetUrl).entries()) {
  lookup[a.charCodeAt(0)] = i;
}
lookup["=".charCodeAt(0)] = 0;

const encodeLookup = new Uint8Array(64);
for (const [i, a] of Array.from(alphabetUrl).entries()) {
  encodeLookup[i] = a.charCodeAt(0);
}

const encodeLookupStd = new Uint8Array(64);
for (const [i, a] of Array.from(alphabetStd).entries()) {
  encodeLookupStd[i] = a.charCodeAt(0);
}
