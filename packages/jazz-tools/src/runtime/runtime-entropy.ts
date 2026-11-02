let nativeEntropy: (() => Uint8Array) | undefined;

/** @internal Installed by the native account host, backed by OS randomness. */
export function installNativeRuntimeEntropy(source: () => Uint8Array): void {
  nativeEntropy = source;
}

/** Shared identity/transaction randomness; never install a global crypto shim. */
export function runtimeRandomBytes(length: 16 | 32): Uint8Array {
  if (globalThis.crypto?.getRandomValues) {
    return globalThis.crypto.getRandomValues(new Uint8Array(length));
  }
  if (nativeEntropy) {
    const bytes = nativeEntropy();
    if (!(bytes instanceof Uint8Array) || bytes.byteLength < length) {
      throw new Error(`Native runtime entropy returned fewer than ${length * 8} bits`);
    }
    return bytes.slice(0, length);
  }
  throw new Error("Jazz requires a cryptographically secure runtime entropy source");
}

/**
 * A random version-4 node UUID (RFC 9562), as 16 wire-order bytes: the form
 * every transaction-issuing node is minted in, pooled or not.
 */
export function randomNodeUuidBytes(entropy: Uint8Array = runtimeRandomBytes(16)): Uint8Array {
  const bytes = entropy.slice(0, 16);
  bytes[6] = (bytes[6]! & 0x0f) | 0x40;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  return bytes;
}

/** Whether `bytes` is a version-4 node UUID (see {@link randomNodeUuidBytes}). */
export function isRandomNodeUuid(bytes: Uint8Array): boolean {
  return bytes.byteLength === 16 && (bytes[6]! & 0xf0) === 0x40 && (bytes[8]! & 0xc0) === 0x80;
}

/** Nonzero wire incarnation; independent of process lifetime and numeric order. */
export function runtimeConnectionIncarnation(
  entropy: () => Uint8Array = () => runtimeRandomBytes(16),
): bigint {
  for (;;) {
    const bytes = entropy();
    const epoch = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength).getBigUint64(
      0,
      true,
    );
    if (epoch !== 0n) return epoch;
  }
}
