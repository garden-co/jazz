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
