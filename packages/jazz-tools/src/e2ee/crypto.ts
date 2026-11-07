import { encodeEnvelope } from "./envelope.js";
import type {
  CellCipher,
  CryptoAdapters,
  DeviceSigner,
  JazzCrypto,
  KeyEnvelope,
  EqualityIndex,
  LargeValueCipher,
} from "./types.js";

/** Common selection seam; supplied adapters do not initialise their defaults. */
export async function resolveCrypto(
  overrides: JazzCrypto,
  defaults: {
    cellCipher(): Promise<CellCipher>;
    keyEnvelope(): Promise<KeyEnvelope>;
    deviceSigner(): Promise<DeviceSigner>;
    equalityIndex?(): Promise<EqualityIndex>;
    largeValueCipher?(): Promise<LargeValueCipher>;
  },
): Promise<CryptoAdapters> {
  const [cellCipher, keyEnvelope, deviceSigner] = await Promise.all([
    overrides.cellCipher ?? defaults.cellCipher(),
    overrides.keyEnvelope ?? defaults.keyEnvelope(),
    overrides.deviceSigner ?? defaults.deviceSigner(),
  ]);
  const adapters = {
    cellCipher,
    keyEnvelope,
    deviceSigner,
    equalityIndex: overrides.equalityIndex ?? (await defaults.equalityIndex?.()),
    largeValueCipher: overrides.largeValueCipher ?? (await defaults.largeValueCipher?.()),
  };
  for (const adapter of Object.values(adapters)) {
    if (adapter !== undefined) encodeEnvelope(adapter.mechanism, new Uint8Array());
  }
  return adapters;
}
