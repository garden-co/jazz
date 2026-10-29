import type { CryptoMechanism } from "./envelope.js";

/** Chunk boundaries are transport details, not cryptographic record boundaries.
 * Implementations must bound memory, respect pull backpressure and cancellation,
 * return owned output chunks, and throw on corruption or missing final authentication.
 * Successfully consuming a prefix does not establish whole-stream success.
 * Early return requests upstream cleanup without awaiting it or propagating cleanup
 * failures. Use AbortSignal to interrupt a pending read; return() alone cannot.
 */
export interface LargeValueCipher {
  readonly mechanism: CryptoMechanism;
  encrypt(
    key: Uint8Array,
    context: Uint8Array,
    source: AsyncIterable<Uint8Array>,
    options?: { signal?: AbortSignal },
  ): AsyncIterable<Uint8Array>;
  decrypt(
    key: Uint8Array,
    context: Uint8Array,
    source: AsyncIterable<Uint8Array>,
    options?: { signal?: AbortSignal },
  ): AsyncIterable<Uint8Array>;
}

/** Independent adapter overrides; omitted adapters use available platform defaults. */
export interface JazzCrypto {
  equalityIndex?: EqualityIndex;
  deviceSigner?: DeviceSigner;
  cellCipher?: CellCipher;
  keyEnvelope?: KeyEnvelope;
  largeValueCipher?: LargeValueCipher;
}

/** Resolved platform defaults and independently supplied adapter overrides. */
export interface CryptoAdapters {
  equalityIndex?: EqualityIndex;
  deviceSigner: DeviceSigner;
  cellCipher: CellCipher;
  keyEnvelope: KeyEnvelope;
  largeValueCipher?: LargeValueCipher;
}

export type DeviceKeyPair = Readonly<{ publicKey: Uint8Array; privateKey: Uint8Array }>;

/** Signs canonical lifecycle records; membership and acceptance belong to the caller. */
export interface DeviceSigner {
  readonly mechanism: CryptoMechanism;
  createKeyPair(): Promise<DeviceKeyPair>;
  sign(privateKey: Uint8Array, record: Uint8Array): Promise<Uint8Array>;
  verify(publicKey: Uint8Array, record: Uint8Array, signature: Uint8Array): Promise<boolean>;
}

/** Independently replaceable symmetric key wraps and device envelopes. */
export interface KeyEnvelope {
  readonly mechanism: CryptoMechanism;
  createKeyPair(): Promise<DeviceKeyPair>;
  wrap(wrappingKey: Uint8Array, context: Uint8Array, key: Uint8Array): Promise<Uint8Array>;
  unwrap(wrappingKey: Uint8Array, context: Uint8Array, envelope: Uint8Array): Promise<Uint8Array>;
  seal(publicKey: Uint8Array, context: Uint8Array, key: Uint8Array): Promise<Uint8Array>;
  open(device: DeviceKeyPair, context: Uint8Array, envelope: Uint8Array): Promise<Uint8Array>;
}

/** Replaceable cell encryption; context is the common layer's canonical bytes. */
export interface CellCipher {
  readonly mechanism: CryptoMechanism;
  encrypt(key: Uint8Array, context: Uint8Array, plaintext: Uint8Array): Promise<Uint8Array>;
  decrypt(key: Uint8Array, context: Uint8Array, envelope: Uint8Array): Promise<Uint8Array>;
}

/** Deterministic keyed tokens; the common layer owns scoping and candidate verification. */
export interface EqualityIndex {
  readonly mechanism: CryptoMechanism;
  token(key: Uint8Array, context: Uint8Array, plaintext: Uint8Array): Promise<Uint8Array>;
}
