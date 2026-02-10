import {
  JsonValue,
  RawCoID,
  Stringified,
  base64URLtoBytes,
  bytesToBase64url,
} from "../exports.js";
import { CojsonInternalTypes } from "../exports.js";
import { TransactionID } from "../ids.js";
import { stableStringify } from "../jsonStringify.js";
import { logger } from "../logger.js";
import { Transaction } from "../coValueCore/verifiedState.js";
import {
  CryptoProvider,
  Sealed,
  SealedForGroup,
  SealerID,
  SealerSecret,
  SessionMapImpl,
  ShortHash,
  SignerID,
  SignerSecret,
  textDecoder,
  textEncoder,
} from "./crypto.js";
import {
  blake3HashOnce,
  blake3HashOnceWithContext,
  verify,
  encrypt,
  decrypt,
  newEd25519SigningKey,
  newX25519PrivateKey,
  getSealerId,
  getSignerId,
  shortHash,
  sign,
  seal,
  sealForGroup,
  unseal,
  unsealForGroup,
  Blake3Hasher,
  bytesToBase64url as nativeBytesToBase64url,
  base64urlToBytes as nativeBase64urlToBytes,
  bytesToBase64 as nativeBytesToBase64,
  SessionMap as RNSessionMap,
} from "cojson-core-rn";
import { setNativeBase64Implementation } from "../base64url.js";

type Blake3State = Blake3Hasher;

/**
 *
 * @param view - The Uint8Array to convert to an ArrayBuffer.
 * @returns The ArrayBuffer.
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

export class RNCrypto extends CryptoProvider<Blake3State> {
  private constructor() {
    super();
  }

  getSignerID(secret: SignerSecret): SignerID {
    return getSignerId(secret) as SignerID;
  }
  newX25519StaticSecret(): Uint8Array {
    return new Uint8Array(newX25519PrivateKey());
  }
  getSealerID(secret: SealerSecret): SealerID {
    return getSealerId(secret) as SealerID;
  }
  blake3HashOnce(data: Uint8Array): Uint8Array {
    return new Uint8Array(blake3HashOnce(toArrayBuffer(data)));
  }
  blake3HashOnceWithContext(
    data: Uint8Array,
    { context }: { context: Uint8Array },
  ): Uint8Array {
    return new Uint8Array(
      blake3HashOnceWithContext(toArrayBuffer(data), toArrayBuffer(context)),
    );
  }

  shortHash(value: JsonValue): ShortHash {
    return shortHash(stableStringify(value)) as ShortHash;
  }

  seal<T extends JsonValue>({
    message,
    from,
    to,
    nOnceMaterial,
  }: {
    message: T;
    from: SealerSecret;
    to: SealerID;
    nOnceMaterial: { in: RawCoID; tx: TransactionID };
  }): Sealed<T> {
    const messageBuffer = toArrayBuffer(
      textEncoder.encode(stableStringify(message)),
    );
    const nOnceBuffer = toArrayBuffer(
      textEncoder.encode(stableStringify(nOnceMaterial)),
    );

    return `sealed_U${bytesToBase64url(
      new Uint8Array(seal(messageBuffer, from, to, nOnceBuffer)),
    )}` as Sealed<T>;
  }
  unseal<T extends JsonValue>(
    sealed: Sealed<T>,
    sealer: SealerSecret,
    from: SealerID,
    nOnceMaterial: { in: RawCoID; tx: TransactionID },
  ): T | undefined {
    const sealedBytes = base64URLtoBytes(sealed.substring("sealed_U".length));
    const nonceBuffer = toArrayBuffer(
      textEncoder.encode(stableStringify(nOnceMaterial)),
    );

    const plaintext = textDecoder.decode(
      unseal(toArrayBuffer(sealedBytes), sealer, from, nonceBuffer),
    );
    try {
      return JSON.parse(plaintext) as T;
    } catch (e) {
      logger.error("Failed to decrypt/parse sealed message", { err: e });
      return undefined;
    }
  }

  sealForGroup<T extends JsonValue>({
    message,
    to,
    nOnceMaterial,
  }: {
    message: T;
    to: SealerID;
    nOnceMaterial: { in: RawCoID; tx: TransactionID };
  }): SealedForGroup<T> {
    const messageBuffer = toArrayBuffer(
      textEncoder.encode(stableStringify(message)),
    );
    const nOnceBuffer = toArrayBuffer(
      textEncoder.encode(stableStringify(nOnceMaterial)),
    );

    return `sealedForGroup_U${bytesToBase64url(
      new Uint8Array(sealForGroup(messageBuffer, to, nOnceBuffer)),
    )}` as SealedForGroup<T>;
  }

  unsealForGroup<T extends JsonValue>(
    sealed: SealedForGroup<T>,
    groupSealerSecret: SealerSecret,
    nOnceMaterial: { in: RawCoID; tx: TransactionID },
  ): T | undefined {
    try {
      const sealedBytes = base64URLtoBytes(
        sealed.substring("sealedForGroup_U".length),
      );
      const nonceBuffer = toArrayBuffer(
        textEncoder.encode(stableStringify(nOnceMaterial)),
      );

      const plaintext = textDecoder.decode(
        unsealForGroup(
          toArrayBuffer(sealedBytes),
          groupSealerSecret,
          nonceBuffer,
        ),
      );
      return JSON.parse(plaintext) as T;
    } catch (e) {
      logger.error("Failed to decrypt/parse sealed for group message", {
        err: e,
      });
      return undefined;
    }
  }

  createSessionMap(
    coID: RawCoID,
    headerJson: string,
    maxTxSize?: number,
    skipVerify?: boolean,
  ): SessionMapImpl {
    return new SessionMapAdapter(
      new RNSessionMap(coID, headerJson, maxTxSize, skipVerify),
    );
  }

  static async create(): Promise<RNCrypto> {
    // Register native base64 implementation for React Native
    setNativeBase64Implementation({
      bytesToBase64url: nativeBytesToBase64url,
      base64urlToBytes: nativeBase64urlToBytes,
      bytesToBase64: nativeBytesToBase64,
    });
    return new RNCrypto();
  }

  newEd25519SigningKey(): Uint8Array {
    return new Uint8Array(newEd25519SigningKey());
  }

  sign(
    secret: CojsonInternalTypes.SignerSecret,
    message: JsonValue,
  ): CojsonInternalTypes.Signature {
    return sign(
      toArrayBuffer(textEncoder.encode(stableStringify(message))),
      secret,
    ) as CojsonInternalTypes.Signature;
  }

  verify(
    signature: CojsonInternalTypes.Signature,
    message: JsonValue,
    id: CojsonInternalTypes.SignerID,
  ): boolean {
    const result = verify(
      signature,
      toArrayBuffer(textEncoder.encode(stableStringify(message))),
      id,
    );

    return result;
  }

  encrypt<T extends JsonValue, N extends JsonValue>(
    value: T,
    keySecret: CojsonInternalTypes.KeySecret,
    nOnceMaterial: N,
  ): CojsonInternalTypes.Encrypted<T, N> {
    const valueBytes = toArrayBuffer(
      textEncoder.encode(stableStringify(value)),
    );
    const nOnceBytes = toArrayBuffer(
      textEncoder.encode(stableStringify(nOnceMaterial)),
    );

    const encrypted = `encrypted_U${bytesToBase64url(
      new Uint8Array(encrypt(valueBytes, keySecret, nOnceBytes)),
    )}` as CojsonInternalTypes.Encrypted<T, N>;
    return encrypted;
  }

  decryptRaw<T extends JsonValue, N extends JsonValue>(
    encrypted: CojsonInternalTypes.Encrypted<T, N>,
    keySecret: CojsonInternalTypes.KeySecret,
    nOnceMaterial: N,
  ): Stringified<T> {
    const buffer = base64URLtoBytes(encrypted.substring("encrypted_U".length));

    const decrypted = textDecoder.decode(
      decrypt(
        toArrayBuffer(buffer),
        keySecret,
        toArrayBuffer(textEncoder.encode(stableStringify(nOnceMaterial))),
      ),
    ) as Stringified<T>;

    return decrypted;
  }
}

/**
 * Adapter wrapping RNSessionMap to implement SessionMapImpl interface
 */
class SessionMapAdapter implements SessionMapImpl {
  constructor(private readonly sessionMap: RNSessionMap) {}

  // === Header ===
  getHeader(): string {
    return this.sessionMap.getHeader();
  }

  // === Transaction Operations ===
  addTransactions(
    sessionId: string,
    signerId: string | undefined,
    transactionsJson: string,
    signature: string,
    skipVerify: boolean,
  ): void {
    this.sessionMap.addTransactions(
      sessionId,
      signerId,
      transactionsJson,
      signature,
      skipVerify,
    );
  }

  makeNewPrivateTransaction(
    sessionId: string,
    signerSecret: string,
    changesJson: string,
    keyId: string,
    keySecret: string,
    metaJson: string | undefined,
    madeAt: number,
  ): string {
    return this.sessionMap.makeNewPrivateTransaction(
      sessionId,
      signerSecret,
      changesJson,
      keyId,
      keySecret,
      metaJson,
      madeAt,
    );
  }

  makeNewTrustingTransaction(
    sessionId: string,
    signerSecret: string,
    changesJson: string,
    metaJson: string | undefined,
    madeAt: number,
  ): string {
    return this.sessionMap.makeNewTrustingTransaction(
      sessionId,
      signerSecret,
      changesJson,
      metaJson,
      madeAt,
    );
  }

  // === Session Queries ===
  getSessionIds(): string[] {
    return this.sessionMap.getSessionIds();
  }

  getTransactionCount(sessionId: string): number {
    return this.sessionMap.getTransactionCount(sessionId);
  }

  getTransaction(sessionId: string, txIndex: number): Transaction | undefined {
    const result = this.sessionMap.getTransaction(sessionId, txIndex);
    if (!result) return undefined;
    return JSON.parse(result) as Transaction;
  }

  getSessionTransactions(
    sessionId: string,
    fromIndex: number,
  ): Transaction[] | undefined {
    const result = this.sessionMap.getSessionTransactions(sessionId, fromIndex);
    if (!result) return undefined;
    return result.map((tx) => JSON.parse(tx) as Transaction);
  }

  getLastSignature(sessionId: string): string | undefined {
    return this.sessionMap.getLastSignature(sessionId) ?? undefined;
  }

  getSignatureAfter(sessionId: string, txIndex: number): string | undefined {
    return this.sessionMap.getSignatureAfter(sessionId, txIndex) ?? undefined;
  }

  getLastSignatureCheckpoint(sessionId: string): number | undefined {
    return this.sessionMap.getLastSignatureCheckpoint(sessionId) ?? undefined;
  }

  // === Known State ===
  getKnownState(): {
    id: string;
    header: boolean;
    sessions: Record<string, number>;
  } {
    // Uniffi returns a Record with Map<string, number> for sessions
    // Convert Map to Record for consistency
    // Type assertion needed until Uniffi types are regenerated
    const ks = this.sessionMap.getKnownState() as unknown as {
      id: string;
      header: boolean;
      sessions: Map<string, number>;
    };
    return {
      id: ks.id,
      header: ks.header,
      sessions: Object.fromEntries(ks.sessions),
    };
  }

  getKnownStateWithStreaming():
    | { id: string; header: boolean; sessions: Record<string, number> }
    | undefined {
    // Uniffi returns a Record with Map<string, number> for sessions
    // Type assertion needed until Uniffi types are regenerated
    const ks = this.sessionMap.getKnownStateWithStreaming() as unknown as
      | { id: string; header: boolean; sessions: Map<string, number> }
      | undefined;
    if (!ks || ks === undefined) return undefined;
    return {
      id: ks.id,
      header: ks.header,
      sessions: Object.fromEntries(ks.sessions),
    };
  }

  setStreamingKnownState(streamingJson: string): void {
    this.sessionMap.setStreamingKnownState(streamingJson);
  }

  // === Deletion ===
  markAsDeleted(): void {
    this.sessionMap.markAsDeleted();
  }

  isDeleted(): boolean {
    return this.sessionMap.isDeleted();
  }

  // === Decryption ===
  decryptTransaction(
    sessionId: string,
    txIndex: number,
    keySecret: string,
  ): string | undefined {
    return (
      this.sessionMap.decryptTransaction(sessionId, txIndex, keySecret) ??
      undefined
    );
  }

  decryptTransactionMeta(
    sessionId: string,
    txIndex: number,
    keySecret: string,
  ): string | undefined {
    return (
      this.sessionMap.decryptTransactionMeta(sessionId, txIndex, keySecret) ??
      undefined
    );
  }
}
