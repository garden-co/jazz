import { base64URLtoBytes } from "cojson";
import {
  ControlledAccountOrAgent,
  JsonValue,
  JsonObject,
  stableStringify,
  KeySecret,
  SessionLogImpl,
  Signature,
  PrivateTransaction,
  TrustingTransaction,
  Transaction,
  RawCoID,
  SessionID,
  PureJSCrypto,
  CojsonInternalTypes,
  bytesToBase64url,
} from "cojson";
import { HybridCoJSONCoreRN, SessionLogHandle } from "cojson-core-rn";
import { textDecoder, textEncoder } from "cojson/dist/crypto/crypto.js";

/**
 * React Native implementation of the CryptoProvider interface using cojson-core-rn.
 * This provides the primary implementation using Rust cojson-core-rn crate for optimal performance, offering:
 * - SessionLog
 * - Signing/verifying (Ed25519)
 * - Encryption/decryption (XSalsa20)
 * - Sealing/unsealing (X25519 + XSalsa20-Poly1305)
 * - Hashing (BLAKE3)
 */
export class RNCrypto extends PureJSCrypto {
  private constructor() {
    super();
  }

  static async create(): Promise<RNCrypto> {
    return new RNCrypto();
  }

  seal<T extends JsonValue>({
    message,
    from,
    to,
    nOnceMaterial,
  }: {
    message: T;
    from: CojsonInternalTypes.SealerSecret;
    to: CojsonInternalTypes.SealerID;
    nOnceMaterial: { in: RawCoID; tx: CojsonInternalTypes.TransactionID };
  }): CojsonInternalTypes.Sealed<T> {
    const { success, data, error } = HybridCoJSONCoreRN.sealMessage(
      textEncoder.encode(stableStringify(message)).buffer as ArrayBuffer,
      from,
      to,
      textEncoder.encode(stableStringify(nOnceMaterial)).buffer as ArrayBuffer,
    );
    if (!success) {
      throw new Error(error);
    }

    return `sealed_U${bytesToBase64url(new Uint8Array(data))}` as CojsonInternalTypes.Sealed<T>;
  }

  unseal<T extends JsonValue>(
    sealed: CojsonInternalTypes.Sealed<T>,
    sealer: CojsonInternalTypes.SealerSecret,
    from: CojsonInternalTypes.SealerID,
    nOnceMaterial: { in: RawCoID; tx: CojsonInternalTypes.TransactionID },
  ): T | undefined {
    const { success, data, error } = HybridCoJSONCoreRN.unsealMessage(
      base64URLtoBytes(sealed.substring("sealed_U".length))
        .buffer as ArrayBuffer,
      sealer,
      from,
      textEncoder.encode(stableStringify(nOnceMaterial)).buffer as ArrayBuffer,
    );

    if (!success) {
      throw new Error(error);
    }

    const plaintext = textDecoder.decode(data);
    try {
      return JSON.parse(plaintext) as T;
    } catch (e) {
      console.error("Failed to decrypt/parse sealed message", { err: e });
      return undefined;
    }
  }

  createSessionLog(
    coId: RawCoID,
    sessionId: SessionID,
    signerId?: string,
  ): SessionLogImpl {
    const handle = HybridCoJSONCoreRN.createSessionLog(
      coId,
      sessionId,
      signerId || "",
    );
    return new RNSessionLog(handle);
  }
}

/**
 * Wrapper class that adapts the React Native SessionLog to match the SessionLogImpl interface
 * by converting between ArrayBuffer and Uint8Array types as needed.
 */
class RNSessionLog implements SessionLogImpl {
  constructor(private readonly handle: SessionLogHandle) {}

  clone(): SessionLogImpl {
    const clonedHandle = HybridCoJSONCoreRN.cloneSessionLog(this.handle);
    return new RNSessionLog(clonedHandle);
  }

  tryAdd(
    transactions: Transaction[],
    newSignature: Signature,
    skipVerify: boolean,
  ): void {
    // Convert Transaction objects to JSON strings for the native layer
    const transactionsJson = transactions.map((tx) => stableStringify(tx));

    const { success, error } = HybridCoJSONCoreRN.tryAddTransactions(
      this.handle,
      transactionsJson,
      newSignature,
      skipVerify,
    );
    if (!success) {
      throw new Error(error);
    }
  }

  addNewPrivateTransaction(
    signerAgent: ControlledAccountOrAgent,
    changes: JsonValue[],
    keyID: `key_z${string}`,
    keySecret: KeySecret,
    madeAt: number,
    meta: JsonObject | undefined,
  ): { signature: Signature; transaction: PrivateTransaction } {
    const changesJson = stableStringify(changes);
    const metaString = meta ? stableStringify(meta) : "";

    const { success, result, error } =
      HybridCoJSONCoreRN.addNewPrivateTransaction(
        this.handle,
        changesJson,
        signerAgent.agentSecret,
        keySecret,
        keyID,
        madeAt,
        metaString,
      );
    if (!success) {
      throw new Error(error);
    }

    // Parse the result which should contain both signature and transaction
    const parsed = JSON.parse(result) as any;
    return {
      signature: parsed.signature as Signature,
      transaction: parsed.transaction as PrivateTransaction,
    };
  }

  addNewTrustingTransaction(
    signerAgent: ControlledAccountOrAgent,
    changes: JsonValue[],
    madeAt: number,
    meta: JsonObject | undefined,
  ): { signature: Signature; transaction: TrustingTransaction } {
    const changesJson = stableStringify(changes);
    const metaString = meta ? stableStringify(meta) : "";

    const { success, result, error } =
      HybridCoJSONCoreRN.addNewTrustingTransaction(
        this.handle,
        changesJson,
        signerAgent.agentSecret,
        madeAt,
        metaString,
      );
    if (!success) {
      throw new Error(error);
    }

    // Parse the result which should contain both signature and transaction
    const parsed = JSON.parse(result) as any;
    return {
      signature: parsed.signature as Signature,
      transaction: parsed.transaction as TrustingTransaction,
    };
  }

  decryptNextTransactionChangesJson(
    tx_index: number,
    key_secret: KeySecret,
  ): string {
    // Convert KeySecret string to ArrayBuffer for the native layer
    // KeySecret is a base58-encoded string, we need to decode it to bytes
    const keyBytes = new TextEncoder().encode(key_secret); // Temporary - may need proper base58 decoding
    const arrayBuffer =
      keyBytes.buffer instanceof ArrayBuffer
        ? keyBytes.buffer.slice(
            keyBytes.byteOffset,
            keyBytes.byteOffset + keyBytes.byteLength,
          )
        : new ArrayBuffer(keyBytes.byteLength);

    const { success, result, error } =
      HybridCoJSONCoreRN.decryptNextTransactionChangesJson(
        this.handle,
        tx_index,
        arrayBuffer,
      );
    if (!success) {
      throw new Error(error);
    }
    return result;
  }

  decryptNextTransactionMetaJson(
    tx_index: number,
    key_secret: KeySecret,
  ): string | undefined {
    // Note: The native layer doesn't have a separate decryptNextTransactionMetaJson method
    // Using decryptNextTransactionChangesJson as fallback for now
    try {
      return this.decryptNextTransactionChangesJson(tx_index, key_secret);
    } catch {
      return undefined;
    }
  }

  free() {
    HybridCoJSONCoreRN.destroySessionLog(this.handle);
  }
}
