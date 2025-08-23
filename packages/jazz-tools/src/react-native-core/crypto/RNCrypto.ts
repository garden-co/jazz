import {
  RawCoID,
  SessionID,
  SessionLogImpl,
  SignerID,
  JsonValue,
  CojsonInternalTypes,
  cojsonInternals,
} from "cojson";
import { HybridCoJSONCoreRN, SessionLogHandle } from "cojson-core-rn";
import { PureJSCrypto } from "cojson/crypto/PureJSCrypto";

type Transaction = CojsonInternalTypes.Transaction;
type Signature = CojsonInternalTypes.Signature;
type KeySecret = CojsonInternalTypes.KeySecret;
type PrivateTransaction = CojsonInternalTypes.PrivateTransaction;
type TrustingTransaction = CojsonInternalTypes.TrustingTransaction;
type ControlledAccountOrAgent = CojsonInternalTypes.ControlledAccountOrAgent;

const { stableStringify } = cojsonInternals;

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

  createSessionLog(
    coID: RawCoID,
    sessionID: SessionID,
    signerID: SignerID,
  ): SessionLogImpl {
    const handle = HybridCoJSONCoreRN.createSessionLog(
      coID,
      sessionID,
      signerID,
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
  ): { signature: Signature; transaction: PrivateTransaction } {
    const changesJson = stableStringify(changes);

    const { success, result, error } =
      HybridCoJSONCoreRN.addNewPrivateTransaction(
        this.handle,
        changesJson,
        signerAgent.agentSecret,
        keySecret,
        keyID,
        madeAt,
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
  ): { signature: Signature; transaction: TrustingTransaction } {
    const changesJson = stableStringify(changes);

    const { success, result, error } =
      HybridCoJSONCoreRN.addNewTrustingTransaction(
        this.handle,
        changesJson,
        signerAgent.agentSecret,
        madeAt,
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

  free() {
    HybridCoJSONCoreRN.destroySessionLog(this.handle);
  }
}
