import { RawCoID, SessionID, SessionLogImpl, SignerID } from "cojson";
import { HybridCoJSONCoreRN, SessionLogHandle } from "cojson-core-rn";
import { PureJSCrypto } from "cojson/crypto/PureJSCrypto";

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
    transactionsJson: string[],
    newSignatureStr: string,
    skipVerify: boolean,
  ): string {
    const { success, result, error } = HybridCoJSONCoreRN.tryAddTransactions(
      this.handle,
      transactionsJson,
      newSignatureStr,
      skipVerify,
    );
    if (!success) {
      throw new Error(error);
    }
    return result;
  }

  addNewPrivateTransaction(
    changesJson: string,
    signerSecret: string,
    encryptionKey: string,
    keyId: string,
    madeAt: number,
  ): string {
    const { success, result, error } =
      HybridCoJSONCoreRN.addNewPrivateTransaction(
        this.handle,
        changesJson,
        signerSecret,
        encryptionKey,
        keyId,
        madeAt,
      );
    if (!success) {
      throw new Error(error);
    }
    return result;
  }

  addNewTrustingTransaction(
    changesJson: string,
    signerSecret: string,
    madeAt: number,
  ): string {
    const { success, result, error } =
      HybridCoJSONCoreRN.addNewTrustingTransaction(
        this.handle,
        changesJson,
        signerSecret,
        madeAt,
      );
    if (!success) {
      throw new Error(error);
    }
    return result;
  }

  testExpectedHashAfter(transactionsJson: string[]): string {
    const { success, result, error } = HybridCoJSONCoreRN.testExpectedHashAfter(
      this.handle,
      transactionsJson,
    );
    if (!success) {
      throw new Error(error);
    }
    return result;
  }

  /**
   * Converts Uint8Array to ArrayBuffer for compatibility with React Native SessionLog.
   * This ensures proper type conversion between the SessionLogImpl interface (Uint8Array)
   * and the cojson-core-rn native module (ArrayBuffer).
   */
  decryptNextTransactionChangesJson(
    txIndex: number,
    keySecret: Uint8Array,
  ): string {
    // Convert Uint8Array to ArrayBuffer, ensuring we get a proper ArrayBuffer
    const arrayBuffer =
      keySecret.buffer instanceof ArrayBuffer
        ? keySecret.buffer.slice(
            keySecret.byteOffset,
            keySecret.byteOffset + keySecret.byteLength,
          )
        : new ArrayBuffer(keySecret.byteLength);

    // If we had to create a new ArrayBuffer, copy the data
    if (!(keySecret.buffer instanceof ArrayBuffer)) {
      new Uint8Array(arrayBuffer).set(keySecret);
    }

    const { success, result, error } =
      HybridCoJSONCoreRN.decryptNextTransactionChangesJson(
        this.handle,
        txIndex,
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

  free() {
    HybridCoJSONCoreRN.destroySessionLog(this.handle);
  }
}
