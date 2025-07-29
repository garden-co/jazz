import { RawCoID, SessionID, SessionLogImpl, SignerID } from "cojson";
import { SessionLog } from "cojson-core-rn";
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
    const rnSessionLog = new SessionLog(coID, sessionID, signerID);
    return new RNSessionLog(rnSessionLog);
  }
}

/**
 * Wrapper class that adapts the React Native SessionLog to match the SessionLogImpl interface
 * by converting between ArrayBuffer and Uint8Array types as needed.
 */
class RNSessionLog implements SessionLogImpl {
  constructor(private readonly rnSessionLog: SessionLog) {}

  clone(): SessionLogImpl {
    return new RNSessionLog(this.rnSessionLog.clone());
  }

  tryAdd(
    transactionsJson: string[],
    newSignatureStr: string,
    skipVerify: boolean,
  ): string {
    return this.rnSessionLog.tryAdd(
      transactionsJson,
      newSignatureStr,
      skipVerify,
    );
  }

  addNewPrivateTransaction(
    changesJson: string,
    signerSecret: string,
    encryptionKey: string,
    keyId: string,
    madeAt: number,
  ): string {
    return this.rnSessionLog.addNewPrivateTransaction(
      changesJson,
      signerSecret,
      encryptionKey,
      keyId,
      madeAt,
    );
  }

  addNewTrustingTransaction(
    changesJson: string,
    signerSecret: string,
    madeAt: number,
  ): string {
    return this.rnSessionLog.addNewTrustingTransaction(
      changesJson,
      signerSecret,
      madeAt,
    );
  }

  testExpectedHashAfter(transactionsJson: string[]): string {
    return this.rnSessionLog.testExpectedHashAfter(transactionsJson);
  }

  /**
   * Converts Uint8Array to ArrayBuffer for compatibility with React Native SessionLog
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

    return this.rnSessionLog.decryptNextTransactionChangesJson(
      txIndex,
      arrayBuffer,
    );
  }
}
