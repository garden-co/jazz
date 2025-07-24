import { NitroModules } from "react-native-nitro-modules";
import type {
  CoJSONCoreRN,
  SessionLogHandle,
  TransactionResult,
} from "./cojson-core-rn.nitro";

export const HybridCoJSONCoreRN =
  NitroModules.createHybridObject<CoJSONCoreRN>("CoJSONCoreRN");

// Export types for external use
export type {
  SessionLogHandle,
  TransactionResult,
  MakeTransactionResult,
} from "./cojson-core-rn.nitro";

// Wrapper functions that call the Hybrid object's methods

/**
 * Create a new session log instance
 */
export function createSessionLog(
  coId: string,
  sessionId: string,
  signerId: string,
): SessionLogHandle {
  return HybridCoJSONCoreRN.createSessionLog(coId, sessionId, signerId);
}

/**
 * Clone an existing session log
 */
export function cloneSessionLog(handle: SessionLogHandle): SessionLogHandle {
  return HybridCoJSONCoreRN.cloneSessionLog(handle);
}

/**
 * Try to add transactions to the session log
 */
export function tryAddTransactions(
  handle: SessionLogHandle,
  transactionsJson: string[],
  newSignature: string,
  skipVerify: boolean,
): TransactionResult {
  return HybridCoJSONCoreRN.tryAddTransactions(
    handle,
    transactionsJson,
    newSignature,
    skipVerify,
  );
}

/**
 * Add a new private transaction
 */
export function addNewPrivateTransaction(
  handle: SessionLogHandle,
  changesJson: string,
  signerSecret: string,
  encryptionKey: string,
  keyId: string,
  madeAt: number,
): TransactionResult {
  return HybridCoJSONCoreRN.addNewPrivateTransaction(
    handle,
    changesJson,
    signerSecret,
    encryptionKey,
    keyId,
    madeAt,
  );
}

/**
 * Add a new trusting transaction
 */
export function addNewTrustingTransaction(
  handle: SessionLogHandle,
  changesJson: string,
  signerSecret: string,
  madeAt: number,
): TransactionResult {
  return HybridCoJSONCoreRN.addNewTrustingTransaction(
    handle,
    changesJson,
    signerSecret,
    madeAt,
  );
}

/**
 * Test expected hash after applying transactions
 */
export function testExpectedHashAfter(
  handle: SessionLogHandle,
  transactionsJson: string[],
): TransactionResult {
  return HybridCoJSONCoreRN.testExpectedHashAfter(handle, transactionsJson);
}

/**
 * Decrypt the next transaction changes JSON
 */
export function decryptNextTransactionChangesJson(
  handle: SessionLogHandle,
  txIndex: number,
  keySecret: ArrayBuffer,
): TransactionResult {
  return HybridCoJSONCoreRN.decryptNextTransactionChangesJson(
    handle,
    txIndex,
    keySecret,
  );
}

/**
 * Destroy a session log instance to free memory
 */
export function destroySessionLog(handle: SessionLogHandle): void {
  return HybridCoJSONCoreRN.destroySessionLog(handle);
}

// Optional: Create a SessionLog class wrapper for more object-oriented usage
export class SessionLog {
  private handle: SessionLogHandle;

  constructor(coId: string, sessionId: string, signerId: string) {
    this.handle = createSessionLog(coId, sessionId, signerId);
  }

  clone(): SessionLog {
    const newLog = Object.create(SessionLog.prototype);
    newLog.handle = cloneSessionLog(this.handle);
    return newLog;
  }

  tryAdd(
    transactionsJson: string[],
    newSignature: string,
    skipVerify: boolean = false,
  ): TransactionResult {
    return tryAddTransactions(
      this.handle,
      transactionsJson,
      newSignature,
      skipVerify,
    );
  }

  addNewPrivateTransaction(
    changesJson: string,
    signerSecret: string,
    encryptionKey: string,
    keyId: string,
    madeAt: number,
  ): TransactionResult {
    return addNewPrivateTransaction(
      this.handle,
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
  ): TransactionResult {
    return addNewTrustingTransaction(
      this.handle,
      changesJson,
      signerSecret,
      madeAt,
    );
  }

  testExpectedHashAfter(transactionsJson: string[]): TransactionResult {
    return testExpectedHashAfter(this.handle, transactionsJson);
  }

  decryptNextTransactionChangesJson(
    txIndex: number,
    keySecret: ArrayBuffer,
  ): TransactionResult {
    return decryptNextTransactionChangesJson(this.handle, txIndex, keySecret);
  }

  destroy(): void {
    destroySessionLog(this.handle);
  }

  // Getter for the handle (in case direct access is needed)
  get sessionHandle(): SessionLogHandle {
    return this.handle;
  }
}
