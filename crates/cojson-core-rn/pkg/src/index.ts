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
): string {
  const { success, result, error } = HybridCoJSONCoreRN.tryAddTransactions(
    handle,
    transactionsJson,
    newSignature,
    skipVerify,
  );
  if (!success) {
    throw new Error(error);
  }
  return result;
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
): string {
  const { success, result, error } =
    HybridCoJSONCoreRN.addNewPrivateTransaction(
      handle,
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

/**
 * Add a new trusting transaction
 */
export function addNewTrustingTransaction(
  handle: SessionLogHandle,
  changesJson: string,
  signerSecret: string,
  madeAt: number,
): string {
  const { success, result, error } =
    HybridCoJSONCoreRN.addNewTrustingTransaction(
      handle,
      changesJson,
      signerSecret,
      madeAt,
    );
  if (!success) {
    throw new Error(error);
  }
  return result;
}

/**
 * Test expected hash after applying transactions
 */
export function testExpectedHashAfter(
  handle: SessionLogHandle,
  transactionsJson: string[],
): string {
  const { success, result, error } = HybridCoJSONCoreRN.testExpectedHashAfter(
    handle,
    transactionsJson,
  );
  if (!success) {
    throw new Error(error);
  }
  return result;
}

/**
 * Decrypt the next transaction changes JSON
 */
export function decryptNextTransactionChangesJson(
  handle: SessionLogHandle,
  txIndex: number,
  keySecret: ArrayBuffer,
): string {
  const { success, result, error } =
    HybridCoJSONCoreRN.decryptNextTransactionChangesJson(
      handle,
      txIndex,
      keySecret,
    );
  if (!success) {
    throw new Error(error);
  }
  return result;
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
  ): string {
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
  ): string {
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
  ): string {
    return addNewTrustingTransaction(
      this.handle,
      changesJson,
      signerSecret,
      madeAt,
    );
  }

  testExpectedHashAfter(transactionsJson: string[]): string {
    return testExpectedHashAfter(this.handle, transactionsJson);
  }

  decryptNextTransactionChangesJson(
    txIndex: number,
    keySecret: ArrayBuffer,
  ): string {
    return decryptNextTransactionChangesJson(this.handle, txIndex, keySecret);
  }

  destroy(): void {
    destroySessionLog(this.handle);
  }

  // Alias for destroy() to match WASM API
  free(): void {
    this.destroy();
  }

  // Getter for the handle (in case direct access is needed)
  get sessionHandle(): SessionLogHandle {
    return this.handle;
  }
}
