import type { HybridObject } from "react-native-nitro-modules";

// Types corresponding to Rust FFI structs
export interface SessionLogHandle {
  id: number;
}

export interface TransactionResult {
  success: boolean;
  result: string;
  error: string;
}

export interface MakeTransactionResult {
  signature: string;
  transaction: string;
}

export interface U8VecResult {
  success: boolean;
  data: ArrayBuffer;
  error: string;
}

export interface CoJSONCoreRN
  extends HybridObject<{ ios: "c++"; android: "c++" }> {
  // Create a new session log instance
  createSessionLog(
    coId: string,
    sessionId: string,
    signerId: string,
  ): SessionLogHandle;

  // Clone an existing session log
  cloneSessionLog(handle: SessionLogHandle): SessionLogHandle;

  // Try to add transactions to the session log
  tryAddTransactions(
    handle: SessionLogHandle,
    transactionsJson: string[],
    newSignature: string,
    skipVerify: boolean,
  ): TransactionResult;

  // Add a new private transaction
  addNewPrivateTransaction(
    handle: SessionLogHandle,
    changesJson: string,
    signerSecret: string,
    encryptionKey: string,
    keyId: string,
    madeAt: number,
    meta: string,
  ): TransactionResult;

  // Add a new trusting transaction
  addNewTrustingTransaction(
    handle: SessionLogHandle,
    changesJson: string,
    signerSecret: string,
    madeAt: number,
    meta: string,
  ): TransactionResult;

  // Test expected hash after applying transactions
  testExpectedHashAfter(
    handle: SessionLogHandle,
    transactionsJson: string[],
  ): TransactionResult;

  // Decrypt the next transaction changes JSON
  decryptNextTransactionChangesJson(
    handle: SessionLogHandle,
    txIndex: number,
    keySecret: ArrayBuffer,
  ): TransactionResult;

  // Destroy a session log instance to free memory
  destroySessionLog(handle: SessionLogHandle): void;

  // Seal a message
  sealMessage(
    message: ArrayBuffer,
    senderSecret: string,
    recipientId: string,
    nonceMaterial: ArrayBuffer,
  ): U8VecResult;

  // Unseal a message
  unsealMessage(
    sealedMessage: ArrayBuffer,
    recipientSecret: string,
    senderId: string,
    nonceMaterial: ArrayBuffer,
  ): U8VecResult;
}
