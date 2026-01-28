import { ControlledAccountOrAgent } from "../coValues/account.js";
import { TRANSACTION_CONFIG } from "../config.js";
import type {
  CryptoProvider,
  KeyID,
  KeySecret,
  SessionMapImpl,
  Signature,
  SignerID,
} from "../crypto/crypto.js";
import { isDeleteSessionID, RawCoID, SessionID } from "../ids.js";
import { parseJSON, stableStringify, Stringified } from "../jsonStringify.js";
import { JsonObject, JsonValue } from "../jsonValue.js";
import { Transaction, CoValueHeader } from "./verifiedState.js";
import {
  CoValueKnownState,
  KnownStateSessions,
  cloneKnownState,
} from "../knownState.js";

export type SessionLog = {
  signerID?: SignerID;
  transactions: Transaction[];
  lastSignature: Signature | undefined;
  signatureAfter: { [txIdx: number]: Signature | undefined };
  sessionID: SessionID;
};

/**
 * SessionMap wraps the Rust SessionMapImpl and provides a TypeScript-friendly interface.
 * All transaction storage and crypto operations are delegated to Rust.
 */
export class SessionMap {
  private readonly impl: SessionMapImpl;
  private readonly id: RawCoID;
  private readonly crypto: CryptoProvider;

  // Cache for SessionLog objects to avoid re-parsing on every access
  private sessionLogCache: Map<SessionID, SessionLog> = new Map();
  private sessionLogCacheValid: Map<SessionID, number> = new Map(); // txCount when cached

  // Cache for immutable known state (same reference until invalidated)
  private cachedImmutableKnownState: CoValueKnownState | undefined;
  private cachedImmutableKnownStateWithStreaming: CoValueKnownState | undefined;

  constructor(
    id: RawCoID,
    crypto: CryptoProvider,
    header: CoValueHeader,
    streamingKnownState?: KnownStateSessions,
  ) {
    this.id = id;
    this.crypto = crypto;

    // Create the Rust SessionMapImpl with the header and max tx size threshold
    const headerJson = stableStringify(header);
    this.impl = crypto.createSessionMap(
      id,
      headerJson,
      TRANSACTION_CONFIG.MAX_RECOMMENDED_TX_SIZE,
    );

    // Set streaming known state if provided
    if (streamingKnownState) {
      this.impl.setStreamingKnownState(JSON.stringify(streamingKnownState));
    }
  }

  // Private constructor for cloning
  private static fromImpl(
    id: RawCoID,
    crypto: CryptoProvider,
    impl: SessionMapImpl,
  ): SessionMap {
    const instance = Object.create(SessionMap.prototype) as SessionMap;
    (instance as any).id = id;
    (instance as any).crypto = crypto;
    (instance as any).impl = impl;
    (instance as any).sessionLogCache = new Map();
    (instance as any).sessionLogCacheValid = new Map();
    return instance;
  }

  markAsDeleted() {
    this.impl.markAsDeleted();
    this.invalidateCache();
  }

  get isDeleted(): boolean {
    return this.impl.isDeleted();
  }

  setStreamingKnownState(streamingKnownState: KnownStateSessions) {
    if (this.isDeleted) {
      return;
    }
    this.impl.setStreamingKnownState(JSON.stringify(streamingKnownState));
  }

  get knownState(): CoValueKnownState {
    // Native object returned directly from Rust
    return this.impl.getKnownState() as CoValueKnownState;
  }

  get knownStateWithStreaming(): CoValueKnownState | undefined {
    // Native object returned directly from Rust
    const result = this.impl.getKnownStateWithStreaming();
    if (!result || result === undefined) return undefined;
    return result as CoValueKnownState;
  }

  getImmutableKnownState(): CoValueKnownState {
    if (!this.cachedImmutableKnownState) {
      this.cachedImmutableKnownState = cloneKnownState(this.knownState);
    }
    return this.cachedImmutableKnownState;
  }

  getImmutableKnownStateWithStreaming(): CoValueKnownState {
    const withStreaming = this.knownStateWithStreaming;
    if (!withStreaming) {
      return this.getImmutableKnownState();
    }
    if (!this.cachedImmutableKnownStateWithStreaming) {
      this.cachedImmutableKnownStateWithStreaming =
        cloneKnownState(withStreaming);
    }
    return this.cachedImmutableKnownStateWithStreaming;
  }

  get(sessionID: SessionID): SessionLog | undefined {
    const txCount = this.impl.getTransactionCount(sessionID);
    if (txCount === -1) {
      return undefined;
    }
    return this.getSessionLog(sessionID);
  }

  getTransactionsCount(sessionID: SessionID): number | undefined {
    const txCount = this.impl.getTransactionCount(sessionID);
    if (txCount === -1) {
      return undefined;
    }
    return txCount;
  }

  private invalidateCache() {
    this.sessionLogCache.clear();
    this.sessionLogCacheValid.clear();
    // Invalidate immutable known state caches
    this.cachedImmutableKnownState = undefined;
    this.cachedImmutableKnownStateWithStreaming = undefined;
  }

  /**
   * Update the session log cache directly when adding transactions.
   * This avoids round-trips to Rust on subsequent reads.
   */
  private updateSessionLogCache(
    sessionID: SessionID,
    signerID: SignerID | undefined,
    newTransactions: Transaction[],
    newSignature: Signature,
  ) {
    const cached = this.sessionLogCache.get(sessionID);
    const currentTxCount = this.impl.getTransactionCount(sessionID);

    if (cached) {
      // Append to existing cache
      cached.transactions.push(...newTransactions);
      cached.lastSignature = newSignature;
      if (signerID) {
        cached.signerID = signerID;
      }
      // Check if we need to update signatureAfter (in-between signature)
      const lastCheckpoint = this.impl.getLastSignatureCheckpoint(sessionID);
      if (
        lastCheckpoint !== undefined &&
        lastCheckpoint !== null &&
        lastCheckpoint >= 0
      ) {
        const sig = this.impl.getSignatureAfter(sessionID, lastCheckpoint);
        if (sig) {
          cached.signatureAfter[lastCheckpoint] = sig as Signature;
        }
      }
      this.sessionLogCacheValid.set(sessionID, currentTxCount);
    } else {
      // Create new cache entry
      const sessionLog: SessionLog = {
        signerID,
        transactions: [...newTransactions],
        lastSignature: newSignature,
        signatureAfter: {},
        sessionID,
      };
      // Check for in-between signature
      const lastCheckpoint = this.impl.getLastSignatureCheckpoint(sessionID);
      if (
        lastCheckpoint !== undefined &&
        lastCheckpoint !== null &&
        lastCheckpoint >= 0
      ) {
        const sig = this.impl.getSignatureAfter(sessionID, lastCheckpoint);
        if (sig) {
          sessionLog.signatureAfter[lastCheckpoint] = sig as Signature;
        }
      }
      this.sessionLogCache.set(sessionID, sessionLog);
      this.sessionLogCacheValid.set(sessionID, currentTxCount);
    }
  }

  private getSessionLog(sessionID: SessionID): SessionLog {
    const currentTxCount = this.impl.getTransactionCount(sessionID);
    const cachedTxCount = this.sessionLogCacheValid.get(sessionID);

    // Check if cache is valid
    if (cachedTxCount === currentTxCount) {
      const cached = this.sessionLogCache.get(sessionID);
      if (cached) return cached;
    }

    // Build fresh SessionLog from Rust data
    const transactions: Transaction[] = [];
    const signatureAfter: { [txIdx: number]: Signature | undefined } = {};

    // Fetch all transactions
    if (currentTxCount > 0) {
      // getSessionTransactions returns array of JSON strings directly
      const txStrings = this.impl.getSessionTransactions(sessionID, 0);
      if (txStrings) {
        for (const txStr of txStrings) {
          transactions.push(JSON.parse(txStr) as Transaction);
        }
      }
    }

    // Build signatureAfter map
    const lastCheckpoint = this.impl.getLastSignatureCheckpoint(sessionID);
    if (
      lastCheckpoint !== undefined &&
      lastCheckpoint !== null &&
      lastCheckpoint >= 0
    ) {
      // We need to find all checkpoints - iterate from 0 to lastCheckpoint
      for (let i = 0; i <= lastCheckpoint; i++) {
        const sig = this.impl.getSignatureAfter(sessionID, i);
        if (sig) {
          signatureAfter[i] = sig as Signature;
        }
      }
    }

    const lastSignature = this.impl.getLastSignature(sessionID) as
      | Signature
      | undefined;

    const sessionLog: SessionLog = {
      signerID: undefined, // We don't track this in Rust currently
      transactions,
      lastSignature,
      signatureAfter,
      sessionID,
    };

    // Cache the result
    this.sessionLogCache.set(sessionID, sessionLog);
    this.sessionLogCacheValid.set(sessionID, currentTxCount);

    return sessionLog;
  }

  addTransaction(
    sessionID: SessionID,
    signerID: SignerID | undefined,
    newTransactions: Transaction[],
    newSignature: Signature,
    skipVerify: boolean = false,
  ) {
    if (this.isDeleted && !isDeleteSessionID(sessionID)) {
      throw new Error("Cannot add transactions to a deleted coValue");
    }

    // Convert transactions to JSON array
    const txJson = JSON.stringify(newTransactions);

    this.impl.addTransactions(
      sessionID,
      signerID,
      txJson,
      newSignature,
      skipVerify,
    );

    // Update cache directly instead of invalidating
    this.updateSessionLogCache(
      sessionID,
      signerID,
      newTransactions,
      newSignature,
    );
    // Invalidate immutable known state caches
    this.cachedImmutableKnownState = undefined;
    this.cachedImmutableKnownStateWithStreaming = undefined;
  }

  makeNewPrivateTransaction(
    sessionID: SessionID,
    signerAgent: ControlledAccountOrAgent,
    changes: JsonValue[],
    keyID: KeyID,
    keySecret: KeySecret,
    meta: JsonObject | undefined,
    madeAt: number,
  ): { signature: Signature; transaction: Transaction } {
    if (this.isDeleted) {
      throw new Error(
        "Cannot make new private transaction on a deleted coValue",
      );
    }

    const changesJson = JSON.stringify(changes);
    const metaJson = meta ? JSON.stringify(meta) : undefined;
    const signerSecret = signerAgent.currentSignerSecret();

    const resultJson = this.impl.makeNewPrivateTransaction(
      sessionID,
      signerSecret,
      changesJson,
      keyID,
      keySecret,
      metaJson,
      madeAt,
    );

    const result = JSON.parse(resultJson) as {
      signature: string;
      transaction: Transaction;
    };

    const signature = result.signature as Signature;

    // Update cache directly instead of invalidating
    this.updateSessionLogCache(
      sessionID,
      signerAgent.id as unknown as SignerID,
      [result.transaction],
      signature,
    );
    // Invalidate immutable known state caches
    this.cachedImmutableKnownState = undefined;
    this.cachedImmutableKnownStateWithStreaming = undefined;

    return {
      signature,
      transaction: result.transaction,
    };
  }

  makeNewTrustingTransaction(
    sessionID: SessionID,
    signerAgent: ControlledAccountOrAgent,
    changes: JsonValue[],
    meta: JsonObject | undefined,
    madeAt: number,
  ): { signature: Signature; transaction: Transaction } {
    if (this.isDeleted) {
      throw new Error(
        "Cannot make new trusting transaction on a deleted coValue",
      );
    }

    const changesJson = JSON.stringify(changes);
    const metaJson = meta ? JSON.stringify(meta) : undefined;
    const signerSecret = signerAgent.currentSignerSecret();

    const resultJson = this.impl.makeNewTrustingTransaction(
      sessionID,
      signerSecret,
      changesJson,
      metaJson,
      madeAt,
    );

    const result = JSON.parse(resultJson) as {
      signature: string;
      transaction: Transaction;
    };

    const signature = result.signature as Signature;

    // Update cache directly instead of invalidating
    this.updateSessionLogCache(
      sessionID,
      signerAgent.id as unknown as SignerID,
      [result.transaction],
      signature,
    );
    // Invalidate immutable known state caches
    this.cachedImmutableKnownState = undefined;
    this.cachedImmutableKnownStateWithStreaming = undefined;

    return {
      signature,
      transaction: result.transaction,
    };
  }

  decryptTransaction(
    sessionID: SessionID,
    txIndex: number,
    keySecret: KeySecret,
  ): JsonValue[] | undefined {
    const decrypted = this.impl.decryptTransaction(
      sessionID,
      txIndex,
      keySecret,
    );
    if (!decrypted) {
      return undefined;
    }
    return parseJSON(decrypted as Stringified<JsonValue[]>);
  }

  decryptTransactionMeta(
    sessionID: SessionID,
    txIndex: number,
    keySecret: KeySecret,
  ): JsonObject | undefined {
    const sessionLog = this.get(sessionID);
    if (!sessionLog?.transactions[txIndex]?.meta) {
      return undefined;
    }
    const decrypted = this.impl.decryptTransactionMeta(
      sessionID,
      txIndex,
      keySecret,
    );
    if (!decrypted) {
      return undefined;
    }
    return parseJSON(decrypted as Stringified<JsonObject>);
  }

  get size(): number {
    const sessionIds = JSON.parse(this.impl.getSessionIds()) as string[];
    return sessionIds.length;
  }

  get sessions(): Map<SessionID, SessionLog> {
    // Build a Map from all sessions
    const map = new Map<SessionID, SessionLog>();
    const sessionIds = JSON.parse(this.impl.getSessionIds()) as SessionID[];
    for (const sessionID of sessionIds) {
      map.set(sessionID, this.getSessionLog(sessionID));
    }
    return map;
  }

  *entries(): IterableIterator<[SessionID, SessionLog]> {
    const sessionIds = JSON.parse(this.impl.getSessionIds()) as SessionID[];
    for (const sessionID of sessionIds) {
      yield [sessionID, this.getSessionLog(sessionID)];
    }
  }

  *values(): IterableIterator<SessionLog> {
    const sessionIds = JSON.parse(this.impl.getSessionIds()) as SessionID[];
    for (const sessionID of sessionIds) {
      yield this.getSessionLog(sessionID);
    }
  }

  *keys(): IterableIterator<SessionID> {
    const sessionIds = JSON.parse(this.impl.getSessionIds()) as SessionID[];
    for (const sessionID of sessionIds) {
      yield sessionID;
    }
  }

  clone(): SessionMap {
    // For cloning, we need to create a new SessionMap with the same state
    // Since Rust SessionMapImpl doesn't have a clone method exposed,
    // we need to recreate the state

    // Get header from Rust
    const headerJson = this.impl.getHeader();
    const header = JSON.parse(headerJson) as CoValueHeader;

    // Get streaming known state
    const knownStateWithStreaming = this.knownStateWithStreaming;
    let streamingKnownState: KnownStateSessions | undefined;
    if (knownStateWithStreaming) {
      // Calculate streaming state as difference between withStreaming and base
      const baseKnownState = this.knownState;
      streamingKnownState = {};
      for (const [sessionId, count] of Object.entries(
        knownStateWithStreaming.sessions,
      )) {
        const baseCount = baseKnownState.sessions[sessionId as SessionID] ?? 0;
        if (count > baseCount) {
          streamingKnownState[sessionId as SessionID] = count;
        }
      }
    }

    // Create new SessionMap
    const clone = new SessionMap(
      this.id,
      this.crypto,
      header,
      streamingKnownState,
    );

    // Copy all sessions with their transactions
    const sessionIds = JSON.parse(this.impl.getSessionIds()) as SessionID[];
    for (const sessionID of sessionIds) {
      const txCount = this.impl.getTransactionCount(sessionID);
      if (txCount > 0) {
        // getSessionTransactions returns string[] directly
        const txStrings = this.impl.getSessionTransactions(sessionID, 0);
        if (txStrings) {
          const transactions = txStrings.map(
            (s) => JSON.parse(s) as Transaction,
          );
          const lastSignature = this.impl.getLastSignature(sessionID) as
            | Signature
            | undefined;
          if (lastSignature) {
            clone.impl.addTransactions(
              sessionID,
              undefined, // signerID not tracked
              JSON.stringify(transactions),
              lastSignature,
              true, // skip verify since we're cloning valid data
            );
          }
        }
      }
    }

    // Copy deletion state
    if (this.isDeleted) {
      clone.markAsDeleted();
    }

    return clone;
  }
}
