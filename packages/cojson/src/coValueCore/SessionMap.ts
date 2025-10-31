import { Result, err, ok } from "neverthrow";
import { ControlledAccountOrAgent } from "../coValues/account.js";
import type {
  CryptoProvider,
  KeyID,
  KeySecret,
  SessionLogImpl,
  Signature,
  SignerID,
} from "../crypto/crypto.js";
import { RawCoID, SessionID } from "../ids.js";
import { parseJSON, Stringified } from "../jsonStringify.js";
import { JsonObject, JsonValue } from "../jsonValue.js";
import { TryAddTransactionsError } from "./coValueCore.js";
import { Transaction } from "./verifiedState.js";
import { exceedsRecommendedSize } from "../coValueContentMessage.js";
import {
  CoValueKnownState,
  KnownStateSessions,
  updateSessionCounter,
  cloneKnownState,
  combineKnownStateSessions,
  isKnownStateSubsetOf,
  getKnownStateToSend,
} from "../knownState.js";

export type SessionLog = {
  signerID?: SignerID;
  impl: SessionLogImpl;
  transactions: Transaction[];
  lastSignature: Signature | undefined;
  signatureAfter: { [txIdx: number]: Signature | undefined };
  txSizeSinceLastInbetweenSignature: number;
  sessionID: SessionID;
};

export class SessionMap {
  sessions: Map<SessionID, SessionLog> = new Map();

  // Known state related properies, mutated when adding transactions to the session map
  knownState: CoValueKnownState;
  knownStateWithStreaming: CoValueKnownState | undefined;
  streamingKnownState?: KnownStateSessions;

  constructor(
    private readonly id: RawCoID,
    private readonly crypto: CryptoProvider,
    streamingKnownState?: KnownStateSessions,
  ) {
    this.knownState = { id: this.id, header: true, sessions: {} };
    if (streamingKnownState) {
      this.streamingKnownState = { ...streamingKnownState };
      this.knownStateWithStreaming = {
        id: this.id,
        header: true,
        sessions: { ...streamingKnownState },
      };
    }
  }

  setStreamingKnownState(streamingKnownState: KnownStateSessions) {
    // if the streaming known state is a subset of the current known state, we can skip the update
    if (isKnownStateSubsetOf(streamingKnownState, this.knownState.sessions)) {
      return;
    }

    const actualStreamingKnownState = getKnownStateToSend(
      streamingKnownState,
      this.knownState.sessions,
    );

    if (this.streamingKnownState) {
      combineKnownStateSessions(
        this.streamingKnownState,
        actualStreamingKnownState,
      );
    } else {
      this.streamingKnownState = actualStreamingKnownState;
    }

    if (!this.knownStateWithStreaming) {
      this.knownStateWithStreaming = cloneKnownState(this.knownState);
    }

    combineKnownStateSessions(
      this.knownStateWithStreaming.sessions,
      actualStreamingKnownState,
    );
  }

  get(sessionID: SessionID): SessionLog | undefined {
    return this.sessions.get(sessionID);
  }

  private getOrCreateSessionLog(
    sessionID: SessionID,
    signerID?: SignerID,
  ): SessionLog {
    let sessionLog = this.sessions.get(sessionID);
    if (!sessionLog) {
      sessionLog = {
        signerID,
        impl: this.crypto.createSessionLog(this.id, sessionID, signerID),
        transactions: [],
        lastSignature: undefined,
        signatureAfter: {},
        txSizeSinceLastInbetweenSignature: 0,
        sessionID,
      };
      this.sessions.set(sessionID, sessionLog);
    }

    return sessionLog;
  }

  signerID: SignerID | undefined;
  addTransaction(
    sessionID: SessionID,
    signerID: SignerID | undefined,
    newTransactions: Transaction[],
    newSignature: Signature,
    skipVerify: boolean = false,
  ): Result<true, TryAddTransactionsError> {
    const sessionLog = this.getOrCreateSessionLog(sessionID, signerID);

    try {
      sessionLog.impl.tryAdd(newTransactions, newSignature, skipVerify);

      this.addTransactionsToJsLog(sessionLog, newTransactions, newSignature);

      return ok(true as const);
    } catch (e) {
      return err({
        type: "InvalidSignature",
        id: this.id,
        sessionID,
        newSignature,
        signerID,
      } satisfies TryAddTransactionsError);
    }
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
    const sessionLog = this.getOrCreateSessionLog(
      sessionID,
      signerAgent.currentSignerID(),
    );

    const result = sessionLog.impl.addNewPrivateTransaction(
      signerAgent,
      changes,
      keyID,
      keySecret,
      madeAt,
      meta,
    );

    this.addTransactionsToJsLog(
      sessionLog,
      [result.transaction],
      result.signature,
    );

    return result;
  }

  makeNewTrustingTransaction(
    sessionID: SessionID,
    signerAgent: ControlledAccountOrAgent,
    changes: JsonValue[],
    meta: JsonObject | undefined,
    madeAt: number,
  ): { signature: Signature; transaction: Transaction } {
    const sessionLog = this.getOrCreateSessionLog(
      sessionID,
      signerAgent.currentSignerID(),
    );

    const result = sessionLog.impl.addNewTrustingTransaction(
      signerAgent,
      changes,
      madeAt,
      meta,
    );

    this.addTransactionsToJsLog(
      sessionLog,
      [result.transaction],
      result.signature,
    );

    return result;
  }

  private addTransactionsToJsLog(
    sessionLog: SessionLog,
    newTransactions: Transaction[],
    signature: Signature,
  ) {
    for (const tx of newTransactions) {
      sessionLog.transactions.push(tx);
    }
    sessionLog.lastSignature = signature;

    sessionLog.txSizeSinceLastInbetweenSignature += newTransactions.reduce(
      (sum, tx) =>
        sum +
        (tx.privacy === "private"
          ? tx.encryptedChanges.length
          : tx.changes.length),
      0,
    );

    const transactionsCount = sessionLog.transactions.length;

    if (exceedsRecommendedSize(sessionLog.txSizeSinceLastInbetweenSignature)) {
      sessionLog.signatureAfter[transactionsCount - 1] = signature;
      sessionLog.txSizeSinceLastInbetweenSignature = 0;
    }

    // Update the known state with the new transactions count
    updateSessionCounter(
      this.knownState.sessions,
      sessionLog.sessionID,
      transactionsCount,
    );

    // Check if the updated session matched the streaming state
    // If so, we can delete the session from the streaming state to mark it as synced
    if (this.streamingKnownState) {
      const streamingCount = this.streamingKnownState[sessionLog.sessionID];
      if (streamingCount && streamingCount <= transactionsCount) {
        delete this.streamingKnownState[sessionLog.sessionID];

        if (Object.keys(this.streamingKnownState).length === 0) {
          // Mark the streaming as done by deleting the streaming statuses
          this.streamingKnownState = undefined;
          this.knownStateWithStreaming = undefined;
        }
      }
    }

    if (this.knownStateWithStreaming) {
      // Update the streaming known state with the new transactions count
      updateSessionCounter(
        this.knownStateWithStreaming.sessions,
        sessionLog.sessionID,
        transactionsCount,
      );
    }
  }

  decryptTransaction(
    sessionID: SessionID,
    txIndex: number,
    keySecret: KeySecret,
  ): JsonValue[] | undefined {
    const sessionLog = this.sessions.get(sessionID);
    if (!sessionLog) {
      return undefined;
    }
    const decrypted = sessionLog.impl.decryptNextTransactionChangesJson(
      txIndex,
      keySecret,
    );
    if (!decrypted) {
      return undefined;
    }
    return parseJSON(decrypted as Stringified<JsonValue[] | undefined>);
  }

  decryptTransactionMeta(
    sessionID: SessionID,
    txIndex: number,
    keySecret: KeySecret,
  ): JsonObject | undefined {
    const sessionLog = this.sessions.get(sessionID);
    if (!sessionLog?.transactions[txIndex]?.meta) {
      return undefined;
    }
    const decrypted = sessionLog.impl.decryptNextTransactionMetaJson(
      txIndex,
      keySecret,
    );
    if (!decrypted) {
      return undefined;
    }
    return parseJSON(decrypted as Stringified<JsonObject | undefined>);
  }

  get size() {
    return this.sessions.size;
  }

  entries() {
    return this.sessions.entries();
  }

  values() {
    return this.sessions.values();
  }

  keys() {
    return this.sessions.keys();
  }

  clone(): SessionMap {
    const clone = new SessionMap(this.id, this.crypto);

    for (const [sessionID, sessionLog] of this.sessions) {
      clone.sessions.set(sessionID, {
        impl: sessionLog.impl.clone(),
        transactions: sessionLog.transactions.slice(),
        lastSignature: sessionLog.lastSignature,
        signatureAfter: { ...sessionLog.signatureAfter },
        txSizeSinceLastInbetweenSignature:
          sessionLog.txSizeSinceLastInbetweenSignature,
        signerID: sessionLog.signerID,
        sessionID,
      });
    }

    clone.streamingKnownState = this.streamingKnownState
      ? { ...this.streamingKnownState }
      : undefined;
    clone.knownState = cloneKnownState(this.knownState);
    clone.knownStateWithStreaming = this.knownStateWithStreaming
      ? cloneKnownState(this.knownStateWithStreaming)
      : undefined;

    return clone;
  }
}
