import { Result, err, ok } from "neverthrow";
import { AnyRawCoValue } from "../coValue.js";
import {
  createContentMessage,
  exceedsRecommendedSize,
  getTransactionSize,
} from "../coValueContentMessage.js";
import {
  CryptoProvider,
  Encrypted,
  KeyID,
  KeySecret,
  Signature,
  SignerID,
} from "../crypto/crypto.js";
import { RawCoID, SessionID, TransactionID } from "../ids.js";
import { Stringified } from "../jsonStringify.js";
import { JsonObject, JsonValue } from "../jsonValue.js";
import { PermissionsDef as RulesetDef } from "../permissions.js";
import { CoValueKnownState, NewContentMessage } from "../sync.js";
import { TryAddTransactionsError } from "./coValueCore.js";
import { SessionLog, SessionMap } from "./SessionMap.js";
import { ControlledAccountOrAgent } from "../coValues/account.js";
import { logger } from "../logger.js";

export type CoValueHeader = {
  type: AnyRawCoValue["type"];
  ruleset: RulesetDef;
  meta: JsonObject | null;
} & CoValueUniqueness;

export type CoValueUniqueness = {
  uniqueness: JsonValue;
  createdAt?: `2${string}` | null;
};

export type PrivateTransaction = {
  privacy: "private";
  madeAt: number;
  keyUsed: KeyID;
  encryptedChanges: Encrypted<JsonValue[], { in: RawCoID; tx: TransactionID }>;
  meta?: Encrypted<JsonObject, { in: RawCoID; tx: TransactionID }>;
};
export type TrustingTransaction = {
  privacy: "trusting";
  madeAt: number;
  changes: Stringified<JsonValue[]>;
  meta?: Stringified<JsonObject>;
};

export type Transaction = PrivateTransaction | TrustingTransaction;

export class VerifiedState {
  readonly id: RawCoID;
  readonly crypto: CryptoProvider;
  readonly header: CoValueHeader;
  readonly sessions: SessionMap;
  private _cachedKnownState?: CoValueKnownState;
  private _cachedNewContentSinceEmpty: NewContentMessage[] | undefined;
  private streamingKnownState?: CoValueKnownState["sessions"];
  public lastAccessed: number | undefined;
  public branchSourceId?: RawCoID;
  public branchName?: string;

  constructor(
    id: RawCoID,
    crypto: CryptoProvider,
    header: CoValueHeader,
    sessions?: SessionMap,
    streamingKnownState?: CoValueKnownState["sessions"],
  ) {
    this.id = id;
    this.crypto = crypto;
    this.header = header;
    this.sessions = sessions ?? new SessionMap(id, crypto);
    this.streamingKnownState = streamingKnownState
      ? { ...streamingKnownState }
      : undefined;
    this.branchSourceId = header.meta?.source as RawCoID | undefined;
    this.branchName = header.meta?.branch as string | undefined;
  }

  clone(): VerifiedState {
    return new VerifiedState(
      this.id,
      this.crypto,
      this.header,
      this.sessions.clone(),
      this.streamingKnownState ? { ...this.streamingKnownState } : undefined,
    );
  }

  tryAddTransactions(
    sessionID: SessionID,
    signerID: SignerID | undefined,
    newTransactions: Transaction[],
    newSignature: Signature,
    skipVerify: boolean = false,
  ): Result<true, TryAddTransactionsError> {
    const result = this.sessions.addTransaction(
      sessionID,
      signerID,
      newTransactions,
      newSignature,
      skipVerify,
    );

    if (result.isOk()) {
      this._cachedNewContentSinceEmpty = undefined;
      this._cachedKnownState = undefined;
    }

    return result;
  }

  makeNewTrustingTransaction(
    sessionID: SessionID,
    signerAgent: ControlledAccountOrAgent,
    changes: JsonValue[],
    meta: JsonObject | undefined,
    madeAt: number,
  ) {
    const result = this.sessions.makeNewTrustingTransaction(
      sessionID,
      signerAgent,
      changes,
      meta,
      madeAt,
    );

    this._cachedNewContentSinceEmpty = undefined;
    this._cachedKnownState = undefined;

    return result;
  }

  makeNewPrivateTransaction(
    sessionID: SessionID,
    signerAgent: ControlledAccountOrAgent,
    changes: JsonValue[],
    keyID: KeyID,
    keySecret: KeySecret,
    meta: JsonObject | undefined,
    madeAt: number,
  ) {
    const result = this.sessions.makeNewPrivateTransaction(
      sessionID,
      signerAgent,
      changes,
      keyID,
      keySecret,
      meta,
      madeAt,
    );

    this._cachedNewContentSinceEmpty = undefined;
    this._cachedKnownState = undefined;

    return result;
  }

  getLastSignatureCheckpoint(sessionID: SessionID): number {
    const sessionLog = this.sessions.get(sessionID);

    if (!sessionLog?.signatureAfter) return -1;

    return Object.keys(sessionLog.signatureAfter).reduce(
      (max, idx) => Math.max(max, parseInt(idx)),
      -1,
    );
  }

  newContentSince(
    knownState: CoValueKnownState | undefined,
  ): NewContentMessage[] | undefined {
    const isKnownStateEmpty = !knownState?.header && !knownState?.sessions;

    if (isKnownStateEmpty && this._cachedNewContentSinceEmpty) {
      return this._cachedNewContentSinceEmpty;
    }

    let currentPiece: NewContentMessage = createContentMessage(
      this.id,
      this.header,
      !knownState?.header,
    );

    const pieces = [currentPiece];

    const sentState: CoValueKnownState["sessions"] = {};

    let pieceSize = 0;

    let sessionsTodoAgain: Set<SessionID> | undefined | "first" = "first";

    while (sessionsTodoAgain === "first" || sessionsTodoAgain?.size || 0 > 0) {
      if (sessionsTodoAgain === "first") {
        sessionsTodoAgain = undefined;
      }
      const sessionsTodo = sessionsTodoAgain ?? this.sessions.keys();

      for (const sessionIDKey of sessionsTodo) {
        const sessionID = sessionIDKey as SessionID;
        const log = this.sessions.get(sessionID)!;
        const knownStateForSessionID = knownState?.sessions[sessionID];
        const sentStateForSessionID = sentState[sessionID];
        const nextKnownSignatureIdx = getNextKnownSignatureIdx(
          log,
          knownStateForSessionID,
          sentStateForSessionID,
        );

        const firstNewTxIdx =
          sentStateForSessionID ?? knownStateForSessionID ?? 0;
        const afterLastNewTxIdx =
          nextKnownSignatureIdx === undefined
            ? log.transactions.length
            : nextKnownSignatureIdx + 1;

        const nNewTx = Math.max(0, afterLastNewTxIdx - firstNewTxIdx);

        if (nNewTx === 0) {
          sessionsTodoAgain?.delete(sessionID);
          continue;
        }

        if (afterLastNewTxIdx < log.transactions.length) {
          if (!sessionsTodoAgain) {
            sessionsTodoAgain = new Set();
          }
          sessionsTodoAgain.add(sessionID);
        }

        const oldPieceSize = pieceSize;
        for (let txIdx = firstNewTxIdx; txIdx < afterLastNewTxIdx; txIdx++) {
          const tx = log.transactions[txIdx]!;
          pieceSize += getTransactionSize(tx);
        }

        if (exceedsRecommendedSize(pieceSize)) {
          if (!currentPiece.expectContentUntil && pieces.length === 1) {
            currentPiece.expectContentUntil =
              this.knownStateWithStreaming().sessions;
          }

          currentPiece = createContentMessage(this.id, this.header, false);
          pieces.push(currentPiece);
          pieceSize = pieceSize - oldPieceSize;
        }

        let sessionEntry = currentPiece.new[sessionID];
        if (!sessionEntry) {
          sessionEntry = {
            after: sentStateForSessionID ?? knownStateForSessionID ?? 0,
            newTransactions: [],
            lastSignature: "WILL_BE_REPLACED" as Signature,
          };
          currentPiece.new[sessionID] = sessionEntry;
        }

        for (let txIdx = firstNewTxIdx; txIdx < afterLastNewTxIdx; txIdx++) {
          const tx = log.transactions[txIdx]!;
          sessionEntry.newTransactions.push(tx);
        }

        sessionEntry.lastSignature =
          nextKnownSignatureIdx === undefined
            ? log.lastSignature!
            : log.signatureAfter[nextKnownSignatureIdx]!;

        sentState[sessionID] =
          (sentStateForSessionID ?? knownStateForSessionID ?? 0) + nNewTx;
      }
    }

    const piecesWithContent = pieces.filter(
      (piece) => Object.keys(piece.new).length > 0 || piece.header,
    );

    if (piecesWithContent.length === 0) {
      return undefined;
    }

    if (isKnownStateEmpty) {
      this._cachedNewContentSinceEmpty = piecesWithContent;
    }

    return piecesWithContent;
  }

  /**
   * Returns the known state considering the known state of the streaming source
   *
   * Used to correctly manage the content & subscriptions during the content streaming process
   */
  knownStateWithStreaming(): CoValueKnownState {
    const knownState = this.knownState();

    if (this.streamingKnownState) {
      const newSessions: CoValueKnownState["sessions"] = {};
      const entries = Object.entries(this.streamingKnownState);

      for (const [sessionID, txs] of entries) {
        newSessions[sessionID as SessionID] = txs;
        if ((knownState.sessions[sessionID as SessionID] ?? 0) < txs) {
          newSessions[sessionID as SessionID] = txs;
        } else {
          newSessions[sessionID as SessionID] = txs;
          delete this.streamingKnownState[sessionID as SessionID];
        }
      }

      if (Object.keys(this.streamingKnownState).length === 0) {
        this.streamingKnownState = undefined;
        return knownState;
      } else {
        return {
          id: knownState.id,
          header: knownState.header,
          sessions: newSessions,
        };
      }
    }

    return knownState;
  }

  isStreaming(): boolean {
    // Call knownStateWithStreaming to delete the streamingKnownState when it matches the current knownState
    this.knownStateWithStreaming();

    return this.streamingKnownState !== undefined;
  }

  knownState(): CoValueKnownState {
    if (this._cachedKnownState) {
      return this._cachedKnownState;
    } else {
      const knownState = this.knownStateUncached();
      this._cachedKnownState = knownState;
      return knownState;
    }
  }

  /** @internal */
  knownStateUncached(): CoValueKnownState {
    const sessions: CoValueKnownState["sessions"] = {};

    for (const [sessionID, sessionLog] of this.sessions.entries()) {
      sessions[sessionID] = sessionLog.transactions.length;
    }

    return {
      id: this.id,
      header: true,
      sessions,
    };
  }

  decryptTransaction(
    sessionID: SessionID,
    txIndex: number,
    keySecret: KeySecret,
  ): JsonValue[] | undefined {
    return this.sessions.decryptTransaction(sessionID, txIndex, keySecret);
  }

  decryptTransactionMeta(
    sessionID: SessionID,
    txIndex: number,
    keySecret: KeySecret,
  ): JsonObject | undefined {
    return this.sessions.decryptTransactionMeta(sessionID, txIndex, keySecret);
  }
}

function getNextKnownSignatureIdx(
  log: SessionLog,
  knownStateForSessionID?: number,
  sentStateForSessionID?: number,
) {
  return Object.keys(log.signatureAfter)
    .map(Number)
    .sort((a, b) => a - b)
    .find(
      (idx) => idx >= (sentStateForSessionID ?? knownStateForSessionID ?? -1),
    );
}
