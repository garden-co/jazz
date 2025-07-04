import { Result, err, ok } from "neverthrow";
import { AnyRawCoValue } from "../coValue.js";
import {
  CryptoProvider,
  Encrypted,
  Hash,
  KeyID,
  Signature,
  SignerID,
  StreamingHash,
} from "../crypto/crypto.js";
import { RawCoID, SessionID, TransactionID } from "../ids.js";
import { Stringified } from "../jsonStringify.js";
import { JsonObject, JsonValue } from "../jsonValue.js";
import { PermissionsDef as RulesetDef } from "../permissions.js";
import { getPriorityFromHeader } from "../priority.js";
import { CoValueKnownState, NewContentMessage } from "../sync.js";
import {
  InvalidHashError,
  InvalidSignatureError,
  MAX_RECOMMENDED_TX_SIZE,
} from "./coValueCore.js";
import { TryAddTransactionsError } from "./coValueCore.js";

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
};

export type TrustingTransaction = {
  privacy: "trusting";
  madeAt: number;
  changes: Stringified<JsonValue[]>;
};

export type Transaction = PrivateTransaction | TrustingTransaction;

type SessionLog = {
  readonly transactions: Transaction[];
  lastHash?: Hash;
  streamingHash: StreamingHash;
  readonly signatureAfter: { [txIdx: number]: Signature | undefined };
  lastSignature: Signature;
};

export type ValidatedSessions = Map<SessionID, SessionLog>;

export class VerifiedState {
  readonly id: RawCoID;
  readonly crypto: CryptoProvider;
  readonly header: CoValueHeader;
  readonly sessions: ValidatedSessions;
  private _cachedKnownState?: CoValueKnownState;
  private _cachedNewContentSinceEmpty: NewContentMessage[] | undefined;
  private streamingKnownState?: CoValueKnownState["sessions"];

  constructor(
    id: RawCoID,
    crypto: CryptoProvider,
    header: CoValueHeader,
    sessions: ValidatedSessions,
    streamingKnownState?: CoValueKnownState["sessions"],
  ) {
    this.id = id;
    this.crypto = crypto;
    this.header = header;
    this.sessions = sessions;
    this.streamingKnownState = streamingKnownState;
  }

  clone(): VerifiedState {
    // do a deep clone, including the sessions
    const clonedSessions = new Map();
    for (let [sessionID, sessionLog] of this.sessions) {
      clonedSessions.set(sessionID, {
        lastSignature: sessionLog.lastSignature,
        lastHash: sessionLog.lastHash,
        streamingHash: sessionLog.streamingHash.clone(),
        signatureAfter: { ...sessionLog.signatureAfter },
        transactions: sessionLog.transactions.slice(),
      } satisfies SessionLog);
    }
    return new VerifiedState(
      this.id,
      this.crypto,
      this.header,
      clonedSessions,
      this.streamingKnownState,
    );
  }

  tryAddTransactions(
    sessionID: SessionID,
    signerID: SignerID,
    newTransactions: Transaction[],
    givenExpectedNewHash: Hash | undefined,
    newSignature: Signature,
    skipVerify: boolean = false,
    givenNewStreamingHash?: StreamingHash,
  ): Result<true, TryAddTransactionsError> {
    if (skipVerify === true && givenNewStreamingHash && givenExpectedNewHash) {
      this.doAddTransactions(
        sessionID,
        newTransactions,
        newSignature,
        givenExpectedNewHash,
        givenNewStreamingHash,
      );
    } else {
      const { expectedNewHash, newStreamingHash } = this.expectedNewHashAfter(
        sessionID,
        newTransactions,
      );

      if (givenExpectedNewHash && givenExpectedNewHash !== expectedNewHash) {
        return err({
          type: "InvalidHash",
          id: this.id,
          expectedNewHash,
          givenExpectedNewHash,
        } satisfies InvalidHashError);
      }

      if (!this.crypto.verify(newSignature, expectedNewHash, signerID)) {
        return err({
          type: "InvalidSignature",
          id: this.id,
          newSignature,
          sessionID,
          signerID,
        } satisfies InvalidSignatureError);
      }

      this.doAddTransactions(
        sessionID,
        newTransactions,
        newSignature,
        expectedNewHash,
        newStreamingHash,
      );
    }

    return ok(true as const);
  }

  private doAddTransactions(
    sessionID: SessionID,
    newTransactions: Transaction[],
    newSignature: Signature,
    expectedNewHash: Hash,
    newStreamingHash: StreamingHash,
  ) {
    const transactions = this.sessions.get(sessionID)?.transactions ?? [];

    for (const tx of newTransactions) {
      transactions.push(tx);
    }

    const signatureAfter = this.sessions.get(sessionID)?.signatureAfter ?? {};

    const lastInbetweenSignatureIdx = Object.keys(signatureAfter).reduce(
      (max, idx) => (parseInt(idx) > max ? parseInt(idx) : max),
      -1,
    );

    const sizeOfTxsSinceLastInbetweenSignature = transactions
      .slice(lastInbetweenSignatureIdx + 1)
      .reduce(
        (sum, tx) =>
          sum +
          (tx.privacy === "private"
            ? tx.encryptedChanges.length
            : tx.changes.length),
        0,
      );

    if (sizeOfTxsSinceLastInbetweenSignature > MAX_RECOMMENDED_TX_SIZE) {
      signatureAfter[transactions.length - 1] = newSignature;
    }

    this.sessions.set(sessionID, {
      transactions,
      lastHash: expectedNewHash,
      streamingHash: newStreamingHash,
      lastSignature: newSignature,
      signatureAfter: signatureAfter,
    });

    this._cachedNewContentSinceEmpty = undefined;
    this._cachedKnownState = undefined;
  }

  expectedNewHashAfter(
    sessionID: SessionID,
    newTransactions: Transaction[],
  ): { expectedNewHash: Hash; newStreamingHash: StreamingHash } {
    const streamingHash =
      this.sessions.get(sessionID)?.streamingHash.clone() ??
      new StreamingHash(this.crypto);

    for (const transaction of newTransactions) {
      streamingHash.update(transaction);
    }

    return {
      expectedNewHash: streamingHash.digest(),
      newStreamingHash: streamingHash,
    };
  }

  newContentSince(
    knownState: CoValueKnownState | undefined,
  ): NewContentMessage[] | undefined {
    const isKnownStateEmpty = !knownState?.header && !knownState?.sessions;

    if (isKnownStateEmpty && this._cachedNewContentSinceEmpty) {
      return this._cachedNewContentSinceEmpty;
    }

    let currentPiece: NewContentMessage = {
      action: "content",
      id: this.id,
      header: knownState?.header ? undefined : this.header,
      priority: getPriorityFromHeader(this.header),
      new: {},
    };

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
          pieceSize +=
            tx.privacy === "private"
              ? tx.encryptedChanges.length
              : tx.changes.length;
        }

        if (pieceSize >= MAX_RECOMMENDED_TX_SIZE) {
          if (!currentPiece.streamingTarget) {
            currentPiece.streamingTarget =
              this.knownStateWithStreaming().sessions;
          }

          currentPiece = {
            action: "content",
            id: this.id,
            header: undefined,
            new: {},
            priority: getPriorityFromHeader(this.header),
            streamingTarget: currentPiece.streamingTarget,
          };
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
