import { AnyRawCoValue } from "../coValue.js";
import {
  createContentMessage,
  exceedsRecommendedSize,
  getTransactionSize,
  addTransactionToContentMessage,
  knownStateFromContent,
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
import { NewContentMessage } from "../sync.js";
import { TryAddTransactionsError } from "./coValueCore.js";
import { SessionMap } from "./SessionMap.js";
import { ControlledAccountOrAgent } from "../coValues/account.js";
import {
  CoValueKnownState,
  getKnownStateToSend,
  KnownStateSessions,
} from "../knownState.js";

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
  public lastAccessed: number | undefined;
  public branchSourceId?: RawCoID;
  public branchName?: string;

  constructor(
    id: RawCoID,
    crypto: CryptoProvider,
    header: CoValueHeader,
    sessions?: SessionMap,
  ) {
    this.id = id;
    this.crypto = crypto;
    this.header = header;
    this.sessions = sessions ?? new SessionMap(id, crypto);
    this.branchSourceId = header.meta?.source as RawCoID | undefined;
    this.branchName = header.meta?.branch as string | undefined;
  }

  clone(): VerifiedState {
    return new VerifiedState(
      this.id,
      this.crypto,
      this.header,
      this.sessions.clone(),
    );
  }

  tryAddTransactions(
    sessionID: SessionID,
    signerID: SignerID | undefined,
    newTransactions: Transaction[],
    newSignature: Signature,
    skipVerify: boolean = false,
  ) {
    this.sessions.addTransaction(
      sessionID,
      signerID,
      newTransactions,
      newSignature,
      skipVerify,
    );
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

  setStreamingKnownState(streamingKnownState: KnownStateSessions) {
    this.sessions.setStreamingKnownState(streamingKnownState);
  }

  newContentSince(
    knownState: CoValueKnownState | undefined,
  ): NewContentMessage[] | undefined {
    let currentPiece: NewContentMessage = createContentMessage(
      this.id,
      this.header,
      false,
    );
    const pieces: NewContentMessage[] = [currentPiece];
    let pieceSize = 0;

    const startNewPiece = () => {
      currentPiece = createContentMessage(this.id, this.header, false);
      pieces.push(currentPiece);
      pieceSize = 0;
    };

    const moveSessionContentToNewPiece = (sessionID: SessionID) => {
      const sessionContent = currentPiece.new[sessionID];

      if (!sessionContent) {
        throw new Error("Session content not found", {
          cause: {
            sessionID,
            currentPiece,
          },
        });
      }

      delete currentPiece.new[sessionID];

      const newPiece = createContentMessage(this.id, this.header, false);
      newPiece.new[sessionID] = sessionContent;

      // Insert the new piece before the current piece, to ensure that the order of the new transactions is preserved
      pieces.splice(pieces.length - 1, 0, newPiece);
    };

    const sessionSent = knownState?.sessions;

    for (const [sessionID, log] of this.sessions.sessions) {
      const startFrom = sessionSent?.[sessionID] ?? 0;

      let currentSessionSize = 0;

      for (let txIdx = startFrom; txIdx < log.transactions.length; txIdx++) {
        const isLastItem = txIdx === log.transactions.length - 1;
        const tx = log.transactions[txIdx]!;

        currentSessionSize += getTransactionSize(tx);

        const signatureAfter = log.signatureAfter[txIdx];

        if (signatureAfter) {
          addTransactionToContentMessage(
            currentPiece,
            tx,
            sessionID,
            signatureAfter,
            txIdx,
          );
          // When we meet a signatureAfter it means that the transaction log exceeds the recommended size
          // so we move the session content to a dedicated piece, because it must be sent in a standalone piece
          moveSessionContentToNewPiece(sessionID);
          currentSessionSize = 0;
        } else if (isLastItem) {
          if (!log.lastSignature) {
            throw new Error(
              "All the SessionLogs sent must have a lastSignature",
              {
                cause: log,
              },
            );
          }

          addTransactionToContentMessage(
            currentPiece,
            tx,
            sessionID,
            log.lastSignature,
            txIdx,
          );

          // If the current session size already exceeds the recommended size, we move the session content to a dedicated piece
          if (exceedsRecommendedSize(currentSessionSize)) {
            assertLastSignature(sessionID, currentPiece);
            moveSessionContentToNewPiece(sessionID);
          } else if (exceedsRecommendedSize(pieceSize, currentSessionSize)) {
            assertLastSignature(sessionID, currentPiece);
            startNewPiece();
          } else {
            pieceSize += currentSessionSize;
          }
        } else {
          // Unsafely add the transaction to the content message, without a signature because we don't have one for this session
          // Checks and assertions are enforced in this function to avoid that a content message gets out without a signature
          const signature = undefined as Signature | undefined;
          addTransactionToContentMessage(
            currentPiece,
            tx,
            sessionID,
            signature!,
            txIdx,
          );
        }
      }

      assertLastSignature(sessionID, currentPiece);
    }

    const firstPiece = pieces[0];

    if (!firstPiece) {
      throw new Error("First piece not found", {
        cause: pieces,
      });
    }

    const includeHeader = !knownState?.header;

    if (includeHeader) {
      firstPiece.header = this.header;
    }

    const piecesWithContent = pieces.filter(
      (piece) => piece.header || Object.keys(piece.new).length > 0,
    );

    if (piecesWithContent.length > 1 || this.isStreaming()) {
      if (knownState) {
        firstPiece.expectContentUntil = getKnownStateToSend(
          this.knownStateWithStreaming().sessions,
          knownState.sessions,
        );
      } else {
        firstPiece.expectContentUntil = {
          ...this.knownStateWithStreaming().sessions,
        };
      }
    }

    if (piecesWithContent.length === 0) {
      return undefined;
    }

    return piecesWithContent;
  }

  knownState() {
    return this.sessions.knownState;
  }

  knownStateWithStreaming() {
    return this.sessions.knownStateWithStreaming ?? this.knownState();
  }

  immutableKnownState() {
    return this.sessions.getImmutableKnownState();
  }

  immutableKnownStateWithStreaming() {
    return this.sessions.getImmutableKnownStateWithStreaming();
  }

  isStreaming(): boolean {
    return Boolean(this.sessions.knownStateWithStreaming);
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

function assertLastSignature(sessionID: SessionID, content: NewContentMessage) {
  if (content.new[sessionID] && !content.new[sessionID].lastSignature) {
    throw new Error("The SessionContent sent must have a lastSignature", {
      cause: content.new[sessionID],
    });
  }
}
