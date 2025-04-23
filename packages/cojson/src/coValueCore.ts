import { Result, err, ok } from "neverthrow";
import { AnyRawCoValue, RawCoValue } from "./coValue.js";
import { ControlledAccountOrAgent, RawAccountID } from "./coValues/account.js";
import { RawGroup } from "./coValues/group.js";
import { coreToCoValue } from "./coreToCoValue.js";
import {
  CryptoProvider,
  Encrypted,
  Hash,
  KeyID,
  Signature,
  SignerID,
  StreamingHash,
} from "./crypto/crypto.js";
import {
  AgentID,
  RawCoID,
  SessionID,
  TransactionID,
  getGroupDependentKeyList,
} from "./ids.js";
import { Stringified, stableStringify } from "./jsonStringify.js";
import { JsonObject, JsonValue } from "./jsonValue.js";
import { LocalNode, ResolveAccountAgentError } from "./localNode.js";
import { PermissionsDef as RulesetDef } from "./permissions.js";
import { getPriorityFromHeader } from "./priority.js";
import { CoValueKnownState, NewContentMessage } from "./sync.js";
import { accountOrAgentIDfromSessionID } from "./typeUtils/accountOrAgentIDfromSessionID.js";
import { expectGroup } from "./typeUtils/expectGroup.js";
import { isAccountID } from "./typeUtils/isAccountID.js";

/**
    In order to not block other concurrently syncing CoValues we introduce a maximum size of transactions,
    since they are the smallest unit of progress that can be synced within a CoValue.
    This is particularly important for storing binary data in CoValues, since they are likely to be at least on the order of megabytes.
    This also means that we want to keep signatures roughly after each MAX_RECOMMENDED_TX size chunk,
    to be able to verify partially loaded CoValues or CoValues that are still being created (like a video live stream).
**/
export const MAX_RECOMMENDED_TX_SIZE = 100 * 1024;

export type CoValueHeader = {
  type: AnyRawCoValue["type"];
  ruleset: RulesetDef;
  meta: JsonObject | null;
} & CoValueUniqueness;

export type CoValueUniqueness = {
  uniqueness: JsonValue;
  createdAt?: `2${string}` | null;
};

export function idforHeader(
  header: CoValueHeader,
  crypto: CryptoProvider,
): RawCoID {
  const hash = crypto.shortHash(header);
  return `co_z${hash.slice("shortHash_z".length)}`;
}

type SessionLog = {
  transactions: Transaction[];
  lastHash?: Hash;
  streamingHash: StreamingHash;
  signatureAfter: { [txIdx: number]: Signature | undefined };
  lastSignature: Signature;
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

export class CoValueCore {
  id: RawCoID;

  crypto: CryptoProvider;
  header: CoValueHeader;
  _sessionLogs: Map<SessionID, SessionLog>;
  _cachedKnownState?: CoValueKnownState;

  _cachedNewContentSinceEmpty?: NewContentMessage[] | undefined;

  constructor(
    header: CoValueHeader,
    node: LocalNode,
    internalInitSessions: Map<SessionID, SessionLog> = new Map(),
  ) {
    this.crypto = node.crypto;
    this.id = idforHeader(header, node.crypto);
    this.header = header;
    this._sessionLogs = internalInitSessions;
  }

  get sessionLogs(): Map<SessionID, SessionLog> {
    return this._sessionLogs;
  }

  testWithDifferentAccount(
    account: ControlledAccountOrAgent,
    currentSessionID: SessionID,
  ): CoValueCore {
    const newNode = this.node.testWithDifferentAccount(
      account,
      currentSessionID,
    );

    return newNode.expectCoValueLoaded(this.id);
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

    for (const [sessionID, sessionLog] of this.sessionLogs.entries()) {
      sessions[sessionID] = sessionLog.transactions.length;
    }

    return {
      id: this.id,
      header: true,
      sessions,
    };
  }

  get meta(): JsonValue {
    return this.header?.meta ?? null;
  }

  int_tryAddTransactions(
    agent: AgentID,
    sessionID: SessionID,
    newTransactions: Transaction[],
    givenExpectedNewHash: Hash | undefined,
    newSignature: Signature,
    skipVerify: boolean = false,
    givenNewStreamingHash?: StreamingHash,
  ): Result<true, TryAddTransactionsError> {
    const signerID = this.crypto.getAgentSignerID(agent);

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
    const transactions = this.sessionLogs.get(sessionID)?.transactions ?? [];

    for (const tx of newTransactions) {
      transactions.push(tx);
    }

    const signatureAfter =
      this.sessionLogs.get(sessionID)?.signatureAfter ?? {};

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

    this._sessionLogs.set(sessionID, {
      transactions,
      lastHash: expectedNewHash,
      streamingHash: newStreamingHash,
      lastSignature: newSignature,
      signatureAfter: signatureAfter,
    });

    this._cachedKnownState = undefined;
    this._cachedNewContentSinceEmpty = undefined;
  }

  // subscribe(
  //   listener: (content?: RawCoValue) => void,
  //   immediateInvoke = true,
  // ): () => void {
  //   this.listeners.add(listener);

  //   if (immediateInvoke) {
  //     listener(this.getCurrentContent());
  //   }

  //   return () => {
  //     this.listeners.delete(listener);
  //   };
  // }

  expectedNewHashAfter(
    sessionID: SessionID,
    newTransactions: Transaction[],
  ): { expectedNewHash: Hash; newStreamingHash: StreamingHash } {
    const streamingHash =
      this.sessionLogs.get(sessionID)?.streamingHash.clone() ??
      new StreamingHash(this.crypto);

    for (const transaction of newTransactions) {
      streamingHash.update(transaction);
    }

    return {
      expectedNewHash: streamingHash.digest(),
      newStreamingHash: streamingHash,
    };
  }

  getTx(txID: TransactionID): Transaction | undefined {
    return this.sessionLogs.get(txID.sessionID)?.transactions[txID.txIndex];
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
      const sessionsTodo = sessionsTodoAgain ?? this.sessionLogs.keys();

      for (const sessionIDKey of sessionsTodo) {
        const sessionID = sessionIDKey as SessionID;
        const log = this.sessionLogs.get(sessionID)!;
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
          currentPiece = {
            action: "content",
            id: this.id,
            header: undefined,
            new: {},
            priority: getPriorityFromHeader(this.header),
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

  waitForSync(options?: {
    timeout?: number;
  }) {
    return this.node.syncManager.waitForSync(this.id, options?.timeout);
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

export type InvalidHashError = {
  type: "InvalidHash";
  id: RawCoID;
  expectedNewHash: Hash;
  givenExpectedNewHash: Hash;
};

export type InvalidSignatureError = {
  type: "InvalidSignature";
  id: RawCoID;
  newSignature: Signature;
  sessionID: SessionID;
  signerID: SignerID;
};

export type TryAddTransactionsError =
  | ResolveAccountAgentError
  | InvalidHashError
  | InvalidSignatureError;
