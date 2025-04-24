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
  KeySecret,
  Signature,
  SignerID,
  StreamingHash,
} from "./crypto/crypto.js";
import {
  RawCoID,
  SessionID,
  TransactionID,
  getGroupDependentKeyList,
  getParentGroupId,
  isParentGroupReference,
} from "./ids.js";
import { Stringified, parseJSON, stableStringify } from "./jsonStringify.js";
import { JsonObject, JsonValue } from "./jsonValue.js";
import { LocalNode, ResolveAccountAgentError } from "./localNode.js";
import { logger } from "./logger.js";
import {
  PermissionsDef as RulesetDef,
  determineValidTransactions,
  isKeyForKeyField,
} from "./permissions.js";
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

export type DecryptedTransaction = {
  txID: TransactionID;
  changes: JsonValue[];
  madeAt: number;
};

const readKeyCache = new WeakMap<CoValueCore, { [id: KeyID]: KeySecret }>();

export class CoValueCore {
  id: RawCoID;
  node: LocalNode;
  crypto: CryptoProvider;
  header: CoValueHeader;
  _sessionLogs: Map<SessionID, SessionLog>;
  _cachedContent?: RawCoValue;
  listeners: Set<(content?: RawCoValue) => void> = new Set();
  _decryptionCache: {
    [key: Encrypted<JsonValue[], JsonValue>]: JsonValue[] | undefined;
  } = {};
  _cachedKnownState?: CoValueKnownState;
  _cachedDependentOn?: RawCoID[];
  _cachedNewContentSinceEmpty?: NewContentMessage[] | undefined;
  _currentAsyncAddTransaction?: Promise<void>;

  constructor(
    header: CoValueHeader,
    node: LocalNode,
    internalInitSessions: Map<SessionID, SessionLog> = new Map(),
  ) {
    this.crypto = node.crypto;
    this.id = idforHeader(header, node.crypto);
    this.header = header;
    this._sessionLogs = internalInitSessions;
    this.node = node;
  }

  groupInvalidationSubscription?: () => void;

  subscribeToGroupInvalidation() {
    if (this.groupInvalidationSubscription) {
      return;
    }

    const header = this.header;

    if (header.ruleset.type == "ownedByGroup") {
      const groupId = header.ruleset.group;
      const entry = this.node.coValuesStore.get(groupId);

      if (entry.isAvailable()) {
        this.groupInvalidationSubscription = entry.core.subscribe(
          (_groupUpdate) => {
            this._cachedContent = undefined;
            this.notifyUpdate("immediate");
          },
          false,
        );
      } else {
        logger.error("CoValueCore: Owner group not available", {
          id: this.id,
          groupId,
        });
      }
    }
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

  nextTransactionID(): TransactionID {
    // This is an ugly hack to get a unique but stable session ID for editing the current account
    const sessionID =
      this.header.meta?.type === "account"
        ? (this.node.currentSessionID.replace(
            this.node.account.id,
            this.node.account.currentAgentID(),
          ) as SessionID)
        : this.node.currentSessionID;

    return {
      sessionID,
      txIndex: this.sessionLogs.get(sessionID)?.transactions.length || 0,
    };
  }

  tryAddTransactions(
    sessionID: SessionID,
    newTransactions: Transaction[],
    givenExpectedNewHash: Hash | undefined,
    newSignature: Signature,
    skipVerify: boolean = false,
    givenNewStreamingHash?: StreamingHash,
  ): Result<true, TryAddTransactionsError> {
    return this.node
      .resolveAccountAgent(
        accountOrAgentIDfromSessionID(sessionID),
        "Expected to know signer of transaction",
      )
      .andThen((agent) => {
        const signerID = this.crypto.getAgentSignerID(agent);

        if (
          skipVerify === true &&
          givenNewStreamingHash &&
          givenExpectedNewHash
        ) {
          this.doAddTransactions(
            sessionID,
            newTransactions,
            newSignature,
            givenExpectedNewHash,
            givenNewStreamingHash,
            "immediate",
          );
        } else {
          const { expectedNewHash, newStreamingHash } =
            this.expectedNewHashAfter(sessionID, newTransactions);

          if (
            givenExpectedNewHash &&
            givenExpectedNewHash !== expectedNewHash
          ) {
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
            "immediate",
          );
        }

        return ok(true as const);
      });
  }

  private doAddTransactions(
    sessionID: SessionID,
    newTransactions: Transaction[],
    newSignature: Signature,
    expectedNewHash: Hash,
    newStreamingHash: StreamingHash,
    notifyMode: "immediate" | "deferred",
  ) {
    if (this.node.crashed) {
      throw new Error("Trying to add transactions after node is crashed");
    }
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

    if (
      this._cachedContent &&
      "processNewTransactions" in this._cachedContent &&
      typeof this._cachedContent.processNewTransactions === "function"
    ) {
      this._cachedContent.processNewTransactions();
    } else {
      this._cachedContent = undefined;
    }

    this._cachedKnownState = undefined;
    this._cachedDependentOn = undefined;
    this._cachedNewContentSinceEmpty = undefined;

    this.notifyUpdate(notifyMode);
  }

  deferredUpdates = 0;
  nextDeferredNotify: Promise<void> | undefined;

  notifyUpdate(notifyMode: "immediate" | "deferred") {
    if (this.listeners.size === 0) {
      return;
    }

    if (notifyMode === "immediate") {
      const content = this.getCurrentContent();
      for (const listener of this.listeners) {
        try {
          listener(content);
        } catch (e) {
          logger.error("Error in listener for coValue " + this.id, { err: e });
        }
      }
    } else {
      if (!this.nextDeferredNotify) {
        this.nextDeferredNotify = new Promise((resolve) => {
          setTimeout(() => {
            this.nextDeferredNotify = undefined;
            this.deferredUpdates = 0;
            const content = this.getCurrentContent();
            for (const listener of this.listeners) {
              try {
                listener(content);
              } catch (e) {
                logger.error("Error in listener for coValue " + this.id, {
                  err: e,
                });
              }
            }
            resolve();
          }, 0);
        });
      }
      this.deferredUpdates++;
    }
  }

  subscribe(
    listener: (content?: RawCoValue) => void,
    immediateInvoke = true,
  ): () => void {
    this.listeners.add(listener);

    if (immediateInvoke) {
      listener(this.getCurrentContent());
    }

    return () => {
      this.listeners.delete(listener);
    };
  }

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

  makeTransaction(
    changes: JsonValue[],
    privacy: "private" | "trusting",
  ): boolean {
    const madeAt = Date.now();

    let transaction: Transaction;

    if (privacy === "private") {
      const { secret: keySecret, id: keyID } = this.getCurrentReadKey();

      if (!keySecret) {
        throw new Error("Can't make transaction without read key secret");
      }

      const encrypted = this.crypto.encryptForTransaction(changes, keySecret, {
        in: this.id,
        tx: this.nextTransactionID(),
      });

      this._decryptionCache[encrypted] = changes;

      transaction = {
        privacy: "private",
        madeAt,
        keyUsed: keyID,
        encryptedChanges: encrypted,
      };
    } else {
      transaction = {
        privacy: "trusting",
        madeAt,
        changes: stableStringify(changes),
      };
    }

    // This is an ugly hack to get a unique but stable session ID for editing the current account
    const sessionID =
      this.header.meta?.type === "account"
        ? (this.node.currentSessionID.replace(
            this.node.account.id,
            this.node.account.currentAgentID(),
          ) as SessionID)
        : this.node.currentSessionID;

    const { expectedNewHash, newStreamingHash } = this.expectedNewHashAfter(
      sessionID,
      [transaction],
    );

    const signature = this.crypto.sign(
      this.node.account.currentSignerSecret(),
      expectedNewHash,
    );

    const success = this.tryAddTransactions(
      sessionID,
      [transaction],
      expectedNewHash,
      signature,
      true,
      newStreamingHash,
    )._unsafeUnwrap({ withStackTrace: true });

    if (success) {
      this.node.syncManager.recordTransactionsSize([transaction], "local");
      void this.node.syncManager.syncCoValue(this);
    }

    return success;
  }

  getCurrentContent(options?: {
    ignorePrivateTransactions: true;
  }): RawCoValue {
    if (!options?.ignorePrivateTransactions && this._cachedContent) {
      return this._cachedContent;
    }

    this.subscribeToGroupInvalidation();

    const newContent = coreToCoValue(this, options);

    if (!options?.ignorePrivateTransactions) {
      this._cachedContent = newContent;
    }

    return newContent;
  }

  getValidTransactions(options?: {
    ignorePrivateTransactions: boolean;
    knownTransactions?: CoValueKnownState["sessions"];
  }): DecryptedTransaction[] {
    const validTransactions = determineValidTransactions(
      this,
      options?.knownTransactions,
    );

    const allTransactions: DecryptedTransaction[] = [];

    for (const { txID, tx } of validTransactions) {
      if (options?.knownTransactions?.[txID.sessionID]! >= txID.txIndex) {
        continue;
      }

      if (tx.privacy === "trusting") {
        allTransactions.push({
          txID,
          madeAt: tx.madeAt,
          changes: parseJSON(tx.changes),
        });
        continue;
      }

      if (options?.ignorePrivateTransactions) {
        continue;
      }

      const readKey = this.getReadKey(tx.keyUsed);

      if (!readKey) {
        continue;
      }

      let decryptedChanges = this._decryptionCache[tx.encryptedChanges];

      if (!decryptedChanges) {
        const decryptedString = this.crypto.decryptRawForTransaction(
          tx.encryptedChanges,
          readKey,
          {
            in: this.id,
            tx: txID,
          },
        );
        decryptedChanges = decryptedString && parseJSON(decryptedString);
        this._decryptionCache[tx.encryptedChanges] = decryptedChanges;
      }

      if (!decryptedChanges) {
        logger.error("Failed to decrypt transaction despite having key", {
          err: new Error("Failed to decrypt transaction despite having key"),
        });
        continue;
      }

      allTransactions.push({
        txID,
        madeAt: tx.madeAt,
        changes: decryptedChanges,
      });
    }

    return allTransactions;
  }

  getValidSortedTransactions(options?: {
    ignorePrivateTransactions: boolean;
  }): DecryptedTransaction[] {
    const allTransactions = this.getValidTransactions(options);

    allTransactions.sort(this.compareTransactions);

    return allTransactions;
  }

  compareTransactions(
    a: Pick<DecryptedTransaction, "madeAt" | "txID">,
    b: Pick<DecryptedTransaction, "madeAt" | "txID">,
  ) {
    return (
      a.madeAt - b.madeAt ||
      (a.txID.sessionID === b.txID.sessionID
        ? 0
        : a.txID.sessionID < b.txID.sessionID
          ? -1
          : 1) ||
      a.txID.txIndex - b.txID.txIndex
    );
  }

  getCurrentReadKey(): { secret: KeySecret | undefined; id: KeyID } {
    if (this.header.ruleset.type === "group") {
      const content = expectGroup(this.getCurrentContent());

      const currentKeyId = content.getCurrentReadKeyId();

      if (!currentKeyId) {
        throw new Error("No readKey set");
      }

      const secret = this.getReadKey(currentKeyId);

      return {
        secret: secret,
        id: currentKeyId,
      };
    } else if (this.header.ruleset.type === "ownedByGroup") {
      return this.node
        .expectCoValueLoaded(this.header.ruleset.group)
        .getCurrentReadKey();
    } else {
      throw new Error(
        "Only groups or values owned by groups have read secrets",
      );
    }
  }

  getReadKey(keyID: KeyID): KeySecret | undefined {
    let key = readKeyCache.get(this)?.[keyID];
    if (!key) {
      key = this.getUncachedReadKey(keyID);
      if (key) {
        let cache = readKeyCache.get(this);
        if (!cache) {
          cache = {};
          readKeyCache.set(this, cache);
        }
        cache[keyID] = key;
      }
    }
    return key;
  }

  getUncachedReadKey(keyID: KeyID): KeySecret | undefined {
    if (this.header.ruleset.type === "group") {
      const content = expectGroup(
        this.getCurrentContent({ ignorePrivateTransactions: true }),
      );

      const keyForEveryone = content.get(`${keyID}_for_everyone`);
      if (keyForEveryone) return keyForEveryone;

      // Try to find key revelation for us
      const lookupAccountOrAgentID =
        this.header.meta?.type === "account"
          ? this.node.account.currentAgentID()
          : this.node.account.id;

      const lastReadyKeyEdit = content.lastEditAt(
        `${keyID}_for_${lookupAccountOrAgentID}`,
      );

      if (lastReadyKeyEdit?.value) {
        const revealer = lastReadyKeyEdit.by;
        const revealerAgent = this.node
          .resolveAccountAgent(revealer, "Expected to know revealer")
          ._unsafeUnwrap({ withStackTrace: true });

        const secret = this.crypto.unseal(
          lastReadyKeyEdit.value,
          this.node.account.currentSealerSecret(),
          this.crypto.getAgentSealerID(revealerAgent),
          {
            in: this.id,
            tx: lastReadyKeyEdit.tx,
          },
        );

        if (secret) {
          return secret as KeySecret;
        }
      }

      // Try to find indirect revelation through previousKeys

      for (const co of content.keys()) {
        if (isKeyForKeyField(co) && co.startsWith(keyID)) {
          const encryptingKeyID = co.split("_for_")[1] as KeyID;
          const encryptingKeySecret = this.getReadKey(encryptingKeyID);

          if (!encryptingKeySecret) {
            continue;
          }

          const encryptedPreviousKey = content.get(co)!;

          const secret = this.crypto.decryptKeySecret(
            {
              encryptedID: keyID,
              encryptingID: encryptingKeyID,
              encrypted: encryptedPreviousKey,
            },
            encryptingKeySecret,
          );

          if (secret) {
            return secret as KeySecret;
          } else {
            logger.warn(
              `Encrypting ${encryptingKeyID} key didn't decrypt ${keyID}`,
            );
          }
        }
      }

      // try to find revelation to parent group read keys
      for (const co of content.keys()) {
        if (isParentGroupReference(co)) {
          const parentGroupID = getParentGroupId(co);
          const parentGroup = this.node.expectCoValueLoaded(
            parentGroupID,
            "Expected parent group to be loaded",
          );

          const parentKeys = this.findValidParentKeys(
            keyID,
            content,
            parentGroup,
          );

          for (const parentKey of parentKeys) {
            const revelationForParentKey = content.get(
              `${keyID}_for_${parentKey.id}`,
            );

            if (revelationForParentKey) {
              const secret = parentGroup.crypto.decryptKeySecret(
                {
                  encryptedID: keyID,
                  encryptingID: parentKey.id,
                  encrypted: revelationForParentKey,
                },
                parentKey.secret,
              );

              if (secret) {
                return secret as KeySecret;
              } else {
                logger.warn(
                  `Encrypting parent ${parentKey.id} key didn't decrypt ${keyID}`,
                );
              }
            }
          }
        }
      }

      return undefined;
    } else if (this.header.ruleset.type === "ownedByGroup") {
      return this.node
        .expectCoValueLoaded(this.header.ruleset.group)
        .getReadKey(keyID);
    } else {
      throw new Error(
        "Only groups or values owned by groups have read secrets",
      );
    }
  }

  findValidParentKeys(keyID: KeyID, group: RawGroup, parentGroup: CoValueCore) {
    const validParentKeys: { id: KeyID; secret: KeySecret }[] = [];

    for (const co of group.keys()) {
      if (isKeyForKeyField(co) && co.startsWith(keyID)) {
        const encryptingKeyID = co.split("_for_")[1] as KeyID;
        const encryptingKeySecret = parentGroup.getReadKey(encryptingKeyID);

        if (!encryptingKeySecret) {
          continue;
        }

        validParentKeys.push({
          id: encryptingKeyID,
          secret: encryptingKeySecret,
        });
      }
    }

    return validParentKeys;
  }

  getGroup(): RawGroup {
    if (this.header.ruleset.type !== "ownedByGroup") {
      throw new Error("Only values owned by groups have groups");
    }

    return expectGroup(
      this.node
        .expectCoValueLoaded(this.header.ruleset.group)
        .getCurrentContent(),
    );
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

  getDependedOnCoValues(): RawCoID[] {
    if (this._cachedDependentOn) {
      return this._cachedDependentOn;
    } else {
      const dependentOn = this.getDependedOnCoValuesUncached();
      this._cachedDependentOn = dependentOn;
      return dependentOn;
    }
  }

  /** @internal */
  getDependedOnCoValuesUncached(): RawCoID[] {
    return this.header.ruleset.type === "group"
      ? getGroupDependentKeyList(expectGroup(this.getCurrentContent()).keys())
      : this.header.ruleset.type === "ownedByGroup"
        ? [
            this.header.ruleset.group,
            ...new Set(
              [...this.sessionLogs.keys()]
                .map((sessionID) =>
                  accountOrAgentIDfromSessionID(sessionID as SessionID),
                )
                .filter(
                  (session): session is RawAccountID =>
                    isAccountID(session) && session !== this.id,
                ),
            ),
          ]
        : [];
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
