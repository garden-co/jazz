import { UpDownCounter, ValueType, metrics } from "@opentelemetry/api";
import { Result, err, ok } from "neverthrow";
import type { PeerState } from "../PeerState.js";
import type { RawCoValue } from "../coValue.js";
import type { ControlledAccountOrAgent } from "../coValues/account.js";
import type { RawGroup } from "../coValues/group.js";
import { CO_VALUE_LOADING_CONFIG } from "../config.js";
import { validateTxSizeLimitInBytes } from "../coValueContentMessage.js";
import { coreToCoValue } from "../coreToCoValue.js";
import {
  CryptoProvider,
  Hash,
  KeyID,
  KeySecret,
  Signature,
  SignerID,
} from "../crypto/crypto.js";
import { AgentID, RawCoID, SessionID, TransactionID } from "../ids.js";
import { JsonObject, JsonValue } from "../jsonValue.js";
import { LocalNode, ResolveAccountAgentError } from "../localNode.js";
import { logger } from "../logger.js";
import { determineValidTransactions } from "../permissions.js";
import { NewContentMessage, PeerID } from "../sync.js";
import { accountOrAgentIDfromSessionID } from "../typeUtils/accountOrAgentIDfromSessionID.js";
import { expectGroup } from "../typeUtils/expectGroup.js";
import {
  getDependenciesFromContentMessage,
  getDependenciesFromGroupRawTransactions,
  getDependenciesFromHeader,
} from "./utils.js";
import { CoValueHeader, Transaction, VerifiedState } from "./verifiedState.js";
import { SessionMap } from "./SessionMap.js";
import {
  MergeCommit,
  BranchPointerCommit,
  MergedTransactionMetadata,
  createBranch,
  getBranchId,
  getBranchOwnerId,
  getBranchSource,
  mergeBranch,
  BranchStartCommit,
} from "./branching.js";
import { type RawAccountID } from "../coValues/account.js";
import { decryptTransactionChangesAndMeta } from "./decryptTransactionChangesAndMeta.js";
import {
  combineKnownStateSessions,
  CoValueKnownState,
  emptyKnownState,
  KnownStateSessions,
} from "../knownState.js";
import { safeParseJSON } from "../jsonStringify.js";

export function idforHeader(
  header: CoValueHeader,
  crypto: CryptoProvider,
): RawCoID {
  const hash = crypto.shortHash(header);
  return `co_z${hash.slice("shortHash_z".length)}`;
}

export class VerifiedTransaction {
  // The account or agent that made the transaction
  author: RawAccountID | AgentID;
  // An object containing the session ID and the transaction index
  currentTxID: TransactionID;
  // If this is a merged transaction, the TxID of the transaction inside the original branch
  sourceTxID: TransactionID | undefined;
  tx: Transaction;
  // The Unix time when the transaction was made
  currentMadeAt: number;
  // If this is a merged transaction, the madeAt of the transaction inside the original branch
  sourceTxMadeAt: number | undefined;
  // The decoded changes of the transaction
  changes: JsonValue[] | undefined;
  // The decoded meta information of the transaction
  meta: JsonObject | undefined;
  // Whether the transaction is valid, as per membership rules
  isValid: boolean = false;
  // Whether the transaction has been validated, used to track if determinedValidTransactions needs to check this
  isValidated: boolean = false;
  // True if the transaction has been decrypted
  isDecrypted: boolean = false;
  // True if the meta information has been parsed and loaded in the CoValueCore
  hasMetaBeenParsed: boolean = false;
  // The previous verified transaction for the same session
  previous: VerifiedTransaction | undefined;

  constructor(
    sessionID: SessionID,
    txIndex: number,
    tx: Transaction,
    branchId: RawCoID | undefined,
    parsingCache:
      | { changes: JsonValue[]; meta: JsonObject | undefined }
      | undefined,
    previous: VerifiedTransaction | undefined,
  ) {
    this.author = accountOrAgentIDfromSessionID(sessionID);

    const txID = branchId
      ? {
          sessionID,
          txIndex,
          branch: branchId,
        }
      : {
          sessionID,
          txIndex,
        };

    this.currentTxID = txID;
    this.sourceTxID = undefined;
    this.tx = tx;
    this.currentMadeAt = tx.madeAt;
    this.sourceTxMadeAt = undefined;
    this.isValidated = false;

    this.previous = previous;

    if (parsingCache) {
      this.changes = parsingCache.changes;
      this.meta = parsingCache.meta;
    } else {
      // Decoding the trusting transactions here because they might be useful in the permissions checks
      if (this.tx.privacy === "trusting") {
        this.changes = safeParseJSON(this.tx.changes);

        if (this.tx.meta) {
          this.meta = safeParseJSON(this.tx.meta);
        }
      }
    }
  }

  // The TxID that refers to the current position in the session map
  // If this is a merged transaction, the txID is the TxID of the merged transaction
  get txID() {
    return this.sourceTxID ?? this.currentTxID;
  }

  // The madeAt that refers to the time when the transaction was made
  // If this is a merged transaction, the madeAt is the time when the transaction has been made in the branch
  get madeAt() {
    return this.sourceTxMadeAt ?? this.currentMadeAt;
  }

  isValidTransactionWithChanges(): this is {
    changes: JsonValue[];
    isValid: true;
  } {
    return Boolean(this.isValid && this.changes);
  }
}

export type DecryptedTransaction = Omit<VerifiedTransaction, "changes"> & {
  changes: JsonValue[];
};

export type AvailableCoValueCore = CoValueCore & { verified: VerifiedState };

export class CoValueCore {
  // context
  readonly node: LocalNode;
  private readonly crypto: CryptoProvider;

  // state
  id: RawCoID;
  private _verified: VerifiedState | null;
  /** Holds the fundamental syncable content of a CoValue,
   * consisting of the header (verified by hash -> RawCoID)
   * and the sessions (verified by signature).
   *
   * It does not do any *validation* or *decryption* and as such doesn't
   * depend on other CoValues or the LocalNode.
   *
   * `CoValueCore.verified` may be null when a CoValue is requested to be
   * loaded but no content has been received from storage or peers yet.
   * In this case, it acts as a centralised entry to keep track of peer loading
   * state and to subscribe to its content when it does become available. */
  get verified() {
    return this._verified;
  }
  private readonly peers = new Map<
    PeerID,
    | {
        type: "unknown" | "pending" | "available" | "unavailable";
      }
    | {
        type: "errored";
        error: TryAddTransactionsError;
      }
  >();

  // cached state and listeners
  private _cachedContent?: RawCoValue;
  readonly listeners: Set<(core: CoValueCore, unsub: () => void) => void> =
    new Set();
  private counter: UpDownCounter;

  private constructor(
    init: { header: CoValueHeader } | { id: RawCoID },
    node: LocalNode,
  ) {
    this.crypto = node.crypto;
    if ("header" in init) {
      this.id = idforHeader(init.header, node.crypto);
      this._verified = new VerifiedState(this.id, node.crypto, init.header);
    } else {
      this.id = init.id;
      this._verified = null;
    }
    this.node = node;

    this.counter = metrics
      .getMeter("cojson")
      .createUpDownCounter("jazz.covalues.loaded", {
        description: "The number of covalues in the system",
        unit: "covalue",
        valueType: ValueType.INT,
      });

    this.updateCounter(null);
  }

  static fromID(id: RawCoID, node: LocalNode): CoValueCore {
    return new CoValueCore({ id }, node);
  }

  static fromHeader(
    header: CoValueHeader,
    node: LocalNode,
  ): AvailableCoValueCore {
    return new CoValueCore({ header }, node) as AvailableCoValueCore;
  }

  get loadingState() {
    if (this.verified) {
      return "available";
    } else if (this.peers.size === 0) {
      return "unknown";
    }

    for (const peer of this.peers.values()) {
      if (peer.type === "pending") {
        return "loading";
      } else if (peer.type === "unknown") {
        return "unknown";
      }
    }

    return "unavailable";
  }

  hasMissingDependencies() {
    return this.missingDependencies.size > 0;
  }

  isAvailable(): this is AvailableCoValueCore {
    return this.hasVerifiedContent();
  }

  hasVerifiedContent(): this is AvailableCoValueCore {
    return !!this.verified;
  }

  isErroredInPeer(peerId: PeerID) {
    return this.peers.get(peerId)?.type === "errored";
  }

  waitForAvailableOrUnavailable(): Promise<CoValueCore> {
    return new Promise<CoValueCore>((resolve) => {
      const listener = (core: CoValueCore) => {
        if (core.isAvailable() || core.loadingState === "unavailable") {
          resolve(core);
          this.listeners.delete(listener);
        }
      };

      this.listeners.add(listener);
      listener(this);
    });
  }

  waitForAvailable(): Promise<CoValueCore> {
    return new Promise<CoValueCore>((resolve) => {
      const listener = (core: CoValueCore) => {
        if (core.isAvailable()) {
          resolve(core);
          this.listeners.delete(listener);
        }
      };

      this.listeners.add(listener);
      listener(this);
    });
  }

  waitForFullStreaming(): Promise<CoValueCore> {
    return new Promise<CoValueCore>((resolve) => {
      const listener = (core: CoValueCore) => {
        if (core.isAvailable() && !core.verified.isStreaming()) {
          resolve(core);
          this.listeners.delete(listener);
        }
      };

      this.listeners.add(listener);
      listener(this);
    });
  }

  getStateForPeer(peerId: PeerID) {
    return this.peers.get(peerId);
  }

  private updateCounter(previousState: string | null) {
    const newState = this.loadingState;

    if (previousState !== newState) {
      if (previousState) {
        this.counter.add(-1, { state: previousState });
      }
      this.counter.add(1, { state: newState });
    }
  }

  unmount(garbageCollectGroups = false) {
    if (
      !garbageCollectGroups &&
      this.verified?.header.ruleset.type === "group"
    ) {
      return false;
    }

    if (this.listeners.size > 0) {
      return false; // The coValue is still in use
    }

    this.counter.add(-1, { state: this.loadingState });

    if (this.groupInvalidationSubscription) {
      this.groupInvalidationSubscription();
      this.groupInvalidationSubscription = undefined;
    }

    this.node.internalDeleteCoValue(this.id);

    return true;
  }

  markNotFoundInPeer(peerId: PeerID) {
    const previousState = this.loadingState;
    this.peers.set(peerId, { type: "unavailable" });
    this.updateCounter(previousState);
    this.scheduleNotifyUpdate();
  }

  markFoundInPeer(peerId: PeerID, previousState: string) {
    this.peers.set(peerId, { type: "available" });
    this.updateCounter(previousState);
    this.scheduleNotifyUpdate();
  }

  missingDependencies = new Set<RawCoID>();

  isCircularMissingDependency(dependency: CoValueCore) {
    const visited = new Set<RawCoID>();
    const stack = [dependency];

    while (stack.length > 0) {
      const current = stack.pop();

      if (!current) {
        return false;
      }

      visited.add(current.id);

      for (const dependency of current.missingDependencies) {
        if (dependency === this.id) {
          return true;
        }

        if (!visited.has(dependency)) {
          stack.push(this.node.getCoValue(dependency));
        }
      }
    }

    return false;
  }

  markDependencyAvailable(dependency: RawCoID) {
    this.missingDependencies.delete(dependency);

    if (this.missingDependencies.size === 0) {
      this.scheduleNotifyUpdate();
    }
  }

  newContentQueue: {
    msg: NewContentMessage;
    from: PeerState | "storage" | "import";
  }[] = [];
  /**
   * Add a new content to the queue and handle it when the dependencies are available
   */
  addNewContentToQueue(
    msg: NewContentMessage,
    from: PeerState | "storage" | "import",
  ) {
    const alreadyEnqueued = this.newContentQueue.length > 0;

    this.newContentQueue.push({ msg, from });

    if (alreadyEnqueued) {
      return;
    }

    this.subscribe((core, unsubscribe) => {
      if (!core.hasMissingDependencies()) {
        unsubscribe();

        const enqueuedNewContent = this.newContentQueue;
        this.newContentQueue = [];

        for (const { msg, from } of enqueuedNewContent) {
          this.node.syncManager.handleNewContent(msg, from);
        }
      }
    });
  }

  addDependencyFromHeader(header: CoValueHeader) {
    for (const dep of getDependenciesFromHeader(header)) {
      this.addDependency(dep);
    }
  }

  provideHeader(
    header: CoValueHeader,
    streamingKnownState?: KnownStateSessions,
    skipVerify?: boolean,
  ) {
    if (!skipVerify) {
      const expectedId = idforHeader(header, this.node.crypto);

      if (this.id !== expectedId) {
        return false;
      }
    }

    this.addDependencyFromHeader(header);

    if (this._verified?.sessions.size) {
      throw new Error(
        "CoValueCore: provideHeader called on coValue with verified sessions present!",
      );
    }
    this._verified = new VerifiedState(
      this.id,
      this.node.crypto,
      header,
      new SessionMap(this.id, this.node.crypto, streamingKnownState),
    );

    return true;
  }

  markErrored(peerId: PeerID, error: TryAddTransactionsError) {
    const previousState = this.loadingState;
    this.peers.set(peerId, { type: "errored", error });
    this.updateCounter(previousState);
    this.scheduleNotifyUpdate();
  }

  markPending(peerId: PeerID) {
    const previousState = this.loadingState;
    this.peers.set(peerId, { type: "pending" });
    this.updateCounter(previousState);
    this.scheduleNotifyUpdate();
  }

  groupInvalidationSubscription?: () => void;

  subscribeToGroupInvalidation() {
    if (!this.verified) {
      return;
    }

    if (this.groupInvalidationSubscription) {
      return;
    }

    const header = this.verified.header;

    if (header.ruleset.type == "ownedByGroup") {
      const groupId = header.ruleset.group;
      const entry = this.node.getCoValue(groupId);

      if (entry.isAvailable()) {
        this.groupInvalidationSubscription = entry.subscribe((_groupUpdate) => {
          // When the group is updated, we need to reset the cached content because the transactions validity might have changed
          this.resetParsedTransactions();
          this.scheduleNotifyUpdate();
        }, false);
      } else {
        logger.error("CoValueCore: Owner group not available", {
          id: this.id,
          groupId,
        });
      }
    }
  }

  contentInClonedNodeWithDifferentAccount(account: ControlledAccountOrAgent) {
    return this.node
      .loadCoValueAsDifferentAgent(this.id, account.agentSecret, account.id)
      .then((core) => core.getCurrentContent());
  }

  knownStateWithStreaming(): CoValueKnownState {
    if (this.verified) {
      return this.verified.knownStateWithStreaming();
    } else {
      return emptyKnownState(this.id);
    }
  }

  knownState(): CoValueKnownState {
    if (this.verified) {
      return this.verified.knownState();
    } else {
      return emptyKnownState(this.id);
    }
  }

  get meta(): JsonValue {
    return this.verified?.header.meta ?? null;
  }

  nextTransactionID(): TransactionID {
    if (!this.verified) {
      throw new Error(
        "CoValueCore: nextTransactionID called on coValue without verified state",
      );
    }

    // This is an ugly hack to get a unique but stable session ID for editing the current account
    const sessionID =
      this.verified.header.meta?.type === "account"
        ? (this.node.currentSessionID.replace(
            this.node.getCurrentAgent().id,
            this.node.getCurrentAgent().currentAgentID(),
          ) as SessionID)
        : this.node.currentSessionID;

    return {
      sessionID,
      txIndex: this.verified.sessions.get(sessionID)?.transactions.length || 0,
    };
  }

  addDependenciesFromContentMessage(newContent: NewContentMessage) {
    const dependencies = getDependenciesFromContentMessage(this, newContent);

    for (const dependency of dependencies) {
      this.addDependency(dependency);
    }
  }

  tryAddTransactions(
    sessionID: SessionID,
    newTransactions: Transaction[],
    newSignature: Signature,
    skipVerify: boolean = false,
  ): Result<true, TryAddTransactionsError> {
    let result: Result<SignerID | undefined, TryAddTransactionsError>;

    if (skipVerify) {
      result = ok(undefined);
    } else {
      result = this.node
        .resolveAccountAgent(
          accountOrAgentIDfromSessionID(sessionID),
          "Expected to know signer of transaction",
        )
        .andThen((agent) => {
          return ok(this.crypto.getAgentSignerID(agent));
        });
    }

    return result.andThen((signerID) => {
      if (!this.verified) {
        return err({
          type: "TriedToAddTransactionsWithoutVerifiedState",
          id: this.id,
        } satisfies TriedToAddTransactionsWithoutVerifiedStateErrpr);
      }

      const result = this.verified.tryAddTransactions(
        sessionID,
        signerID,
        newTransactions,
        newSignature,
        skipVerify,
      );

      if (result.isOk()) {
        this.processNewTransactions();
        this.scheduleNotifyUpdate();
      }

      return result;
    });
  }

  private processNewTransactions() {
    if (this._cachedContent) {
      // Does the cached content support incremental processing?
      if (
        "processNewTransactions" in this._cachedContent &&
        typeof this._cachedContent.processNewTransactions === "function"
      ) {
        this._cachedContent.processNewTransactions();
      } else {
        this._cachedContent = undefined;
      }
    }
  }

  #isNotificationScheduled = false;
  #batchedUpdates = false;

  private scheduleNotifyUpdate() {
    if (this.listeners.size === 0) {
      return;
    }

    this.#batchedUpdates = true;

    if (!this.#isNotificationScheduled) {
      this.#isNotificationScheduled = true;

      queueMicrotask(() => {
        this.#isNotificationScheduled = false;

        // Check if an immediate update has been notified
        if (this.#batchedUpdates) {
          this.notifyUpdate();
        }
      });
    }
  }

  #isNotifyUpdatePaused = false;
  pauseNotifyUpdate() {
    this.#isNotifyUpdatePaused = true;
  }

  resumeNotifyUpdate() {
    this.#isNotifyUpdatePaused = false;
    this.notifyUpdate();
  }

  private notifyUpdate() {
    if (this.listeners.size === 0 || this.#isNotifyUpdatePaused) {
      return;
    }

    this.#batchedUpdates = false;

    for (const listener of this.listeners) {
      try {
        listener(this, () => {
          this.listeners.delete(listener);
        });
      } catch (e) {
        logger.error("Error in listener for coValue " + this.id, { err: e });
      }
    }
  }

  subscribe(
    listener: (core: CoValueCore, unsub: () => void) => void,
    immediateInvoke = true,
  ): () => void {
    this.listeners.add(listener);

    if (immediateInvoke) {
      listener(this, () => {
        this.listeners.delete(listener);
      });
    }

    return () => {
      this.listeners.delete(listener);
    };
  }

  makeTransaction(
    changes: JsonValue[],
    privacy: "private" | "trusting",
    meta?: JsonObject,
    madeAt?: number,
  ): boolean {
    if (!this.verified) {
      throw new Error(
        "CoValueCore: makeTransaction called on coValue without verified state",
      );
    }

    validateTxSizeLimitInBytes(changes);

    // This is an ugly hack to get a unique but stable session ID for editing the current account
    const sessionID =
      this.verified.header.meta?.type === "account"
        ? (this.node.currentSessionID.replace(
            this.node.getCurrentAgent().id,
            this.node.getCurrentAgent().currentAgentID(),
          ) as SessionID)
        : this.node.currentSessionID;

    const signerAgent = this.node.getCurrentAgent();

    let result: { signature: Signature; transaction: Transaction };

    if (privacy === "private") {
      const { secret: keySecret, id: keyID } = this.getCurrentReadKey();

      if (!keySecret) {
        throw new Error("Can't make transaction without read key secret");
      }

      result = this.verified.makeNewPrivateTransaction(
        sessionID,
        signerAgent,
        changes,
        keyID,
        keySecret,
        meta,
        madeAt ?? Date.now(),
      );
    } else {
      result = this.verified.makeNewTrustingTransaction(
        sessionID,
        signerAgent,
        changes,
        meta,
        madeAt ?? Date.now(),
      );
    }

    const { transaction, signature } = result;

    // Assign pre-parsed meta and changes to skip the parse/decrypt operation when loading
    // this transaction in the current content
    this.parsingCache.set(transaction, { changes, meta });

    this.node.syncManager.recordTransactionsSize([transaction], "local");

    const session = this.verified.sessions.get(sessionID);
    const txIdx = session ? session.transactions.length - 1 : 0;

    this.processNewTransactions();
    this.addDependenciesFromNewTransaction(transaction);

    // force immediate notification because local updates may come from the UI
    // where we need synchronous updates
    this.notifyUpdate();
    this.node.syncManager.syncLocalTransaction(
      this.verified,
      transaction,
      sessionID,
      signature,
      txIdx,
    );

    return true;
  }

  addDependenciesFromNewTransaction(transaction: Transaction) {
    if (this.verified?.header.ruleset.type === "group") {
      for (const dependency of getDependenciesFromGroupRawTransactions([
        transaction,
      ])) {
        this.addDependency(dependency);
      }
    }
  }

  getCurrentContent(options?: { ignorePrivateTransactions: true }): RawCoValue {
    if (!this.verified) {
      throw new Error(
        "CoValueCore: getCurrentContent called on coValue without verified state",
      );
    }

    if (!options?.ignorePrivateTransactions && this._cachedContent) {
      return this._cachedContent;
    }

    this.subscribeToGroupInvalidation();

    const newContent = coreToCoValue(this as AvailableCoValueCore, options);

    if (!options?.ignorePrivateTransactions) {
      this._cachedContent = newContent;
    }

    return newContent;
  }

  // The starting point of the branch, in case this CoValue is a branch
  branchStart: BranchStartCommit["from"] | undefined;

  // The list of merge commits that have been made
  mergeCommits: MergeCommit[] = [];
  branches: BranchPointerCommit[] = [];
  earliestTxMadeAt: number = Number.MAX_SAFE_INTEGER;
  latestTxMadeAt: number = 0;

  // Reset the parsed transactions and branches, to validate them again from scratch when the group is updated
  resetParsedTransactions() {
    this._cachedContent = undefined;

    this.branchStart = undefined;
    this.mergeCommits = [];

    for (const transaction of this.verifiedTransactions) {
      transaction.isValidated = false;
      transaction.hasMetaBeenParsed = false;
    }
  }

  verifiedTransactions: VerifiedTransaction[] = [];
  private verifiedTransactionsKnownSessions: CoValueKnownState["sessions"] = {};

  private lastVerifiedTransactionBySessionID: Record<
    SessionID,
    VerifiedTransaction
  > = {};

  private parsingCache = new Map<
    Transaction,
    { changes: JsonValue[]; meta: JsonObject | undefined }
  >();

  /**
   * Loads the new transaction from the SessionMap into verifiedTransactions as a VerifiedTransaction.
   *
   * If the transaction is already loaded from the SessionMap in the past, it will not be loaded again.
   *
   * Used to have a fast way to iterate over the CoValue transactions, and track their validation/decoding state.

  * @internal
   * */
  loadVerifiedTransactionsFromLogs() {
    if (!this.verified) {
      return;
    }

    const isBranched = this.isBranched();

    for (const [sessionID, sessionLog] of this.verified.sessions.entries()) {
      const count = this.verifiedTransactionsKnownSessions[sessionID] ?? 0;

      for (
        let txIndex = count;
        txIndex < sessionLog.transactions.length;
        txIndex++
      ) {
        const tx = sessionLog.transactions[txIndex];
        if (!tx) {
          continue;
        }

        const cache = this.parsingCache.get(tx);
        if (cache) {
          this.parsingCache.delete(tx);
        }

        const verifiedTransaction = new VerifiedTransaction(
          sessionID,
          txIndex,
          tx,
          isBranched ? this.id : undefined,
          cache,
          this.lastVerifiedTransactionBySessionID[sessionID],
        );

        if (verifiedTransaction.madeAt > this.latestTxMadeAt) {
          this.latestTxMadeAt = verifiedTransaction.madeAt;
        }

        if (verifiedTransaction.madeAt < this.earliestTxMadeAt) {
          this.earliestTxMadeAt = verifiedTransaction.madeAt;
        }

        this.verifiedTransactions.push(verifiedTransaction);
        this.lastVerifiedTransactionBySessionID[sessionID] =
          verifiedTransaction;
      }

      this.verifiedTransactionsKnownSessions[sessionID] =
        sessionLog.transactions.length;
    }
  }

  /**
   * Iterates over the verifiedTransactions and marks them as valid or invalid, based on the group membership of the authors of the transactions  .
   */
  private determineValidTransactions() {
    determineValidTransactions(this);
  }

  /**
   * Parses the meta information of a transaction, and set the branchStart and mergeCommits.
   */
  private parseMetaInformation(transaction: VerifiedTransaction) {
    if (
      !transaction.meta ||
      !transaction.isValid ||
      transaction.hasMetaBeenParsed
    ) {
      return;
    }

    transaction.hasMetaBeenParsed = true;

    // Branch related meta information
    if (this.isBranched()) {
      // Check if the transaction is a branch start
      if ("from" in transaction.meta) {
        const meta = transaction.meta as BranchStartCommit;

        if (this.branchStart) {
          this.branchStart = combineKnownStateSessions(
            this.branchStart,
            meta.from,
          );
        } else {
          this.branchStart = meta.from;
        }
      }
    }

    // Check if the transaction is a branch pointer
    if ("branch" in transaction.meta) {
      const branch = transaction.meta as BranchPointerCommit;

      this.branches.push(branch);
    }

    // Check if the transaction is a merged checkpoint for a branch
    if ("merged" in transaction.meta) {
      const mergeCommit = transaction.meta as MergeCommit;
      this.mergeCommits.push(mergeCommit);
    }

    // Check if the transaction has been merged from a branch
    if ("mi" in transaction.meta) {
      const meta = transaction.meta as MergedTransactionMetadata;

      // Check if the transaction is a merge commit
      const previousTransaction = transaction.previous;
      const sessionID = meta.s ?? previousTransaction?.txID.sessionID;

      if (meta.t) {
        transaction.sourceTxMadeAt = transaction.currentMadeAt - meta.t;
      } else if (previousTransaction) {
        transaction.sourceTxMadeAt = previousTransaction.madeAt;
      }

      // Check against tampering of the meta.t value to write transaction after the access revocation
      if (
        transaction.sourceTxMadeAt &&
        transaction.sourceTxMadeAt > transaction.currentMadeAt
      ) {
        transaction.isValid = false;
      }

      if (sessionID) {
        transaction.sourceTxID = {
          sessionID,
          txIndex: meta.mi,
          branch: meta.b ?? previousTransaction?.txID.branch,
        };
      } else {
        logger.error("Merge commit without session ID", {
          txID: transaction.txID,
          prevTxID: previousTransaction?.txID ?? null,
        });
      }
    }
  }

  /**
   * Loads the new transactions from SessionMap and:
   * - Validates each transaction based on the group membership of the authors
   * - Decodes the changes & meta for each transaction
   * - Parses the meta information of the transaction
   */
  private parseNewTransactions(ignorePrivateTransactions: boolean) {
    if (!this.isAvailable()) {
      return;
    }

    this.loadVerifiedTransactionsFromLogs();
    this.determineValidTransactions();

    for (const transaction of this.verifiedTransactions) {
      if (!ignorePrivateTransactions) {
        decryptTransactionChangesAndMeta(this, transaction);
      }

      this.parseMetaInformation(transaction);
    }
  }

  /**
   * Returns the valid transactions matching the criteria specified in the options
   */
  getValidTransactions(options?: {
    ignorePrivateTransactions: boolean;
    // The range, described as knownState sessions, to filter the transactions returned
    from?: CoValueKnownState["sessions"];
    to?: CoValueKnownState["sessions"];

    // The transactions that have already been processed, used for the incremental builds of the content views
    knownTransactions?: Set<Transaction>;

    // If true, the branch source transactions will be skipped. Used to gather the transactions for the merge operation.
    skipBranchSource?: boolean;
  }): DecryptedTransaction[] {
    if (!this.verified) {
      return [];
    }

    this.parseNewTransactions(options?.ignorePrivateTransactions ?? false);

    const matchingTransactions: DecryptedTransaction[] = [];

    const source = getBranchSource(this);

    for (const transaction of this.verifiedTransactions) {
      if (!transaction.isValidTransactionWithChanges()) {
        continue;
      }

      if (options?.knownTransactions?.has(transaction.tx)) {
        continue;
      }

      options?.knownTransactions?.add(transaction.tx);

      // Using the currentTxID to filter the transactions, because the TxID is modified by the merge meta
      const txID = transaction.currentTxID;

      const from = options?.from?.[txID.sessionID] ?? -1;

      // Load the to filter index. Sessions that are not in the to filter will be skipped
      const to = options?.to ? (options.to[txID.sessionID] ?? -1) : Infinity;

      // The txIndex starts at 0 and from/to are referring to the count of transactions
      if (from > txID.txIndex || to < txID.txIndex) {
        continue;
      }

      matchingTransactions.push(transaction);
    }

    // If this is a branch, we load the valid transactions from the source
    if (source && this.branchStart && !options?.skipBranchSource) {
      const sourceTransactions = source.getValidTransactions({
        to: this.branchStart,
        ignorePrivateTransactions: options?.ignorePrivateTransactions ?? false,
        knownTransactions: options?.knownTransactions,
      });

      for (const transaction of sourceTransactions) {
        matchingTransactions.push(transaction);
      }
    }

    return matchingTransactions;
  }

  dependencies: Set<RawCoID> = new Set();
  private addDependency(dependency: RawCoID) {
    if (this.dependencies.has(dependency)) {
      return true;
    }

    this.dependencies.add(dependency);

    const dependencyCoValue = this.node.getCoValue(dependency);

    if (
      !dependencyCoValue.isAvailable() &&
      !this.isCircularMissingDependency(dependencyCoValue)
    ) {
      this.missingDependencies.add(dependency);

      dependencyCoValue.subscribe((dependencyCoValue, unsubscribe) => {
        if (dependencyCoValue.isAvailable()) {
          unsubscribe();
          this.markDependencyAvailable(dependency);
        }
      });
      return false;
    }
  }

  createBranch(name: string, ownerId?: RawCoID) {
    return createBranch(this, name, ownerId);
  }

  mergeBranch() {
    return mergeBranch(this);
  }

  getBranch(name: string, ownerId?: RawCoID) {
    return this.node.getCoValue(getBranchId(this, name, ownerId));
  }

  getCurrentBranchName() {
    return this.verified?.branchName;
  }

  getCurrentBranchSourceId() {
    return this.verified?.branchSourceId;
  }

  isBranched() {
    return Boolean(this.verified?.branchSourceId);
  }

  hasBranch(name: string, ownerId?: RawCoID) {
    // This function requires the meta information to be parsed, which might not be the case
    // if the value content hasn't been loaded yet
    this.parseNewTransactions(false);

    const currentOwnerId = getBranchOwnerId(this);
    return this.branches.some((item) => {
      if (item.branch !== name) {
        return false;
      }

      if (item.ownerId === ownerId) {
        return true;
      }

      if (!ownerId) {
        return item.ownerId === currentOwnerId;
      }

      if (!item.ownerId) {
        return ownerId === currentOwnerId;
      }
    });
  }

  getMergeCommits() {
    return this.mergeCommits;
  }

  getValidSortedTransactions(options?: {
    ignorePrivateTransactions: boolean;

    // The transactions that have already been processed, used for the incremental builds of the content views
    knownTransactions?: Set<Transaction>;
  }): DecryptedTransaction[] {
    const allTransactions = this.getValidTransactions(options);

    allTransactions.sort(this.compareTransactions);

    return allTransactions;
  }

  compareTransactions(
    a: Pick<VerifiedTransaction, "madeAt" | "txID">,
    b: Pick<VerifiedTransaction, "madeAt" | "txID">,
  ) {
    if (a.madeAt !== b.madeAt) {
      return a.madeAt - b.madeAt;
    }

    if (a.txID.sessionID === b.txID.sessionID) {
      return a.txID.txIndex - b.txID.txIndex;
    }

    return 0;
  }

  getCurrentReadKey(): {
    secret: KeySecret | undefined;
    id: KeyID;
  } {
    if (!this.verified) {
      throw new Error(
        "CoValueCore: getCurrentReadKey called on coValue without verified state",
      );
    }

    if (this.verified.header.ruleset.type === "group") {
      return expectGroup(this.getCurrentContent()).getCurrentReadKey();
    } else if (this.verified.header.ruleset.type === "ownedByGroup") {
      return this.node
        .expectCoValueLoaded(this.verified.header.ruleset.group)
        .getCurrentReadKey();
    } else {
      throw new Error(
        "Only groups or values owned by groups have read secrets",
      );
    }
  }

  readKeyCache = new Map<KeyID, KeySecret>();
  getReadKey(keyID: KeyID): KeySecret | undefined {
    // We want to check the cache here, to skip re-computing the group content
    const cachedSecret = this.readKeyCache.get(keyID);

    if (cachedSecret) {
      return cachedSecret;
    }

    if (!this.verified) {
      throw new Error(
        "CoValueCore: getUncachedReadKey called on coValue without verified state",
      );
    }

    // Getting the readKey from accounts
    if (this.verified.header.ruleset.type === "group") {
      const content = expectGroup(
        // load the account without private transactions, because we are here
        // to be able to decrypt those
        this.getCurrentContent({ ignorePrivateTransactions: true }),
      );

      return content.getReadKey(keyID);
    } else if (this.verified.header.ruleset.type === "ownedByGroup") {
      return expectGroup(
        this.node
          .expectCoValueLoaded(this.verified.header.ruleset.group)
          .getCurrentContent(),
      ).getReadKey(keyID);
    } else {
      throw new Error(
        "Only groups or values owned by groups have read secrets",
      );
    }
  }

  safeGetGroup(): RawGroup | undefined {
    if (!this.verified) {
      throw new Error(
        "CoValueCore: getGroup called on coValue without verified state",
      );
    }

    if (this.verified.header.ruleset.type !== "ownedByGroup") {
      return undefined;
    }

    return expectGroup(
      this.node
        .expectCoValueLoaded(this.verified.header.ruleset.group)
        .getCurrentContent(),
    );
  }

  getGroup(): RawGroup {
    const group = this.safeGetGroup();

    if (!group) {
      throw new Error("Only values owned by groups have groups");
    }

    return group;
  }

  getTx(txID: TransactionID): Transaction | undefined {
    return this.verified?.sessions.get(txID.sessionID)?.transactions[
      txID.txIndex
    ];
  }

  getDependedOnCoValues(): Set<RawCoID> {
    return this.dependencies;
  }

  waitForSync(options?: { timeout?: number }) {
    return this.node.syncManager.waitForSync(this.id, options?.timeout);
  }

  load(peers: PeerState[]) {
    this.loadFromStorage((found) => {
      // When found the load is triggered by handleNewContent
      if (!found) {
        this.loadFromPeers(peers);
      }
    });
  }

  loadFromStorage(done?: (found: boolean) => void) {
    const node = this.node;

    if (!node.storage) {
      done?.(false);
      return;
    }

    const currentState = this.peers.get("storage");

    if (currentState && currentState.type !== "unknown") {
      done?.(currentState.type === "available");
      return;
    }

    this.markPending("storage");
    node.storage.load(
      this.id,
      (data) => {
        node.syncManager.handleNewContent(data, "storage");
      },
      (found) => {
        done?.(found);

        if (!found) {
          this.markNotFoundInPeer("storage");
        }
      },
    );
  }

  loadFromPeers(peers: PeerState[]) {
    if (peers.length === 0) {
      return;
    }

    for (const peer of peers) {
      const currentState = this.peers.get(peer.id)?.type ?? "unknown";

      if (currentState === "unknown" || currentState === "unavailable") {
        this.markPending(peer.id);
        this.internalLoadFromPeer(peer);
      }
    }
  }

  internalLoadFromPeer(peer: PeerState) {
    if (peer.closed && !peer.persistent) {
      this.markNotFoundInPeer(peer.id);
      return;
    }

    /**
     * On reconnection persistent peers will automatically fire the load request
     * as part of the reconnection process.
     */
    if (!peer.closed) {
      peer.pushOutgoingMessage({
        action: "load",
        ...this.knownState(),
      });
      peer.trackLoadRequestSent(this.id);
    }

    return new Promise<void>((resolve) => {
      const markNotFound = () => {
        if (this.peers.get(peer.id)?.type === "pending") {
          logger.warn("Timeout waiting for peer to load coValue", {
            id: this.id,
            peerID: peer.id,
          });
          this.markNotFoundInPeer(peer.id);
        }
      };

      const timeout = setTimeout(markNotFound, CO_VALUE_LOADING_CONFIG.TIMEOUT);
      const removeCloseListener = peer.persistent
        ? undefined
        : peer.addCloseListener(markNotFound);

      const listener = (state: CoValueCore) => {
        const peerState = state.peers.get(peer.id);
        if (
          state.isAvailable() || // might have become available from another peer e.g. through handleNewContent
          peerState?.type === "available" ||
          peerState?.type === "errored" ||
          peerState?.type === "unavailable"
        ) {
          this.listeners.delete(listener);
          removeCloseListener?.();
          clearTimeout(timeout);
          resolve();
        }
      };

      this.listeners.add(listener);
      listener(this);
    });
  }
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
  signerID: SignerID | undefined;
};

export type TriedToAddTransactionsWithoutVerifiedStateErrpr = {
  type: "TriedToAddTransactionsWithoutVerifiedState";
  id: RawCoID;
};

export type TriedToAddTransactionsWithoutSignerIDError = {
  type: "TriedToAddTransactionsWithoutSignerID";
  id: RawCoID;
  sessionID: SessionID;
};

export type TryAddTransactionsError =
  | TriedToAddTransactionsWithoutVerifiedStateErrpr
  | TriedToAddTransactionsWithoutSignerIDError
  | ResolveAccountAgentError
  | InvalidHashError
  | InvalidSignatureError;
