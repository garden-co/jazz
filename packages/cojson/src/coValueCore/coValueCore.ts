import { UpDownCounter, ValueType, metrics } from "@opentelemetry/api";
import { SessionLog } from "cojson-core-wasm";
import { Result, err } from "neverthrow";
import { PeerState } from "../PeerState.js";
import { RawCoValue } from "../coValue.js";
import { ControlledAccountOrAgent, RawAccountID } from "../coValues/account.js";
import { RawGroup } from "../coValues/group.js";
import { CO_VALUE_LOADING_CONFIG, MAX_RECOMMENDED_TX_SIZE } from "../config.js";
import { coreToCoValue } from "../coreToCoValue.js";
import {
  CryptoProvider,
  Encrypted,
  Hash,
  KeyID,
  KeySecret,
  Signature,
  SignerID,
  StreamingHash,
} from "../crypto/crypto.js";
import {
  AgentID,
  RawCoID,
  SessionID,
  TransactionID,
  getParentGroupId,
  isParentGroupReference,
} from "../ids.js";
import { parseJSON, stableStringify } from "../jsonStringify.js";
import { JsonValue } from "../jsonValue.js";
import { LocalNode, ResolveAccountAgentError } from "../localNode.js";
import { logger } from "../logger.js";
import {
  MemberState,
  isKeyForKeyField,
  validationPass,
} from "../permissions.js";
import { CoValueKnownState, PeerID, emptyKnownState } from "../sync.js";
import { accountOrAgentIDfromSessionID } from "../typeUtils/accountOrAgentIDfromSessionID.js";
import { expectGroup } from "../typeUtils/expectGroup.js";
import { isAccountID } from "../typeUtils/isAccountID.js";
import { getDependedOnCoValuesFromRawData } from "./utils.js";
import { CoValueHeader, Transaction, VerifiedState } from "./verifiedState.js";

export function idforHeader(
  header: CoValueHeader,
  crypto: CryptoProvider,
): RawCoID {
  const hash = crypto.shortHash(header);
  return `co_z${hash.slice("shortHash_z".length)}`;
}

const readKeyCache = new WeakMap<CoValueCore, { [id: KeyID]: KeySecret }>();

export type AvailableCoValueCore = CoValueCore & { verified: VerifiedState };

export type ProcessedTransaction = {
  txID: TransactionID;
  tx: Transaction;
  valid: true | false | null;
  invalidReason?: string;
  changes: JsonValue[] | null;
  madeAt: number;
};

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

  processedSorted: ProcessedTransaction[] = [];
  nValidated: number = 0;
  nDecrypted: number = 0;
  // in Groups: own member state, in OwnedByGroup: group member state
  memberState: MemberState | undefined;

  private readonly peers = new Map<
    PeerID,
    | { type: "unknown" | "pending" | "available" | "unavailable" }
    | {
        type: "errored";
        error: TryAddTransactionsError;
      }
  >();

  // cached state and listeners
  private _cachedContent?: RawCoValue;
  private readonly listeners: Set<
    (core: CoValueCore, unsub: () => void) => void
  > = new Set();
  private _cachedDependentOn?: Set<RawCoID>;
  private counter: UpDownCounter;

  private constructor(
    init: { header: CoValueHeader } | { id: RawCoID },
    node: LocalNode,
  ) {
    this.crypto = node.crypto;
    if ("header" in init) {
      this.id = idforHeader(init.header, node.crypto);
      this._verified = new VerifiedState(
        this.id,
        node.crypto,
        init.header,
        new Map(),
      );
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

  isAvailable(): this is AvailableCoValueCore {
    return this.hasVerifiedContent() && this.missingDependencies.size === 0;
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

  markNotFoundInPeer(peerId: PeerID) {
    const previousState = this.loadingState;
    this.peers.set(peerId, { type: "unavailable" });
    this.updateCounter(previousState);
    this.notifyUpdate("immediate");
  }

  missingDependencies = new Set<RawCoID>();

  // Checks if the current CoValueCore is already a missing dependency of the given CoValueCore
  checkCircularDependencies(dependency: CoValueCore) {
    const visited = new Set<RawCoID>();
    const stack = [dependency];

    while (stack.length > 0) {
      const current = stack.pop();

      if (!current) {
        return true;
      }

      visited.add(current.id);

      for (const dependency of current.missingDependencies) {
        if (dependency === this.id) {
          return false;
        }

        if (!visited.has(dependency)) {
          stack.push(this.node.getCoValue(dependency));
        }
      }
    }

    return true;
  }

  markMissingDependency(dependency: RawCoID) {
    const value = this.node.getCoValue(dependency);

    if (value.isAvailable()) {
      this.missingDependencies.delete(dependency);
    } else if (this.checkCircularDependencies(value)) {
      const unsubscribe = value.subscribe(() => {
        if (value.isAvailable()) {
          this.missingDependencies.delete(dependency);
          unsubscribe();
        }

        if (this.isAvailable()) {
          this.notifyUpdate("immediate");
        }
      });

      this.missingDependencies.add(dependency);
    }
  }

  provideHeader(
    header: CoValueHeader,
    fromPeerId: PeerID,
    streamingKnownState?: CoValueKnownState["sessions"],
  ) {
    const previousState = this.loadingState;

    if (this._verified?.sessions.size) {
      throw new Error(
        "CoValueCore: provideHeader called on coValue with verified sessions present!",
      );
    }
    this._verified = new VerifiedState(
      this.id,
      this.node.crypto,
      header,
      new Map(),
      streamingKnownState,
    );

    this.peers.set(fromPeerId, { type: "available" });

    this.updateCounter(previousState);
    this.notifyUpdate("immediate");
  }

  internalMarkMagicallyAvailable(
    verified: VerifiedState,
    { forceOverwrite = false }: { forceOverwrite?: boolean } = {},
  ) {
    const previousState = this.loadingState;
    this.internalShamefullyCloneVerifiedStateFrom(verified, {
      forceOverwrite,
    });
    this.updateCounter(previousState);
    this.notifyUpdate("immediate");
  }

  markErrored(peerId: PeerID, error: TryAddTransactionsError) {
    const previousState = this.loadingState;
    this.peers.set(peerId, { type: "errored", error });
    this.updateCounter(previousState);
    this.notifyUpdate("immediate");
  }

  markPending(peerId: PeerID) {
    const previousState = this.loadingState;
    this.peers.set(peerId, { type: "pending" });
    this.updateCounter(previousState);
    this.notifyUpdate("immediate");
  }

  internalShamefullyCloneVerifiedStateFrom(
    state: VerifiedState,
    { forceOverwrite = false }: { forceOverwrite?: boolean } = {},
  ) {
    if (!forceOverwrite && this._verified?.sessions.size) {
      throw new Error(
        "CoValueCore: internalShamefullyCloneVerifiedStateFrom called on coValue with verified sessions present!",
      );
    }
    this._verified = state.clone();
    this._cachedContent = undefined;
    this._cachedDependentOn = undefined;
    for (const [sessionID, session] of this._verified.sessions.entries()) {
      this.createProcessedTransactions(
        sessionID,
        session.transactions,
        0,
        null,
      );
    }
    this.nValidated = 0;
    this.nDecrypted = 0;
  }

  internalShamefullyResetCachedContent() {
    this._cachedContent = undefined;
    this._cachedDependentOn = undefined;
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
          this._cachedContent = undefined;
          for (const tx of this.processedSorted) {
            tx.valid = null;
          }
          this.nValidated = 0;
          this.notifyUpdate("immediate");
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
      .getCurrentContent();
  }

  knownStateWithStreaming(): CoValueKnownState {
    if (this.isAvailable()) {
      return this.verified.knownStateWithStreaming();
    } else {
      return emptyKnownState(this.id);
    }
  }

  knownState(): CoValueKnownState {
    if (this.isAvailable()) {
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

  tryAddTransactions(
    sessionID: SessionID,
    newTransactions: Transaction[],
    givenExpectedNewHash: Hash | undefined,
    newSignature: Signature,
    notifyMode: "immediate" | "deferred",
    skipVerify: boolean = false,
    givenNewStreamingHash?: StreamingHash,
  ): Result<true, TryAddTransactionsError> {
    return this.node
      .resolveAccountAgent(
        accountOrAgentIDfromSessionID(sessionID),
        "Expected to know signer of transaction",
      )
      .andThen((agent) => {
        if (!this.verified) {
          return err({
            type: "TriedToAddTransactionsWithoutVerifiedState",
            id: this.id,
          } satisfies TriedToAddTransactionsWithoutVerifiedStateErrpr);
        }

        const signerID = this.crypto.getAgentSignerID(agent);

        const nTxBefore =
          this.verified.sessions.get(sessionID)?.transactions.length || 0;

        const result = this.verified.tryAddTransactions(
          sessionID,
          signerID,
          newTransactions,
          givenExpectedNewHash,
          newSignature,
          skipVerify,
          givenNewStreamingHash,
        );

        if (result.isOk()) {
          this.createProcessedTransactions(
            sessionID,
            newTransactions,
            nTxBefore,
            null,
          );

          if (
            this._cachedContent &&
            "processNewTransactions" in this._cachedContent &&
            typeof this._cachedContent.processNewTransactions === "function"
          ) {
            this._cachedContent.processNewTransactions();
          } else {
            this._cachedContent = undefined;
          }

          this._cachedDependentOn = undefined;

          this.notifyUpdate(notifyMode);
        }

        return result;
      });
  }

  createProcessedTransactions(
    sessionID: SessionID,
    newTransactions: Transaction[],
    startTxIndex: number,
    initialDecryptedChanges: JsonValue[][] | null,
  ) {
    for (const [idx, tx] of newTransactions.entries()) {
      const processed: ProcessedTransaction = {
        txID: {
          sessionID,
          txIndex: startTxIndex + idx,
        },
        tx,
        valid: null,
        changes: initialDecryptedChanges?.[idx] ?? null,
        madeAt: tx.madeAt,
      };

      if (tx.privacy === "trusting" && !processed.changes) {
        const changesString = tx.changes;
        try {
          processed.changes = parseJSON(changesString);
        } catch (e) {
          logger.error("Failed to parse trusting transaction on " + this.id, {
            err: e,
            txID: processed.txID,
            changes: changesString.slice(0, 50),
          });
          processed.valid = false;
          processed.invalidReason = "Failed to parse trusting changes";
        }
      }

      // insert into processedSorted by ascending madeAt, then sessionID, then txIndex
      const insertIndex = this.processedSorted.findIndex(
        (tx) =>
          tx.madeAt > processed.madeAt ||
          (tx.madeAt === processed.madeAt &&
            tx.txID.sessionID > processed.txID.sessionID) ||
          (tx.madeAt === processed.madeAt &&
            tx.txID.sessionID === processed.txID.sessionID &&
            tx.txID.txIndex > processed.txID.txIndex),
      );

      if (insertIndex === -1) {
        this.processedSorted.push(processed);
      } else {
        this.processedSorted.splice(insertIndex, 0, processed);
        this.nValidated = Math.min(this.nValidated ?? 0, insertIndex);
        this.nDecrypted = Math.min(this.nDecrypted ?? 0, insertIndex);
      }
    }
  }

  deferredUpdates = 0;
  nextDeferredNotify: Promise<void> | undefined;

  notifyUpdate(notifyMode: "immediate" | "deferred") {
    if (this.listeners.size === 0) {
      return;
    }

    if (notifyMode === "immediate") {
      for (const listener of this.listeners) {
        try {
          listener(this, () => {
            this.listeners.delete(listener);
          });
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
            for (const listener of this.listeners) {
              try {
                listener(this, () => {
                  this.listeners.delete(listener);
                });
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
  ): boolean {
    if (!this.verified) {
      throw new Error(
        "CoValueCore: makeTransaction called on coValue without verified state",
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

    const signerAgent = this.node.getCurrentAgent();

    let privacyMode:
      | { type: "private"; keyID: KeyID; keySecret: KeySecret }
      | { type: "trusting" };

    if (privacy === "private") {
      const { secret: keySecret, id: keyID } = this.getCurrentReadKey();

      if (!keySecret) {
        throw new Error("Can't make transaction without read key secret");
      }

      privacyMode = { type: "private", keyID, keySecret };
    } else {
      privacyMode = { type: "trusting" };
    }

    const nTxBefore =
      this.verified.sessions.get(sessionID)?.transactions.length || 0;

    const { transaction } = this.verified.makeNewTransaction(
      sessionID,
      signerAgent,
      changes,
      privacyMode,
    );

    this.createProcessedTransactions(sessionID, [transaction], nTxBefore, [
      changes,
    ]);

    this.node.syncManager.recordTransactionsSize([transaction], "local");
    void this.node.syncManager.requestCoValueSync(this);

    return true;
  }

  getCurrentContent(options?: {
    ignorePrivateTransactions: true;
  }): RawCoValue {
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

  validationPass() {
    validationPass(this);
    this.nValidated = this.processedSorted.length;
  }

  decryptionPass(options?: {
    ignorePrivateTransactions: boolean;
  }) {
    let continouslyDescryptedUntil = this.nDecrypted;
    for (let i = this.nDecrypted; i < this.processedSorted.length; i++) {
      const processed = this.processedSorted[i]!;

      if (!processed.changes && processed.valid === true) {
        if (processed.tx.privacy === "trusting") {
          logger.error(
            "Trusting transaction should already have changes set " + this.id,
            {
              txID: processed.txID,
              changes: processed.tx.changes.slice(0, 50),
            },
          );
          processed.valid = false;
          processed.invalidReason =
            "Trusting transaction should already have changes set";
          continue;
        } else {
          if (options?.ignorePrivateTransactions) {
            continue;
          }

          const readKey = this.getReadKey(processed.tx.keyUsed);

          if (!readKey) {
            continue;
          }

          const decryptedString = this.crypto.decryptRawForTransaction(
            processed.tx.encryptedChanges,
            readKey,
            {
              in: this.id,
              tx: processed.txID,
            },
          );

          if (!decryptedString) {
            processed.valid = false;
            processed.invalidReason =
              "Failed to decrypt private transaction despite having key";
            continue;
          }

          try {
            processed.changes = decryptedString && parseJSON(decryptedString);
          } catch (e) {
            logger.error("Failed to parse private transaction on " + this.id, {
              err: e,
              txID: processed.txID,
              changes: decryptedString?.slice(0, 50),
            });
            processed.valid = false;
            processed.invalidReason = "Failed to parse private changes";
            continue;
          }
        }
      }
    }
    this.nDecrypted = continouslyDescryptedUntil;
  }

  getValidDecryptedTransactions(options?: {
    ignorePrivateTransactions: boolean;
    knownTransactions?: CoValueKnownState["sessions"];
  }): (ProcessedTransaction & {
    valid: true;
    changes: JsonValue[];
  })[] {
    this.validationPass();
    this.decryptionPass(options);

    return this.processedSorted.filter(
      (tx) =>
        tx.valid === true &&
        tx.changes !== null &&
        (options?.ignorePrivateTransactions
          ? tx.tx.privacy !== "private"
          : true) &&
        (options?.knownTransactions
          ? tx.txID.txIndex >
            (options.knownTransactions[tx.txID.sessionID] ?? -1)
          : true),
    ) as (ProcessedTransaction & {
      valid: true;
      changes: JsonValue[];
    })[];
  }

  compareTransactions(
    a: Pick<ProcessedTransaction, "madeAt" | "txID">,
    b: Pick<ProcessedTransaction, "madeAt" | "txID">,
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
    if (!this.verified) {
      throw new Error(
        "CoValueCore: getUncachedReadKey called on coValue without verified state",
      );
    }

    if (this.verified.header.ruleset.type === "group") {
      const content = expectGroup(
        this.getCurrentContent({ ignorePrivateTransactions: true }), // to prevent recursion
      );
      const keyForEveryone = content.get(`${keyID}_for_everyone`);
      if (keyForEveryone) {
        return keyForEveryone;
      }

      // Try to find key revelation for us
      const currentAgentOrAccountID = accountOrAgentIDfromSessionID(
        this.node.currentSessionID,
      );

      // being careful here to avoid recursion
      const lookupAccountOrAgentID = isAccountID(currentAgentOrAccountID)
        ? this.id === currentAgentOrAccountID
          ? this.crypto.getAgentID(this.node.agentSecret) // in accounts, the read key is revealed for the primitive agent
          : currentAgentOrAccountID // current account ID
        : currentAgentOrAccountID; // current agent ID

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
          this.crypto.getAgentSealerSecret(this.node.agentSecret), // being careful here to avoid recursion
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
    } else if (this.verified.header.ruleset.type === "ownedByGroup") {
      return this.node
        .expectCoValueLoaded(this.verified.header.ruleset.group)
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
    if (!this.verified) {
      throw new Error(
        "CoValueCore: getGroup called on coValue without verified state",
      );
    }

    if (this.verified.header.ruleset.type !== "ownedByGroup") {
      throw new Error("Only values owned by groups have groups");
    }

    return expectGroup(
      this.node
        .expectCoValueLoaded(this.verified.header.ruleset.group)
        .getCurrentContent(),
    );
  }

  getTx(txID: TransactionID): Transaction | undefined {
    return this.verified?.sessions.get(txID.sessionID)?.transactions[
      txID.txIndex
    ];
  }

  getDependedOnCoValues(): Set<RawCoID> {
    if (this._cachedDependentOn) {
      return this._cachedDependentOn;
    } else {
      if (!this.verified) {
        return new Set();
      }

      const dependentOn = getDependedOnCoValuesFromRawData(
        this.id,
        this.verified.header,
        this.verified.sessions.keys(),
        Array.from(
          this.verified.sessions.values(),
          (session) => session.transactions,
        ),
      );
      this._cachedDependentOn = dependentOn;
      return dependentOn;
    }
  }

  waitForSync(options?: {
    timeout?: number;
  }) {
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
        if (!found) {
          this.markNotFoundInPeer("storage");
        }

        done?.(found);
      },
    );
  }

  loadFromPeers(peers: PeerState[]) {
    if (peers.length === 0) {
      return;
    }

    const peersToActuallyLoadFrom = [] as PeerState[];

    for (const peer of peers) {
      const currentState = this.peers.get(peer.id)?.type;

      if (
        !currentState ||
        currentState === "unknown" ||
        currentState === "unavailable"
      ) {
        peersToActuallyLoadFrom.push(peer);
        this.markPending(peer.id);
      }
    }

    for (const peer of peersToActuallyLoadFrom) {
      this.internalLoadFromPeer(peer);
    }
  }

  internalLoadFromPeer(peer: PeerState) {
    if (peer.closed) {
      this.markNotFoundInPeer(peer.id);
      return;
    }

    peer.pushOutgoingMessage({
      action: "load",
      ...this.knownState(),
    });
    peer.trackLoadRequestSent(this.id);

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
      const removeCloseListener = peer.addCloseListener(markNotFound);

      const listener = (state: CoValueCore) => {
        const peerState = state.peers.get(peer.id);
        if (
          state.isAvailable() || // might have become available from another peer e.g. through handleNewContent
          peerState?.type === "available" ||
          peerState?.type === "errored" ||
          peerState?.type === "unavailable"
        ) {
          this.listeners.delete(listener);
          removeCloseListener();
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
  signerID: SignerID;
  error: Error;
};

export type TriedToAddTransactionsWithoutVerifiedStateErrpr = {
  type: "TriedToAddTransactionsWithoutVerifiedState";
  id: RawCoID;
};

export type TryAddTransactionsError =
  | TriedToAddTransactionsWithoutVerifiedStateErrpr
  | ResolveAccountAgentError
  | InvalidHashError
  | InvalidSignatureError;
