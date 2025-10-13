import { CoID, RawCoValue } from "../coValue.js";
import { AvailableCoValueCore } from "../coValueCore/coValueCore.js";
import { AgentID, SessionID, TransactionID } from "../ids.js";
import { JsonObject, JsonValue } from "../jsonValue.js";
import { accountOrAgentIDfromSessionID } from "../typeUtils/accountOrAgentIDfromSessionID.js";
import { isCoValue } from "../typeUtils/isCoValue.js";
import { RawAccountID } from "./account.js";
import { RawGroup } from "./group.js";
import { Transaction } from "../coValueCore/verifiedState.js";

export type OpID = TransactionID & { changeIdx: number };

export type InsertionOpPayload<T extends JsonValue> =
  | {
      op: "pre";
      value: T;
      before: OpID | "end";
    }
  | {
      op: "app";
      value: T;
      after: OpID | "start";
    };

export type DeletionOpPayload = {
  op: "del";
  insertion: OpID;
};

export type ListOpPayload<T extends JsonValue> =
  | InsertionOpPayload<T>
  | DeletionOpPayload;

type InsertionEntry<T extends JsonValue> = {
  madeAt: number;
  predecessors: OpID[];
  successors: OpID[];
  change: InsertionOpPayload<T>;
};

type DeletionEntry = {
  madeAt: number;
  deletionID: OpID;
  change: DeletionOpPayload;
};

export class RawCoList<
  Item extends JsonValue = JsonValue,
  Meta extends JsonObject | null = null,
> implements RawCoValue
{
  /** @category 6. Meta */
  id: CoID<this>;
  /** @category 6. Meta */
  type: "colist" | "coplaintext" = "colist" as const;
  /** @category 6. Meta */
  core: AvailableCoValueCore;
  /** @internal */
  afterStart: OpID[];
  /** @internal */
  beforeEnd: OpID[];
  /** @internal */
  insertions: {
    [sessionID: SessionID]: {
      [txIdx: number]: {
        [changeIdx: number]: InsertionEntry<Item>;
      };
    };
  };
  /** @internal */
  deletionsByInsertion: {
    [deletedSessionID: SessionID]: {
      [deletedTxIdx: number]: {
        [deletedChangeIdx: number]: DeletionEntry[];
      };
    };
  };
  /** @category 6. Meta */
  readonly _item!: Item;

  /** @internal */
  _cachedEntries?: {
    value: Item;
    madeAt: number;
    opID: OpID;
  }[];
  /** @internal */
  knownTransactions: Set<Transaction>;
  /** @internal - Map from chain start key to chain info */
  _linearChains: Map<
    string,
    {
      opIDs: OpID[];
      lastOpID: OpID;
    }
  >;
  /** @internal - Map from OpID key to chain start key */
  _opIDToChain: Map<string, string>;
  /** @internal - Set of chain start keys */
  _chainStarts: Set<string>;

  get totalValidTransactions() {
    return this.knownTransactions.size;
  }

  lastValidTransaction: number | undefined;

  /** @internal */
  constructor(core: AvailableCoValueCore) {
    this.id = core.id as CoID<this>;
    this.core = core;

    this.insertions = {};
    this.deletionsByInsertion = {};
    this.afterStart = [];
    this.beforeEnd = [];
    this.knownTransactions = new Set<Transaction>();
    this._linearChains = new Map();
    this._opIDToChain = new Map();
    this._chainStarts = new Set();

    this.processNewTransactions();
  }

  private getInsertionsEntry(opID: OpID) {
    const index = getSessionIndex(opID);

    const sessionEntry = this.insertions[index];
    if (!sessionEntry) {
      return undefined;
    }

    const txEntry = sessionEntry[opID.txIndex];
    if (!txEntry) {
      return undefined;
    }

    return txEntry[opID.changeIdx];
  }

  private createInsertionsEntry(opID: OpID, value: InsertionEntry<Item>) {
    const index = getSessionIndex(opID);

    let sessionEntry = this.insertions[index];
    if (!sessionEntry) {
      sessionEntry = {};
      this.insertions[index] = sessionEntry;
    }

    let txEntry = sessionEntry[opID.txIndex];
    if (!txEntry) {
      txEntry = {};
      sessionEntry[opID.txIndex] = txEntry;
    }

    // Check if the change index already exists, may be the case of double merges
    if (txEntry[opID.changeIdx]) {
      return false;
    }

    txEntry[opID.changeIdx] = value;
    return true;
  }

  private isDeleted(opID: OpID) {
    const index = getSessionIndex(opID);

    const sessionEntry = this.deletionsByInsertion[index];

    if (!sessionEntry) {
      return false;
    }

    const txEntry = sessionEntry[opID.txIndex];

    if (!txEntry) {
      return false;
    }

    return Boolean(txEntry[opID.changeIdx]?.length);
  }

  private pushDeletionsByInsertionEntry(opID: OpID, value: DeletionEntry) {
    const index = getSessionIndex(opID);

    let sessionEntry = this.deletionsByInsertion[index];
    if (!sessionEntry) {
      sessionEntry = {};
      this.deletionsByInsertion[index] = sessionEntry;
    }

    let txEntry = sessionEntry[opID.txIndex];
    if (!txEntry) {
      txEntry = {};
      sessionEntry[opID.txIndex] = txEntry;
    }

    let list = txEntry[opID.changeIdx];

    if (!list) {
      list = [];
      txEntry[opID.changeIdx] = list;
    }

    list.push(value);
  }

  processNewTransactions() {
    const transactions = this.core.getValidSortedTransactions({
      ignorePrivateTransactions: false,
      knownTransactions: this.knownTransactions,
    });

    if (transactions.length === 0) {
      return;
    }

    let lastValidTransaction: number | undefined = undefined;
    let oldestValidTransaction: number | undefined = undefined;
    this._cachedEntries = undefined;

    for (const { txID, changes, madeAt } of transactions) {
      lastValidTransaction = Math.max(lastValidTransaction ?? 0, madeAt);
      oldestValidTransaction = Math.min(
        oldestValidTransaction ?? Infinity,
        madeAt,
      );

      for (const [changeIdx, changeUntyped] of changes.entries()) {
        const change = changeUntyped as ListOpPayload<Item>;

        const opID = {
          sessionID: txID.sessionID,
          txIndex: txID.txIndex,
          branch: txID.branch,
          changeIdx,
        };

        if (change.op === "pre" || change.op === "app") {
          const created = this.createInsertionsEntry(opID, {
            madeAt,
            predecessors: [],
            successors: [],
            change,
          });

          // If the change index already exists, we don't need to process it again
          if (!created) {
            continue;
          }

          if (change.op === "pre") {
            if (change.before === "end") {
              this.beforeEnd.push(opID);
            } else {
              const beforeEntry = this.getInsertionsEntry(change.before);

              if (!beforeEntry) {
                continue;
              }

              beforeEntry.predecessors.push(opID);
              // Update chains incrementally
              this.updateChainsAfterInsertion(change.before, opID, "pre");
            }
          } else {
            if (change.after === "start") {
              this.afterStart.push(opID);
            } else {
              const afterEntry = this.getInsertionsEntry(change.after);

              if (!afterEntry) {
                continue;
              }

              afterEntry.successors.push(opID);
              // Update chains incrementally
              this.updateChainsAfterInsertion(change.after, opID, "app");
            }
          }
        } else if (change.op === "del") {
          this.pushDeletionsByInsertionEntry(change.insertion, {
            madeAt,
            deletionID: opID,
            change,
          });
        } else {
          throw new Error(
            "Unknown list operation " + (change as { op: unknown }).op,
          );
        }
      }
    }

    if (
      this.lastValidTransaction &&
      oldestValidTransaction &&
      oldestValidTransaction < this.lastValidTransaction
    ) {
      this.rebuildFromCore();
    } else {
      this.lastValidTransaction = lastValidTransaction;
    }
  }

  /** @category 6. Meta */
  get headerMeta(): Meta {
    return this.core.verified.header.meta as Meta;
  }

  /**
   * Analizza la struttura del grafo per identificare opportunità di compattazione.
   * Utile per debugging e ottimizzazione.
   * Le catene lineari vengono mantenute incrementalmente per performance ottimali.
   *
   * @category 6. Meta
   */
  getCompactionStats(): {
    totalNodes: number;
    linearChains: number;
    compactableNodes: number;
    avgChainLength: number;
    maxChainLength: number;
    compactionRatio: number;
  } {
    // Count total nodes by iterating through insertions
    let totalNodes = 0;
    for (const sessionID in this.insertions) {
      const sessionEntry = this.insertions[sessionID as SessionID];
      for (const txIdx in sessionEntry) {
        const txEntry = sessionEntry[Number(txIdx)];
        for (const changeIdx in txEntry) {
          totalNodes++;
        }
      }
    }

    // Use pre-computed chains
    const linearChains = this._linearChains.size;
    let compactableNodes = 0;
    let maxChainLength = 0;

    for (const chain of this._linearChains.values()) {
      compactableNodes += chain.opIDs.length;
      maxChainLength = Math.max(maxChainLength, chain.opIDs.length);
    }

    return {
      totalNodes,
      linearChains,
      compactableNodes,
      avgChainLength: linearChains > 0 ? compactableNodes / linearChains : 0,
      maxChainLength,
      compactionRatio:
        totalNodes > 0
          ? (totalNodes - compactableNodes + linearChains) / totalNodes
          : 1,
    };
  }

  /** @category 6. Meta */
  get group(): RawGroup {
    return this.core.getGroup();
  }

  /**
   * Not yet implemented
   *
   * @category 4. Time travel
   */
  atTime(_time: number): this {
    throw new Error("Not yet implemented");
  }

  /**
   * Get the item currently at `idx`.
   *
   * @category 1. Reading
   */
  get(idx: number): Item | undefined {
    const entry = this.entries()[idx];
    if (!entry) {
      return undefined;
    }
    return entry.value;
  }

  /**
   * Returns the current items in the CoList as an array.
   *
   * @category 1. Reading
   **/
  asArray(): Item[] {
    return this.entries().map((entry) => entry.value);
  }

  /** @internal */
  entries(): {
    value: Item;
    madeAt: number;
    opID: OpID;
  }[] {
    if (this._cachedEntries) {
      return this._cachedEntries;
    }
    const arr = this.entriesUncached();
    this._cachedEntries = arr;
    return arr;
  }

  /** @internal */
  entriesUncached(): {
    value: Item;
    madeAt: number;
    opID: OpID;
  }[] {
    const arr: {
      value: Item;
      madeAt: number;
      opID: OpID;
    }[] = [];
    for (const opID of this.afterStart) {
      this.fillArrayFromOpID(opID, arr);
    }
    for (const opID of this.beforeEnd) {
      this.fillArrayFromOpID(opID, arr);
    }
    return arr;
  }

  /** @internal - Update chains incrementally when a new insertion happens */
  private updateChainsAfterInsertion(
    adjacentOpID: OpID,
    newOpID: OpID,
    insertionType: "pre" | "app",
  ) {
    const adjacentEntry = this.getInsertionsEntry(adjacentOpID);
    const newEntry = this.getInsertionsEntry(newOpID);

    if (!adjacentEntry || !newEntry) return;

    // Check if we can form/extend a chain
    // IMPORTANT: Only form chains for append operations to avoid issues with mixed prepend/append
    const canFormChain =
      insertionType === "app"
        ? // For append: adjacent should have only this as successor, new should have no other predecessors
          // AND adjacent should not be a predecessor of other nodes (which would make topology complex)
          adjacentEntry.successors.length === 1 &&
          newEntry.predecessors.length === 0 && // Stricter: new node must have NO predecessors
          newEntry.successors.length === 0 // And NO successors initially
        : // For prepend: DISABLED - don't create chains with prepend to avoid topology issues
          false;

    if (!canFormChain) {
      // Can't form a chain, remove any existing chains that are broken
      this.removeFromChain(adjacentOpID);
      return;
    }

    const adjacentKey = opIDKey(adjacentOpID);
    const newKey = opIDKey(newOpID);

    if (insertionType === "app") {
      // We're appending newOpID after adjacentOpID
      const existingChainKey = this._opIDToChain.get(adjacentKey);

      if (existingChainKey) {
        // Adjacent is in a chain, extend it
        const chain = this._linearChains.get(existingChainKey);
        if (chain) {
          chain.opIDs.push(newOpID);
          chain.lastOpID = newOpID;
          this._opIDToChain.set(newKey, existingChainKey);
        }
      } else {
        // Start a new chain - use adjacent key as the chain key
        const chainKey = adjacentKey;
        this._linearChains.set(chainKey, {
          opIDs: [adjacentOpID, newOpID],
          lastOpID: newOpID,
        });
        this._opIDToChain.set(adjacentKey, chainKey);
        this._opIDToChain.set(newKey, chainKey);
        this._chainStarts.add(chainKey);
      }
    } else {
      // insertionType === "pre"
      // We're prepending newOpID before adjacentOpID
      const existingChainKey = this._opIDToChain.get(adjacentKey);

      if (existingChainKey) {
        // Adjacent is in a chain
        const chain = this._linearChains.get(existingChainKey);
        if (chain) {
          // If adjacent is the first node of the chain, we need to create a new chain
          if (opIDKey(chain.opIDs[0]!) === adjacentKey) {
            // Create new chain with newOpID as first
            const newChainKey = newKey;
            this._linearChains.set(newChainKey, {
              opIDs: [newOpID, ...chain.opIDs],
              lastOpID: chain.lastOpID,
            });

            // Update all mappings
            for (const opID of chain.opIDs) {
              this._opIDToChain.set(opIDKey(opID), newChainKey);
            }
            this._opIDToChain.set(newKey, newChainKey);

            // Update chain starts
            this._chainStarts.delete(existingChainKey);
            this._chainStarts.add(newChainKey);

            // Remove old chain
            this._linearChains.delete(existingChainKey);
          } else {
            // Adjacent is not the first, chain is broken
            this.removeFromChain(adjacentOpID);
          }
        }
      } else {
        // Start a new chain (prepend order) - use new key as the chain key
        const chainKey = newKey;
        this._linearChains.set(chainKey, {
          opIDs: [newOpID, adjacentOpID],
          lastOpID: adjacentOpID,
        });
        this._opIDToChain.set(newKey, chainKey);
        this._opIDToChain.set(adjacentKey, chainKey);
        this._chainStarts.add(chainKey);
      }
    }
  }

  /** @internal - Remove an OpID from its chain */
  private removeFromChain(opID: OpID) {
    const key = opIDKey(opID);
    const chainKey = this._opIDToChain.get(key);

    if (chainKey) {
      const chain = this._linearChains.get(chainKey);
      if (chain) {
        // Remove all OpIDs from the chain
        for (const chainOpID of chain.opIDs) {
          this._opIDToChain.delete(opIDKey(chainOpID));
        }
        this._linearChains.delete(chainKey);
        this._chainStarts.delete(chainKey);
      }
    }
  }

  /** @internal */
  private fillArrayFromOpID(
    opID: OpID,
    arr: {
      value: Item;
      madeAt: number;
      opID: OpID;
    }[],
  ) {
    const todo = [opID]; // a stack with the next item to do at the end
    const predecessorsVisited = new Set<string>();
    const processedChains = new Set<string>();

    while (todo.length > 0) {
      const currentOpID = todo[todo.length - 1]!;

      const entry = this.getInsertionsEntry(currentOpID);

      if (!entry) {
        throw new Error("Missing op " + opIDKey(currentOpID));
      }

      const currentKey = opIDKey(currentOpID);
      const shouldTraversePredecessors =
        entry.predecessors.length > 0 && !predecessorsVisited.has(currentKey);

      // We navigate the predecessors before processing the current opID in the list
      if (shouldTraversePredecessors) {
        for (const predecessor of entry.predecessors) {
          todo.push(predecessor);
        }
        predecessorsVisited.add(currentKey);
      } else {
        // Remove the current opID from the todo stack to consider it processed.
        todo.pop();

        // Check if this opID is the start of a pre-computed chain (fast O(1) lookup)
        const isChainStart = this._chainStarts.has(currentKey);

        if (isChainStart && !processedChains.has(currentKey)) {
          // Process entire chain at once
          const chain = this._linearChains.get(currentKey)!;
          processedChains.add(currentKey);

          for (const chainOpID of chain.opIDs) {
            const chainEntry = this.getInsertionsEntry(chainOpID);
            if (!chainEntry) continue;

            const deleted = this.isDeleted(chainOpID);
            if (!deleted) {
              arr.push({
                value: chainEntry.change.value,
                madeAt: chainEntry.madeAt,
                opID: chainOpID,
              });
            }
          }

          // Add successors of the last node in the chain
          const lastEntry = this.getInsertionsEntry(chain.lastOpID);
          if (lastEntry) {
            for (const successor of lastEntry.successors) {
              todo.push(successor);
            }
          }
        } else {
          // Check if this node is part of a chain that was already processed
          const chainKey = this._opIDToChain.get(currentKey);
          if (chainKey && processedChains.has(chainKey)) {
            // Skip, already processed as part of a chain
            continue;
          }
          // Single node (not in a chain, or chain already processed)
          const deleted = this.isDeleted(currentOpID);

          if (!deleted) {
            arr.push({
              value: entry.change.value,
              madeAt: entry.madeAt,
              opID: currentOpID,
            });
          }

          // traverse successors in reverse for correct insertion behavior
          for (const successor of entry.successors) {
            todo.push(successor);
          }
        }
      }
    }
  }

  /**
   * Returns the current items in the CoList as an array. (alias of `asArray`)
   *
   * @category 1. Reading
   */
  toJSON(): Item[] {
    return this.asArray();
  }

  /** @category 5. Edit history */
  editAt(idx: number):
    | {
        by: RawAccountID | AgentID;
        tx: TransactionID;
        at: Date;
        value: Item;
      }
    | undefined {
    const entry = this.entries()[idx];
    if (!entry) {
      return undefined;
    }
    const madeAt = new Date(entry.madeAt);
    const by = accountOrAgentIDfromSessionID(entry.opID.sessionID);
    const value = entry.value;
    return {
      by,
      tx: {
        sessionID: entry.opID.sessionID,
        txIndex: entry.opID.txIndex,
      },
      at: madeAt,
      value,
    };
  }

  /** @category 5. Edit history */
  deletionEdits(): {
    by: RawAccountID | AgentID;
    tx: TransactionID;
    at: Date;
    // TODO: add indices that are now before and after the deleted item
  }[] {
    const edits: {
      by: RawAccountID | AgentID;
      tx: TransactionID;
      at: Date;
    }[] = [];

    for (const sessionID in this.deletionsByInsertion) {
      const sessionEntry = this.deletionsByInsertion[sessionID as SessionID];
      for (const txIdx in sessionEntry) {
        const txEntry = sessionEntry[Number(txIdx)];
        for (const changeIdx in txEntry) {
          const changeEntry = txEntry[Number(changeIdx)];
          for (const deletion of changeEntry || []) {
            const madeAt = new Date(deletion.madeAt);
            const by = accountOrAgentIDfromSessionID(
              deletion.deletionID.sessionID,
            );
            edits.push({
              by,
              tx: deletion.deletionID,
              at: madeAt,
            });
          }
        }
      }
    }

    return edits;
  }

  /** @category 3. Subscription */
  subscribe(listener: (coList: this) => void): () => void {
    return this.core.subscribe((core) => {
      listener(core.getCurrentContent() as this);
    });
  }

  /** Appends `item` after the item currently at index `after`.
   *
   * If `privacy` is `"private"` **(default)**, `item` is encrypted in the transaction, only readable by other members of the group this `CoList` belongs to. Not even sync servers can see the content in plaintext.
   *
   * If `privacy` is `"trusting"`, `item` is stored in plaintext in the transaction, visible to everyone who gets a hold of it, including sync servers.
   *
   * @category 2. Editing
   **/
  append(
    item: Item,
    after?: number,
    privacy: "private" | "trusting" = "private",
  ) {
    this.appendItems([item], after, privacy);
  }

  /**
   * Appends `items` to the list at index `after`. If `after` is negative, it is treated as `0`.
   *
   * If `privacy` is `"private"` **(default)**, `items` are encrypted in the transaction, only readable by other members of the group this `CoList` belongs to. Not even sync servers can see the content in plaintext.
   *
   * If `privacy` is `"trusting"`, `items` are stored in plaintext in the transaction, visible to everyone who gets a hold of it, including sync servers.
   *
   * @category 2. Editing
   */
  appendItems(
    items: Item[],
    after?: number,
    privacy: "private" | "trusting" = "private",
  ) {
    const entries = this.entries();
    after =
      after === undefined
        ? entries.length > 0
          ? entries.length - 1
          : 0
        : Math.max(0, after);
    let opIDBefore: OpID | "start";
    if (entries.length > 0) {
      const entryBefore = entries[after];
      if (!entryBefore) {
        throw new Error("Invalid index " + after);
      }
      opIDBefore = entryBefore.opID;
    } else {
      if (after !== 0) {
        throw new Error("Invalid index " + after);
      }
      opIDBefore = "start";
    }

    const changes = items.map((item) => ({
      op: "app",
      value: isCoValue(item) ? item.id : item,
      after: opIDBefore,
    }));

    if (opIDBefore !== "start") {
      // When added as successors we need to reverse the items
      // to keep the same insertion order
      changes.reverse();
    }

    this.core.makeTransaction(changes, privacy);
    this.processNewTransactions();
  }

  /**
   * Prepends `item` before the item currently at index `before`.
   *
   * If `privacy` is `"private"` **(default)**, `item` is encrypted in the transaction, only readable by other members of the group this `CoList` belongs to. Not even sync servers can see the content in plaintext.
   *
   * If `privacy` is `"trusting"`, `item` is stored in plaintext in the transaction, visible to everyone who gets a hold of it, including sync servers.
   *
   * @category 2. Editing
   */
  prepend(
    item: Item,
    before?: number,
    privacy: "private" | "trusting" = "private",
  ) {
    const entries = this.entries();
    before = before === undefined ? 0 : before;
    let opIDAfter;
    if (entries.length > 0) {
      const entryAfter = entries[before];
      if (entryAfter) {
        opIDAfter = entryAfter.opID;
      } else {
        if (before !== entries.length) {
          throw new Error("Invalid index " + before);
        }
        opIDAfter = "end";
      }
    } else {
      if (before !== 0) {
        throw new Error("Invalid index " + before);
      }
      opIDAfter = "end";
    }
    this.core.makeTransaction(
      [
        {
          op: "pre",
          value: isCoValue(item) ? item.id : item,
          before: opIDAfter,
        },
      ],
      privacy,
    );

    this.processNewTransactions();
  }

  /** Deletes the item at index `at`.
   *
   * If `privacy` is `"private"` **(default)**, the fact of this deletion is encrypted in the transaction, only readable by other members of the group this `CoList` belongs to. Not even sync servers can see the content in plaintext.
   *
   * If `privacy` is `"trusting"`, the fact of this deletion is stored in plaintext in the transaction, visible to everyone who gets a hold of it, including sync servers.
   *
   * @category 2. Editing
   **/
  delete(at: number, privacy: "private" | "trusting" = "private") {
    const entries = this.entries();
    const entry = entries[at];
    if (!entry) {
      throw new Error("Invalid index " + at);
    }
    this.core.makeTransaction(
      [
        {
          op: "del",
          insertion: entry.opID,
        },
      ],
      privacy,
    );

    this.processNewTransactions();
  }

  replace(
    at: number,
    newItem: Item,
    privacy: "private" | "trusting" = "private",
  ) {
    const entries = this.entries();
    const entry = entries[at];
    if (!entry) {
      throw new Error("Invalid index " + at);
    }

    this.core.makeTransaction(
      [
        {
          op: "app",
          value: isCoValue(newItem) ? newItem.id : newItem,
          after: entry.opID,
        },
        {
          op: "del",
          insertion: entry.opID,
        },
      ],
      privacy,
    );
    this.processNewTransactions();
  }

  /** @internal */
  rebuildFromCore() {
    const listAfter = new RawCoList(this.core) as this;

    this.afterStart = listAfter.afterStart;
    this.beforeEnd = listAfter.beforeEnd;
    this.insertions = listAfter.insertions;
    this.lastValidTransaction = listAfter.lastValidTransaction;
    this.knownTransactions = listAfter.knownTransactions;
    this.deletionsByInsertion = listAfter.deletionsByInsertion;
    this._linearChains = listAfter._linearChains;
    this._opIDToChain = listAfter._opIDToChain;
    this._chainStarts = listAfter._chainStarts;
    this._cachedEntries = undefined;
  }
}

function getSessionIndex(txID: TransactionID): SessionID {
  if (txID.branch) {
    return `${txID.sessionID}_branch_${txID.branch}`;
  }
  return txID.sessionID;
}

function opIDKey(opID: OpID): string {
  const sessionIndex = getSessionIndex(opID);
  return `${sessionIndex}_${opID.txIndex}_${opID.changeIdx}`;
}
