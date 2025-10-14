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

/** @internal */
type TodoNode = {
  value: OpID;
  predecessorsVisited: boolean;
  next: TodoNode | null;
};

/** @internal */
class CoListTraversalLinkedList {
  private garbage: TodoNode | null = null;

  /** Get an object from the pool or create a new one, initialized with the provided values */
  newNode(value: OpID, next: TodoNode | null = null): TodoNode {
    if (this.garbage) {
      const item = this.garbage;
      this.garbage = this.garbage.next;
      this.garbage = null;
      item.value = value;
      item.predecessorsVisited = false;
      item.next = next;
      return item;
    }
    return { value, predecessorsVisited: false, next };
  }

  /** Return an object to the pool for reuse */
  recycleNode(item: TodoNode): void {
    item.next = this.garbage;
    this.garbage = item;
  }
}

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

  get totalValidTransactions() {
    return this.knownTransactions.size;
  }

  lastValidTransaction: number | undefined;

  #insertionsLookup: Map<OpID, InsertionEntry<Item>> = new Map();

  /** @internal */
  constructor(core: AvailableCoValueCore) {
    this.id = core.id as CoID<this>;
    this.core = core;

    this.insertions = {};
    this.deletionsByInsertion = {};
    this.afterStart = [];
    this.beforeEnd = [];
    this.knownTransactions = new Set<Transaction>();

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
    this.#insertionsLookup.set(opID, value);
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
    this.#cachedArray = undefined;
    this.#cachedOpIDs = undefined;

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
    return this.asArray()[idx];
  }

  #cachedArray: Item[] | undefined;
  #cachedOpIDs: OpID[] | undefined;

  /**
   * Returns the current items in the CoList as an array.
   *
   * @category 1. Reading
   **/
  asArray(): Item[] {
    if (this.#cachedArray) {
      return this.#cachedArray;
    }

    this.buildEntries();

    return this.#cachedArray!;
  }

  getOpIDs(): OpID[] {
    if (this.#cachedOpIDs) {
      return this.#cachedOpIDs;
    }
    this.buildEntries();
    return this.#cachedOpIDs!;
  }

  /** @internal */
  buildEntries() {
    const values: Item[] = [];
    const opIDs: OpID[] = [];

    for (const opID of this.afterStart) {
      this.fillArrayFromOpID(opID, values, opIDs);
    }
    for (const opID of this.beforeEnd) {
      this.fillArrayFromOpID(opID, values, opIDs);
    }

    this.#cachedArray = values;
    this.#cachedOpIDs = opIDs;
  }

  /** @internal */
  private fillArrayFromOpID(opID: OpID, values: Item[], opIDs: OpID[]) {
    const todoList = new CoListTraversalLinkedList();
    let head: TodoNode | null = todoList.newNode(opID);

    while (head !== null) {
      const todo = head;
      const currentOpID = todo.value;

      const entry =
        this.#insertionsLookup.get(currentOpID) ??
        this.getInsertionsEntry(currentOpID);

      if (!entry) {
        throw new Error("Missing op " + currentOpID);
      }

      const predecessorsVisited = todo.predecessorsVisited;
      const shouldTraversePredecessors =
        entry.predecessors.length > 0 && !predecessorsVisited;

      // We navigate the predecessors before processing the current opID in the list
      if (shouldTraversePredecessors) {
        todo.predecessorsVisited = true;

        for (const predecessor of entry.predecessors) {
          head = todoList.newNode(predecessor, head);
        }
      } else {
        // Remove the current opID from the todo stack to consider it processed.
        todoList.recycleNode(head);
        head = head.next;

        const deleted = this.isDeleted(currentOpID);

        if (!deleted) {
          values.push(entry.change.value);
          opIDs.push(currentOpID);
        }

        // traverse successors in reverse for correct insertion behavior
        for (const successor of entry.successors) {
          head = todoList.newNode(successor, head);
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
    const entries = this.getOpIDs();
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
      opIDBefore = entryBefore;
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
    const entries = this.getOpIDs();
    before = before === undefined ? 0 : before;
    let opIDAfter;
    if (entries.length > 0) {
      const entryAfter = entries[before];
      if (entryAfter) {
        opIDAfter = entryAfter;
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
    const entries = this.getOpIDs();
    const entry = entries[at];
    if (!entry) {
      throw new Error("Invalid index " + at);
    }
    this.core.makeTransaction(
      [
        {
          op: "del",
          insertion: entry,
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
    const entries = this.getOpIDs();
    const entry = entries[at];
    if (!entry) {
      throw new Error("Invalid index " + at);
    }

    this.core.makeTransaction(
      [
        {
          op: "app",
          value: isCoValue(newItem) ? newItem.id : newItem,
          after: entry,
        },
        {
          op: "del",
          insertion: entry,
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
    this._cachedEntries = undefined;
  }
}

function getSessionIndex(txID: TransactionID): SessionID {
  if (txID.branch) {
    return `${txID.sessionID}_branch_${txID.branch}`;
  }
  return txID.sessionID;
}
