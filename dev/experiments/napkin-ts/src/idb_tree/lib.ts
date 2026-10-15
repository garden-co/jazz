// An asynchronous copy-on-write B-tree designed for IndexedDB page persistence.
import { compareBytes, equalBytes } from "../support/bytes.js";
import {
  INTERNAL_BASE_LEN,
  LEAF_BASE_LEN,
  type Page,
  type PageId,
  type ValueCell,
  decodePage,
  encodePage,
  encodedLen,
  internalKeyLen,
  leafEntryLen,
  leafPage,
} from "./page.js";
import {
  type Commit,
  type Metadata,
  type PageStore,
  TreeOwnership,
  emptyMetadata,
} from "./store.js";

export * from "./store.js";
export { IndexedDbPageStore } from "./web.js";

const DEFAULT_PAGE_SIZE = 16 * 1024;
const MIN_PAGE_SIZE = 1024;
const MAX_JS_SAFE_INTEGER = 9_007_199_254_740_991;
const NO_FLOOR = Number.MAX_SAFE_INTEGER + 1;

export type KeyValue = [Uint8Array, Uint8Array];
type LeafEntry = [Uint8Array, ValueCell];
type Descent = [PageId, LeafEntry[], [PageId, number][], Set<PageId>];
type Replacement =
  | { kind: "One"; page: PageId }
  | { kind: "Split"; left: PageId; separator: Uint8Array; right: PageId };

/** A precise cache miss: hydrate this page and retry. */
class Missing {
  constructor(readonly pageId: PageId) {}
}
type Attempt<T> = T | Missing;

/** Choose the most byte-balanced boundary for which both encoded pages fit. */
function byteBalancedSplit(
  sizes: number[],
  baseLen: number,
  pageSize: number,
  promoteSeparator: boolean,
): number | undefined {
  const count = sizes.length;
  let left = baseLen;
  let right = baseLen + sizes.reduce((a, b) => a + b, 0);
  let best: [number, number, number] | undefined;
  sizes.forEach((size, index) => {
    right -= size;
    if (!promoteSeparator) left += size;
    if ((promoteSeparator || index + 1 < count) && left <= pageSize && right <= pageSize) {
      const boundary = promoteSeparator ? index : index + 1;
      // Preserve the count midpoint on equal-byte choices.
      const a = Math.abs(left - right);
      const b = Math.abs(boundary - Math.floor(count / 2));
      if (!best || a < best[1] || (a === best[1] && b < best[2])) best = [boundary, a, b];
    }
    if (promoteSeparator) left += size;
  });
  return best?.[0];
}

export type WriteOperation =
  | { kind: "Set"; key: Uint8Array; value: Uint8Array }
  | { kind: "Delete"; key: Uint8Array };

export type IdbTreeErrorKind =
  | "InvalidOptions"
  | "MissingPage"
  | "InvalidPage"
  | "PageTooLarge"
  | "Store"
  | "GenerationConflict"
  | "OwnershipExpired"
  | "CommitInFlight"
  | "CommitAbandoned"
  | "NotResident"
  | "ReadOnlyView";

export class IdbTreeError extends Error {
  constructor(
    readonly kind: IdbTreeErrorKind,
    message: string,
  ) {
    super(message);
  }
}
const err = {
  invalidOptions: (m: string) =>
    new IdbTreeError("InvalidOptions", `invalid IDBTree options: ${m}`),
  missingPage: (id: PageId) => new IdbTreeError("MissingPage", `IDBTree page ${id} is missing`),
  invalidPage: (m: unknown) => new IdbTreeError("InvalidPage", `invalid IDBTree page: ${m}`),
  pageTooLarge: (id: PageId, size: number) =>
    new IdbTreeError(
      "PageTooLarge",
      `IDBTree page ${id} exceeds the configured ${size}-byte page size`,
    ),
  store: (m: unknown) => new IdbTreeError("Store", `IDBTree store error: ${m}`),
  generationConflict: (m: string) =>
    new IdbTreeError("GenerationConflict", `IDBTree generation conflict: ${m}`),
  ownershipExpired: () => new IdbTreeError("OwnershipExpired", "IDBTree ownership has expired"),
  commitInFlight: () =>
    new IdbTreeError("CommitInFlight", "an IDBTree commit is already in flight"),
  commitAbandoned: () =>
    new IdbTreeError(
      "CommitAbandoned",
      "an IDBTree commit was abandoned; reload before using the tree",
    ),
  notResident: (id: PageId) =>
    new IdbTreeError("NotResident", `IDBTree page ${id} is not resident for a read-committed view`),
  readOnlyView: () =>
    new IdbTreeError("ReadOnlyView", "a read-committed IDBTree view cannot write"),
};

export interface Options {
  pageSize: number;
}

function validateOptions(o: Options): Options {
  const p = o.pageSize;
  if (p < MIN_PAGE_SIZE || (p & (p - 1)) !== 0 || p > 0xffffffff)
    throw err.invalidOptions(
      `page_size must be a power of two between ${MIN_PAGE_SIZE} and ${0xffffffff}`,
    );
  return o;
}

export interface PreparedCommit {
  commit: Commit;
  retired: Set<PageId>;
}

const keyIndex = (entries: LeafEntry[], key: Uint8Array): [boolean, number] => {
  let lo = 0;
  let hi = entries.length;
  while (lo < hi) {
    const mid = (lo + hi) >>> 1;
    const c = compareBytes(entries[mid][0], key);
    if (c === 0) return [true, mid];
    if (c < 0) lo = mid + 1;
    else hi = mid;
  }
  return [false, lo];
};
const childIndex = (keys: Uint8Array[], key: Uint8Array) => {
  let i = 0;
  while (i < keys.length && compareBytes(keys[i], key) <= 0) i++;
  return i;
};
const cycle = () => err.invalidPage("tree child graph contains a cycle or shared page");

/** The tree core: structure, residency and dirty generations. */
class TreeCore {
  durableRoot: PageId | undefined;
  pages = new Map<PageId, Page>();
  dirty = new Set<PageId>();
  writeFloor = NO_FLOOR;
  deleted = new Set<PageId>();
  retirementUndo: PageId[] = [];
  commitInFlight = false;
  commitAbandoned = false;

  private constructor(
    readonly store: PageStore,
    readonly options: Options,
    public metadata: Metadata,
  ) {
    this.durableRoot = metadata.rootPageId;
  }

  static async open(store: PageStore, options: Options): Promise<TreeCore> {
    options = validateOptions(options);
    let metadata: Metadata | undefined;
    try {
      metadata = await store.loadMetadata();
    } catch (e) {
      throw err.store(e);
    }
    const tree = new TreeCore(store, options, metadata ?? emptyMetadata(options.pageSize));
    if (tree.metadata.pageSize !== options.pageSize)
      throw err.invalidOptions(
        `store uses ${tree.metadata.pageSize}-byte pages, requested ${options.pageSize}`,
      );
    if (tree.metadata.rootPageId === undefined)
      tree.metadata.rootPageId = tree.allocatePage(leafPage());
    return tree;
  }

  applyWriteMany(operations: WriteOperation[]): void {
    for (const op of operations) {
      const attempt = op.kind === "Set" ? this.tryPut(op.key, op.value) : this.tryDelete(op.key);
      if (attempt instanceof Missing)
        throw err.invalidPage(`prepared batch unexpectedly missed page ${attempt.pageId}`);
    }
  }

  private isFresh(pageId: PageId): boolean {
    return pageId >= this.writeFloor;
  }

  /** Run a write under a checkpoint; roll back on failure or a miss. */
  withWriteCheckpoint<T>(write: () => T, completed: (t: T) => boolean = () => true): T {
    const checkpoint = { ...this.metadata };
    this.writeFloor = this.metadata.nextPageId;
    let ok = false;
    try {
      const out = write();
      ok = completed(out);
      return out;
    } finally {
      this.writeFloor = NO_FLOOR;
      if (ok) this.retirementUndo = [];
      else this.rollbackWrite(checkpoint);
    }
  }

  private rollbackWrite(checkpoint: Metadata): void {
    for (let id = checkpoint.nextPageId; id < this.metadata.nextPageId; id++) {
      this.pages.delete(id);
      this.dirty.delete(id);
    }
    for (const id of this.retirementUndo.splice(0)) this.deleted.delete(id);
    this.metadata = checkpoint;
  }

  tryValueEquals(
    root: PageId,
    key: Uint8Array,
    expected: Uint8Array,
  ): Attempt<boolean | undefined> {
    const descent = this.residentDescentFrom(root, key);
    if (!descent) return new Missing(this.missingPageForKeyFrom(root, key));
    const [, entries, , visited] = descent;
    const [found, index] = keyIndex(entries, key);
    if (!found) return undefined;
    const value = entries[index][1];
    if (value.kind === "Inline") return equalBytes(value.bytes, expected);
    if (expected.length !== value.len) return false;
    let current: PageId | undefined = value.head;
    let offset = 0;
    while (current !== undefined) {
      const link = this.overflowPage(current, visited);
      if (!link) return new Missing(current);
      const end = offset + link[1].length;
      if (end > expected.length || !equalBytes(expected.subarray(offset, end), link[1]))
        return false;
      offset = end;
      current = link[0];
    }
    return offset === expected.length;
  }

  tryGet(root: PageId, key: Uint8Array): Attempt<Uint8Array | undefined> {
    const descent = this.residentDescentFrom(root, key);
    if (!descent) return new Missing(this.missingPageForKeyFrom(root, key));
    const [, entries, , visited] = descent;
    const [found, index] = keyIndex(entries, key);
    return found ? this.readValueResident(entries[index][1], visited) : undefined;
  }

  tryPut(key: Uint8Array, value: Uint8Array): Attempt<void> {
    const descent = this.residentDescentFrom(this.rootPageId(), key);
    if (!descent) return new Missing(this.missingPageForKeyFrom(this.rootPageId(), key));
    const [pageId, leafEntries, path, visited] = descent;
    if (this.isFresh(pageId)) {
      // This write already owns every value in this leaf: change it in place.
      const newValue = this.buildValue(value);
      const entries = this.takeFreshLeaf(pageId);
      const [found, index] = keyIndex(entries, key);
      if (found) {
        const old = entries[index][1];
        entries[index][1] = newValue;
        this.retireValue(old);
      } else entries.splice(index, 0, [key, newValue]);
      this.finishFreshLeafWrite(pageId, entries, path);
      return;
    }
    // Copying the leaf republishes every retained value edge: verify them first.
    const miss = this.leafValuesResident(leafEntries, visited);
    if (miss) return miss;
    const entries = leafEntries.map(([k, v]) => [k, v] as LeafEntry);
    const newValue = this.buildValue(value);
    const [found, index] = keyIndex(entries, key);
    if (found) {
      this.retireValue(entries[index][1]);
      entries[index][1] = newValue;
    } else entries.splice(index, 0, [key, newValue]);
    this.finishLeafWrite(pageId, entries, path);
  }

  tryDelete(key: Uint8Array): Attempt<boolean> {
    const descent = this.residentDescentFrom(this.rootPageId(), key);
    if (!descent) return new Missing(this.missingPageForKeyFrom(this.rootPageId(), key));
    const [pageId, leafEntries, path, visited] = descent;
    const [found, index] = keyIndex(leafEntries, key);
    if (!found) return false;
    if (this.isFresh(pageId)) {
      const entries = this.takeFreshLeaf(pageId);
      const [[, old]] = entries.splice(index, 1);
      this.retireValue(old);
      this.finishFreshLeafWrite(pageId, entries, path);
      return true;
    }
    const miss = this.leafValuesResident(leafEntries, visited);
    if (miss) return miss;
    const entries = leafEntries.slice();
    this.retireValue(entries[index][1]);
    entries.splice(index, 1);
    this.finishLeafWrite(pageId, entries, path);
    return true;
  }

  writePathResident(key: Uint8Array): Attempt<void> {
    const descent = this.residentDescentFrom(this.rootPageId(), key);
    if (!descent) return new Missing(this.missingPageForKeyFrom(this.rootPageId(), key));
    return this.leafValuesResident(descent[1], descent[3]);
  }

  tryRange(
    root: PageId,
    start: Uint8Array,
    end: Uint8Array,
    limit: number,
    reverse: boolean,
  ): KeyValue[] | PageId[] {
    const cells: LeafEntry[] = [];
    const missing: PageId[] = [];
    const visited = new Set<PageId>();
    this.collectRangeResident(root, start, end, limit, reverse, cells, missing, visited);
    if (missing.length) return missing;
    return this.materializeRangeValues(cells, visited);
  }

  /** Swap the active dirty generation without awaiting persistence. */
  prepareCommit(): PreparedCommit | undefined {
    if (this.commitInFlight) throw err.commitInFlight();
    if (!this.dirty.size && !this.deleted.size) return undefined;
    const pages: [PageId, Uint8Array][] = [];
    for (const pageId of [...this.dirty].sort((a, b) => a - b)) {
      if (this.deleted.has(pageId)) continue;
      let bytes: Uint8Array;
      try {
        bytes = encodePage(this.pages.get(pageId)!);
      } catch (e) {
        throw err.invalidPage(e);
      }
      if (bytes.length > this.options.pageSize)
        throw err.pageTooLarge(pageId, this.options.pageSize);
      pages.push([pageId, bytes]);
    }
    this.commitInFlight = true;
    const commit: Commit = {
      expectedGeneration: this.metadata.generation,
      metadata: { ...this.metadata },
      pages,
      deletedPageIds: this.store.canReclaimObsoletePages()
        ? [...this.deleted].filter((id) => !this.dirty.has(id)).sort((a, b) => a - b)
        : [],
    };
    this.dirty.clear();
    const retired = this.deleted;
    this.deleted = new Set();
    return { commit, retired };
  }

  /** Reconcile an atomic commit result with writes made after prepareCommit. */
  completeCommit(
    prepared: PreparedCommit,
    outcome: { ok: true; value: Metadata } | { ok: false; error: string },
  ): void {
    if (!this.commitInFlight)
      throw err.invalidPage("completed an IDBTree commit that was not in flight");
    this.commitInFlight = false;
    if (outcome.ok) {
      const expected = prepared.commit.expectedGeneration + 1;
      if (outcome.value.generation !== expected)
        throw err.store(
          `commit returned generation ${outcome.value.generation}, expected ${expected}`,
        );
      // Only advance the durable base of the next dirty generation.
      for (const id of prepared.retired) this.pages.delete(id);
      this.durableRoot = prepared.commit.metadata.rootPageId;
      this.metadata.generation = outcome.value.generation;
      return;
    }
    for (const [id] of prepared.commit.pages) if (this.pages.has(id)) this.dirty.add(id);
    for (const id of prepared.retired) this.deleted.add(id);
    throw outcome.error.includes("generation changed")
      ? err.generationConflict(outcome.error)
      : err.store(outcome.error);
  }

  rootPageId(): PageId {
    return this.metadata.rootPageId!;
  }

  residentDescentFrom(root: PageId, key: Uint8Array): Descent | undefined {
    let pageId = root;
    const path: [PageId, number][] = [];
    const visited = new Set<PageId>();
    for (;;) {
      if (visited.has(pageId)) throw cycle();
      visited.add(pageId);
      const page = this.pages.get(pageId);
      if (!page) return undefined;
      if (page.kind === "Leaf") return [pageId, page.entries, path, visited];
      if (page.kind === "Overflow")
        throw err.invalidPage("overflow page reached during tree descent");
      const index = childIndex(page.keys, key);
      path.push([pageId, index]);
      pageId = page.children[index];
    }
  }

  missingPageForKeyFrom(root: PageId, key: Uint8Array): PageId {
    let pageId = root;
    const visited = new Set<PageId>();
    for (;;) {
      if (visited.has(pageId)) throw cycle();
      visited.add(pageId);
      const page = this.pages.get(pageId);
      if (!page) return pageId;
      if (page.kind === "Leaf")
        throw err.invalidPage("requested a missing page for a resident descent");
      if (page.kind === "Overflow")
        throw err.invalidPage("overflow page reached during tree descent");
      pageId = page.children[childIndex(page.keys, key)];
    }
  }

  /** Every overflow chain reaching a cold page contributes to one batched miss. */
  private materializeRangeValues(cells: LeafEntry[], visited: Set<PageId>): KeyValue[] | PageId[] {
    const rows: KeyValue[] = [];
    const missing: PageId[] = [];
    for (const [key, value] of cells) {
      const v = this.readValueResident(value, visited);
      if (v instanceof Missing) missing.push(v.pageId);
      else if (!missing.length) rows.push([key, v]);
    }
    return missing.length ? missing : rows;
  }

  // A bounded scan stops at the first miss; an unbounded one gathers the frontier.
  private collectRangeResident(
    pageId: PageId,
    start: Uint8Array,
    end: Uint8Array,
    limit: number,
    reverse: boolean,
    output: LeafEntry[],
    missing: PageId[],
    visited: Set<PageId>,
  ): void {
    const stop = () =>
      output.length === limit || (limit !== Number.MAX_SAFE_INTEGER && missing.length > 0);
    if (stop()) return;
    if (visited.has(pageId)) throw cycle();
    visited.add(pageId);
    const page = this.pages.get(pageId);
    if (!page) {
      missing.push(pageId);
      return;
    }
    switch (page.kind) {
      case "Leaf": {
        if (missing.length) return;
        const take = limit - output.length;
        const inRange = ([k]: LeafEntry) => compareBytes(k, start) >= 0 && compareBytes(k, end) < 0;
        const entries = reverse ? page.entries.slice().reverse() : page.entries;
        output.push(...entries.filter(inRange).slice(0, take));
        return;
      }
      case "Internal": {
        const { keys, children } = page;
        for (let step = 0; step < children.length; step++) {
          const index = reverse ? children.length - 1 - step : step;
          const belowEnd = index === 0 || compareBytes(keys[index - 1], end) < 0;
          const aboveStart = index === keys.length || compareBytes(keys[index], start) > 0;
          if (belowEnd && aboveStart)
            this.collectRangeResident(
              children[index],
              start,
              end,
              limit,
              reverse,
              output,
              missing,
              visited,
            );
          if (stop()) break;
        }
        return;
      }
      case "Overflow":
        throw err.invalidPage(
          reverse
            ? "overflow page reached during reverse range traversal"
            : "overflow page reached during range traversal",
        );
    }
  }

  allocatePage(page: Page): PageId {
    const pageId = this.metadata.nextPageId;
    if (pageId >= MAX_JS_SAFE_INTEGER)
      throw err.invalidPage("IDBTree page id space exceeds JavaScript's safe integer range");
    if (this.pages.has(pageId) || this.dirty.has(pageId))
      throw err.invalidPage("IDBTree next page id already exists");
    if (!this.pageFits(page)) throw err.pageTooLarge(pageId, this.options.pageSize);
    this.metadata.nextPageId = pageId + 1;
    this.pages.set(pageId, page);
    this.dirty.add(pageId);
    return pageId;
  }

  private takeFreshLeaf(pageId: PageId): LeafEntry[] {
    const page = this.pages.get(pageId) as Extract<Page, { kind: "Leaf" }>;
    const entries = page.entries;
    page.entries = [];
    return entries;
  }

  /** A leaf this write owns keeps its id when it fits, and its left half when it splits. */
  private finishFreshLeafWrite(
    pageId: PageId,
    entries: LeafEntry[],
    path: [PageId, number][],
  ): void {
    if (this.pageFits({ kind: "Leaf", entries })) {
      this.pages.set(pageId, { kind: "Leaf", entries });
      return;
    }
    const rightEntries = this.splitLeafEntries(pageId, entries);
    const separator = rightEntries[0][0];
    const right = this.allocatePage({ kind: "Leaf", entries: rightEntries });
    this.pages.set(pageId, { kind: "Leaf", entries });
    this.publishReplacement({ kind: "Split", left: pageId, separator, right }, path);
  }

  private splitLeafEntries(pageId: PageId, entries: LeafEntry[]): LeafEntry[] {
    const size = this.options.pageSize;
    if (entries.length < 2) throw err.pageTooLarge(pageId, size);
    const split = byteBalancedSplit(
      entries.map(([k, v]) => leafEntryLen(k, v)),
      LEAF_BASE_LEN,
      size,
      false,
    );
    if (split === undefined) throw err.pageTooLarge(pageId, size);
    return entries.splice(split);
  }

  private finishLeafWrite(pageId: PageId, entries: LeafEntry[], path: [PageId, number][]): void {
    let replacement: Replacement;
    if (this.pageFits({ kind: "Leaf", entries }))
      replacement = { kind: "One", page: this.allocatePage({ kind: "Leaf", entries }) };
    else {
      const rightEntries = this.splitLeafEntries(pageId, entries);
      replacement = {
        kind: "Split",
        left: this.allocatePage({ kind: "Leaf", entries }),
        separator: rightEntries[0][0],
        right: this.allocatePage({ kind: "Leaf", entries: rightEntries }),
      };
    }
    this.retirePage(pageId);
    this.publishReplacement(replacement, path);
  }

  /** Rebuild every changed ancestor under fresh page ids. */
  private publishReplacement(replacement: Replacement, path: [PageId, number][]): void {
    for (const [parentId, index] of path.slice().reverse()) {
      const fresh = this.isFresh(parentId);
      const parent = this.pages.get(parentId);
      if (parent?.kind !== "Internal") throw err.invalidPage("descent parent is not internal");
      const keys = fresh ? parent.keys : parent.keys.slice();
      const children = fresh ? parent.children : parent.children.slice();
      if (replacement.kind === "One") children[index] = replacement.page;
      else {
        children[index] = replacement.left;
        keys.splice(index, 0, replacement.separator);
        children.splice(index + 1, 0, replacement.right);
      }
      const page: Page = { kind: "Internal", keys, children };
      if (fresh) {
        // Every ancestor of a fresh page is fresh and already points at it.
        if (replacement.kind === "One" || this.pageFits(page)) {
          this.pages.set(parentId, page);
          return;
        }
        const [left, separator, right] = this.splitInternal(parentId, page);
        this.pages.set(parentId, left);
        replacement = { kind: "Split", left: parentId, separator, right: this.allocatePage(right) };
        continue;
      }
      this.retirePage(parentId);
      if (replacement.kind === "One" || this.pageFits(page))
        replacement = { kind: "One", page: this.allocatePage(page) };
      else {
        const [left, separator, right] = this.splitInternal(parentId, page);
        replacement = {
          kind: "Split",
          left: this.allocatePage(left),
          separator,
          right: this.allocatePage(right),
        };
      }
    }
    this.metadata.rootPageId =
      replacement.kind === "One"
        ? replacement.page
        : this.allocatePage({
            kind: "Internal",
            keys: [replacement.separator],
            children: [replacement.left, replacement.right],
          });
  }

  // Track only new retirements in the current attempt (undo log).
  private retirePage(pageId: PageId): void {
    if (this.deleted.has(pageId)) return;
    this.deleted.add(pageId);
    this.retirementUndo.push(pageId);
  }

  private retireValue(value: ValueCell): void {
    if (value.kind !== "Overflow") return;
    let current: PageId | undefined = value.head;
    while (current !== undefined) {
      const page = this.pages.get(current) as Extract<Page, { kind: "Overflow" }>;
      this.retirePage(current);
      current = page.next;
    }
  }

  private pageFits(page: Page): boolean {
    return encodedLen(page) <= this.options.pageSize;
  }

  /** Split an over-full internal page, promoting one separator key. */
  private splitInternal(pageId: PageId, page: Page): [Page, Uint8Array, Page] {
    const { keys, children } = page as Extract<Page, { kind: "Internal" }>;
    const middle = byteBalancedSplit(
      keys.map(internalKeyLen),
      INTERNAL_BASE_LEN,
      this.options.pageSize,
      true,
    );
    if (middle === undefined) throw err.pageTooLarge(pageId, this.options.pageSize);
    const separator = keys[middle];
    return [
      { kind: "Internal", keys: keys.slice(0, middle), children: children.slice(0, middle + 1) },
      separator,
      { kind: "Internal", keys: keys.slice(middle + 1), children: children.slice(middle + 1) },
    ];
  }

  private buildValue(value: Uint8Array): ValueCell {
    // Keep leaves dense and the worst-case inline insertion bounded.
    if (value.length <= this.options.pageSize / 4) return { kind: "Inline", bytes: value };
    let next: PageId | undefined;
    const chunkSize = Math.max(0, this.options.pageSize - 64);
    for (let end = value.length; end > 0; end -= chunkSize) {
      const page: Page = {
        kind: "Overflow",
        next,
        bytes: value.slice(Math.max(0, end - chunkSize), end),
      };
      if (!this.pageFits(page))
        throw err.invalidPage("overflow chunk does not fit configured page");
      next = this.allocatePage(page);
    }
    return { kind: "Overflow", head: next!, len: value.length };
  }

  private valueResident(value: ValueCell, visited: Set<PageId>): Missing | undefined {
    if (value.kind !== "Overflow") return undefined;
    let current: PageId | undefined = value.head;
    while (current !== undefined) {
      const link = this.overflowPage(current, visited);
      if (!link) return new Missing(current);
      current = link[0];
    }
    return undefined;
  }

  /** One link of an overflow chain, or undefined when not resident. */
  private overflowPage(
    pageId: PageId,
    visited: Set<PageId>,
  ): [PageId | undefined, Uint8Array] | undefined {
    if (visited.has(pageId)) throw err.invalidPage("tree graph contains a cycle or shared page");
    visited.add(pageId);
    const page = this.pages.get(pageId);
    if (!page) return undefined;
    if (page.kind !== "Overflow") throw err.invalidPage("value references a non-overflow page");
    return [page.next, page.bytes];
  }

  private leafValuesResident(entries: LeafEntry[], visited: Set<PageId>): Missing | undefined {
    for (const [, value] of entries) {
      const miss = this.valueResident(value, visited);
      if (miss) return miss;
    }
    return undefined;
  }

  private readValueResident(value: ValueCell, visited: Set<PageId>): Attempt<Uint8Array> {
    if (value.kind === "Inline") return value.bytes;
    // Never trust the durable length for allocation: grow per validated page.
    const parts: Uint8Array[] = [];
    let total = 0;
    let current: PageId | undefined = value.head;
    while (current !== undefined) {
      const link = this.overflowPage(current, visited);
      if (!link) return new Missing(current);
      parts.push(link[1]);
      total += link[1].length;
      current = link[0];
    }
    if (total !== value.len)
      throw err.invalidPage(`overflow value length is ${total}, expected ${value.len}`);
    const out = new Uint8Array(total);
    let o = 0;
    for (const p of parts) {
      out.set(p, o);
      o += p.length;
    }
    return out;
  }
}

/** Cloneable, single-threaded handle. No state is held across page I/O. */
export class IdbTree {
  private constructor(
    private readonly shared: { core: TreeCore; reloadEpoch: number },
    private readonly ownership: TreeOwnership,
    private readonly readCommittedView: boolean,
  ) {}

  static async open(store: PageStore, options: Options = { pageSize: DEFAULT_PAGE_SIZE }) {
    let ownership: TreeOwnership;
    try {
      ownership = store.claimTreeOwnership();
    } catch (e) {
      throw err.store(e);
    }
    const core = await TreeCore.open(store, options);
    if (!ownership.isLive()) throw err.ownershipExpired();
    return new IdbTree({ core, reloadEpoch: 0 }, ownership, false);
  }

  private get core(): TreeCore {
    return this.shared.core;
  }

  /** A read-only view answering from the last committed generation, without I/O. */
  readCommitted(): IdbTree {
    return new IdbTree(this.shared, this.ownership, true);
  }

  private ensureWritable(): void {
    if (this.readCommittedView) throw err.readOnlyView();
  }

  private readRoot(): PageId | undefined {
    return this.readCommittedView ? this.core.durableRoot : this.core.rootPageId();
  }

  private ensureLive(): void {
    if (!this.ownership.isLive()) throw err.ownershipExpired();
    // The durable root stays a committed generation after an abandoned commit.
    if (!this.readCommittedView && this.core.commitAbandoned) throw err.commitAbandoned();
  }

  /** Discard staged writes and reload the durable root. */
  async reload(): Promise<void> {
    if (!this.ownership.isLive()) throw err.ownershipExpired();
    this.ensureWritable();
    if (this.core.commitInFlight) throw err.commitInFlight();
    const fresh = await TreeCore.open(this.core.store, this.core.options);
    if (!this.ownership.isLive()) throw err.ownershipExpired();
    this.shared.core = fresh;
    this.shared.reloadEpoch += 1;
  }

  metadata(): Metadata {
    return { ...this.core.metadata };
  }
  dirtyPageCount(): number {
    return this.core.dirty.size;
  }

  private async readPoint<T>(tryRead: (t: TreeCore, root: PageId) => Attempt<T | undefined>) {
    for (;;) {
      this.ensureLive();
      const root = this.readRoot();
      const attempt = root === undefined ? undefined : tryRead(this.core, root);
      if (!(attempt instanceof Missing)) return attempt;
      if (this.readCommittedView) throw err.notResident(attempt.pageId);
      await this.hydrate(attempt.pageId);
    }
  }

  private async writePoint<T>(tryWrite: (t: TreeCore) => Attempt<T>): Promise<T> {
    this.ensureWritable();
    for (;;) {
      this.ensureLive();
      const core = this.core;
      const attempt = core.withWriteCheckpoint(
        () => tryWrite(core),
        (a) => !(a instanceof Missing),
      );
      if (!(attempt instanceof Missing)) return attempt;
      await this.hydrate(attempt.pageId);
    }
  }

  /** Compare an existing value in place; undefined denotes an absent key. */
  valueEquals(key: Uint8Array, expected: Uint8Array) {
    return this.readPoint((t, root) => t.tryValueEquals(root, key, expected));
  }
  get(key: Uint8Array) {
    return this.readPoint((t, root) => t.tryGet(root, key));
  }
  put(key: Uint8Array, value: Uint8Array): Promise<void> {
    return this.writePoint((t) => t.tryPut(key, value));
  }
  delete(key: Uint8Array): Promise<boolean> {
    return this.writePoint((t) => t.tryDelete(key));
  }

  async writeMany(operations: WriteOperation[]): Promise<void> {
    this.ensureLive();
    this.ensureWritable();
    for (const op of operations)
      for (;;) {
        this.ensureLive();
        const attempt = this.core.writePathResident(op.key);
        if (!(attempt instanceof Missing)) break;
        await this.hydrate(attempt.pageId);
      }
    const core = this.core;
    core.withWriteCheckpoint(() => core.applyWriteMany(operations));
  }

  range(start: Uint8Array, end: Uint8Array) {
    return this.scan(start, end, Number.MAX_SAFE_INTEGER, false);
  }
  /** At most `limit` rows, never hydrating pages past the bound. */
  rangeLimit(start: Uint8Array, end: Uint8Array, limit: number) {
    return this.scan(start, end, limit, false);
  }
  rangeReverse(start: Uint8Array, end: Uint8Array, limit: number) {
    return this.scan(start, end, limit, true);
  }

  private async scan(start: Uint8Array, end: Uint8Array, limit: number, reverse: boolean) {
    for (;;) {
      this.ensureLive();
      const root = this.readRoot();
      if (root === undefined) return [];
      const attempt = this.core.tryRange(root, start, end, limit, reverse);
      if (!attempt.length || Array.isArray(attempt[0])) return attempt as KeyValue[];
      const ids = attempt as PageId[];
      if (this.readCommittedView) throw err.notResident(ids[0]);
      await this.hydrateMany(ids);
    }
  }

  async flush(): Promise<void> {
    this.ensureLive();
    this.ensureWritable();
    const core = this.core;
    const prepared = core.prepareCommit();
    if (!prepared) return;
    // A dropped flush would leave the commit pending forever: mark it abandoned
    // unless the store settles.
    let settled = false;
    let outcome: { ok: true; value: Metadata } | { ok: false; error: string };
    try {
      outcome = { ok: true, value: await core.store.commit(prepared.commit) };
      settled = true;
    } catch (e) {
      outcome = { ok: false, error: String(e) };
      settled = true;
    } finally {
      if (!settled) {
        core.commitInFlight = false;
        core.commitAbandoned = true;
      }
    }
    core.completeCommit(prepared, outcome);
  }

  /** Hydrate a scan's whole missing frontier with one store read. */
  private async hydrateMany(pageIds: PageId[]): Promise<void> {
    const ids = pageIds.filter((id) => !this.core.pages.has(id));
    if (!ids.length) return;
    if (ids.length === 1) return this.hydrate(ids[0]);
    const store = this.core.store;
    const rootBefore = this.core.metadata.rootPageId;
    const reloadBefore = this.shared.reloadEpoch;
    let pages: (Uint8Array | undefined)[] | undefined;
    let failure: unknown;
    try {
      pages = await store.readPages(ids);
    } catch (e) {
      failure = e;
    }
    this.ensureLive();
    if (this.shared.reloadEpoch !== reloadBefore || this.core.metadata.rootPageId !== rootBefore)
      return;
    if (!pages) throw err.store(failure);
    if (pages.length !== ids.length)
      throw err.store(`read ${pages.length} pages for ${ids.length} requested ids`);
    const decoded = ids.map((id, i) => [id, this.decode(id, pages![i])] as const);
    for (const [id, page] of decoded) if (!this.core.pages.has(id)) this.core.pages.set(id, page);
  }

  private async hydrate(pageId: PageId): Promise<void> {
    if (this.core.pages.has(pageId)) return;
    const store = this.core.store;
    const rootBefore = this.core.metadata.rootPageId;
    const reloadBefore = this.shared.reloadEpoch;
    let bytes: Uint8Array | undefined;
    let failure: unknown;
    let failed = false;
    try {
      bytes = await store.readPage(pageId);
    } catch (e) {
      failure = e;
      failed = true;
    }
    this.ensureLive();
    // Never import an old completion (including an error) across publication or reset.
    if (this.shared.reloadEpoch !== reloadBefore || this.core.metadata.rootPageId !== rootBefore)
      return;
    if (failed) throw err.store(failure);
    const page = this.decode(pageId, bytes);
    if (!this.core.pages.has(pageId)) this.core.pages.set(pageId, page);
  }

  private decode(pageId: PageId, bytes: Uint8Array | undefined): Page {
    if (!bytes) throw err.missingPage(pageId);
    const pageSize = this.core.options.pageSize;
    if (bytes.length > pageSize) throw err.pageTooLarge(pageId, pageSize);
    try {
      return decodePage(bytes);
    } catch (e) {
      throw err.invalidPage(e);
    }
  }
}
