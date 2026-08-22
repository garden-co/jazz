export const INDEXEDDB_BTREE_FORMAT_VERSION = 1;

const PAGES_STORE = "pages";
const METADATA_STORE = "metadata";
const CURRENT_METADATA_KEY = "current";

export interface IndexedDbBtreeMetadata {
  formatVersion: typeof INDEXEDDB_BTREE_FORMAT_VERSION;
  pageSize: number;
  generation: number;
  rootPageId: number | null;
  nextPageId: number;
}

export interface IndexedDbPageCommit {
  /** The generation observed when the dirty snapshot was taken. */
  expectedGeneration: number;
  metadata: Omit<IndexedDbBtreeMetadata, "formatVersion" | "generation">;
  pages: ReadonlyMap<number, Uint8Array>;
  deletedPageIds?: readonly number[];
}

/**
 * Dumb, atomic page persistence for the browser B-tree.
 *
 * Tree traversal, caching, dirty generations, and backpressure deliberately
 * live above this class. IndexedDB only stores opaque pages and atomically
 * advances the small root metadata record in the same relaxed transaction.
 */
export class IndexedDbPageStore {
  private constructor(
    private readonly db: IDBDatabase,
    readonly name: string,
  ) {}

  static async open(name: string): Promise<IndexedDbPageStore> {
    const request = indexedDB.open(name, 1);
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(PAGES_STORE)) db.createObjectStore(PAGES_STORE);
      if (!db.objectStoreNames.contains(METADATA_STORE)) db.createObjectStore(METADATA_STORE);
    };
    return new IndexedDbPageStore(await requestResult(request), name);
  }

  async metadata(): Promise<IndexedDbBtreeMetadata | null> {
    const tx = this.db.transaction(METADATA_STORE, "readonly");
    const value = await requestResult(tx.objectStore(METADATA_STORE).get(CURRENT_METADATA_KEY));
    await transactionDone(tx);
    if (value === undefined) return null;
    assertMetadata(value);
    return value;
  }

  async readPage(pageId: number): Promise<Uint8Array | null> {
    assertPageId(pageId);
    const tx = this.db.transaction(PAGES_STORE, "readonly");
    const value = await requestResult(tx.objectStore(PAGES_STORE).get(pageId));
    await transactionDone(tx);
    if (value === undefined) return null;
    if (!(value instanceof ArrayBuffer) && !ArrayBuffer.isView(value)) {
      throw new Error(`IndexedDB B-tree page ${pageId} is not binary data`);
    }
    const bytes =
      value instanceof ArrayBuffer
        ? new Uint8Array(value)
        : new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
    return bytes.slice();
  }

  async commit(commit: IndexedDbPageCommit): Promise<IndexedDbBtreeMetadata> {
    const tx = relaxedReadWriteTransaction(this.db, [PAGES_STORE, METADATA_STORE]);
    const pages = tx.objectStore(PAGES_STORE);
    const metadataStore = tx.objectStore(METADATA_STORE);
    const currentValue = await requestResult(metadataStore.get(CURRENT_METADATA_KEY));
    const currentGeneration = currentValue === undefined ? 0 : metadataGeneration(currentValue);

    if (currentGeneration !== commit.expectedGeneration) {
      tx.abort();
      await transactionDone(tx).catch(() => undefined);
      throw new Error(
        `IndexedDB B-tree generation changed: expected ${commit.expectedGeneration}, found ${currentGeneration}`,
      );
    }

    for (const [pageId, bytes] of commit.pages) {
      assertPageId(pageId);
      pages.put(bytes.slice().buffer, pageId);
    }
    for (const pageId of commit.deletedPageIds ?? []) {
      assertPageId(pageId);
      pages.delete(pageId);
    }

    const metadata: IndexedDbBtreeMetadata = {
      ...commit.metadata,
      formatVersion: INDEXEDDB_BTREE_FORMAT_VERSION,
      generation: currentGeneration + 1,
    };
    assertMetadata(metadata);
    metadataStore.put(metadata, CURRENT_METADATA_KEY);
    await transactionDone(tx);
    return metadata;
  }

  /** Narrow bridge used by wasm without serializing page bytes through serde. */
  commitPages(
    expectedGeneration: number,
    pageSize: number,
    rootPageId: number,
    nextPageId: number,
    pageIds: readonly number[],
    pageBytes: readonly Uint8Array[],
    deletedPageIds: readonly number[],
  ): Promise<IndexedDbBtreeMetadata> {
    if (pageIds.length !== pageBytes.length) {
      return Promise.reject(new Error("IDBTree page ids and page bytes have different lengths"));
    }
    return this.commit({
      expectedGeneration,
      metadata: {
        pageSize,
        rootPageId: rootPageId < 0 ? null : rootPageId,
        nextPageId,
      },
      pages: new Map(pageIds.map((pageId, index) => [pageId, pageBytes[index]!])),
      deletedPageIds,
    });
  }

  close(): void {
    this.db.close();
  }

  static async destroy(name: string): Promise<void> {
    const request = indexedDB.deleteDatabase(name);
    await requestResult(request);
  }
}

function relaxedReadWriteTransaction(db: IDBDatabase, stores: string[]): IDBTransaction {
  return db.transaction(stores, "readwrite", { durability: "relaxed" });
}

function requestResult<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error("IndexedDB request failed"));
  });
}

function transactionDone(tx: IDBTransaction): Promise<void> {
  return new Promise((resolve, reject) => {
    tx.oncomplete = () => resolve();
    tx.onabort = () => reject(tx.error ?? new Error("IndexedDB transaction aborted"));
    tx.onerror = () => reject(tx.error ?? new Error("IndexedDB transaction failed"));
  });
}

function metadataGeneration(value: unknown): number {
  assertMetadata(value);
  return value.generation;
}

function assertMetadata(value: unknown): asserts value is IndexedDbBtreeMetadata {
  if (!value || typeof value !== "object") throw new Error("Invalid IndexedDB B-tree metadata");
  const metadata = value as Partial<IndexedDbBtreeMetadata>;
  if (
    metadata.formatVersion !== INDEXEDDB_BTREE_FORMAT_VERSION ||
    !Number.isSafeInteger(metadata.pageSize) ||
    Number(metadata.pageSize) <= 0 ||
    !Number.isSafeInteger(metadata.generation) ||
    Number(metadata.generation) < 1 ||
    (metadata.rootPageId !== null && !isPageId(metadata.rootPageId)) ||
    !Number.isSafeInteger(metadata.nextPageId) ||
    Number(metadata.nextPageId) < 0
  ) {
    throw new Error("Invalid IndexedDB B-tree metadata");
  }
}

function assertPageId(pageId: number): void {
  if (!isPageId(pageId)) throw new Error(`Invalid IndexedDB B-tree page id: ${pageId}`);
}

function isPageId(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}
