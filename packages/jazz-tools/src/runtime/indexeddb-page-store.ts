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

export class IndexedDbStorageInvalidatedError extends Error {
  constructor(readonly databaseName: string) {
    super(`IndexedDB storage was invalidated: ${databaseName}`);
    this.name = "IndexedDbStorageInvalidatedError";
  }
}

/**
 * Dumb, atomic page persistence for the browser B-tree.
 *
 * Tree traversal, caching, dirty generations, and backpressure deliberately
 * live above this class. IndexedDB only stores opaque pages and atomically
 * advances the small root metadata record in the same relaxed transaction.
 */
export class IndexedDbPageStore {
  private invalidated = false;

  private constructor(
    private readonly db: IDBDatabase,
    readonly name: string,
    private readonly onInvalidated?: (error: IndexedDbStorageInvalidatedError) => void,
  ) {
    db.addEventListener("versionchange", this.handleVersionChange);
    db.addEventListener("close", this.handleUnexpectedClose);
  }

  static async open(
    name: string,
    onInvalidated?: (error: IndexedDbStorageInvalidatedError) => void,
  ): Promise<IndexedDbPageStore> {
    const request = indexedDB.open(name, 1);
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(PAGES_STORE)) db.createObjectStore(PAGES_STORE);
      if (!db.objectStoreNames.contains(METADATA_STORE)) db.createObjectStore(METADATA_STORE);
    };
    return new IndexedDbPageStore(await requestResult(request), name, onInvalidated);
  }

  async metadata(): Promise<IndexedDbBtreeMetadata | null> {
    this.assertValid();
    const tx = this.db.transaction(METADATA_STORE, "readonly");
    const done = transactionDone(tx);
    const value = await requestResult(tx.objectStore(METADATA_STORE).get(CURRENT_METADATA_KEY));
    await done;
    if (value === undefined) return null;
    assertMetadata(value);
    return value;
  }

  async readPage(pageId: number): Promise<Uint8Array | null> {
    this.assertValid();
    assertPageId(pageId);
    const tx = this.db.transaction(PAGES_STORE, "readonly");
    const done = transactionDone(tx);
    const value = await requestResult(tx.objectStore(PAGES_STORE).get(pageId));
    await done;
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
    this.assertValid();
    const tx = relaxedReadWriteTransaction(this.db, [PAGES_STORE, METADATA_STORE]);
    const done = transactionDone(tx);
    const pages = tx.objectStore(PAGES_STORE);
    const metadataStore = tx.objectStore(METADATA_STORE);
    const currentValue = await requestResult(metadataStore.get(CURRENT_METADATA_KEY));
    const currentGeneration = currentValue === undefined ? 0 : metadataGeneration(currentValue);

    if (currentGeneration !== commit.expectedGeneration) {
      tx.abort();
      await done.catch(() => undefined);
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
    await done;
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
    this.removeInvalidationListeners();
    this.db.close();
  }

  async clear(): Promise<void> {
    this.assertValid();
    const tx = relaxedReadWriteTransaction(this.db, [PAGES_STORE, METADATA_STORE]);
    const done = transactionDone(tx);
    tx.objectStore(PAGES_STORE).clear();
    tx.objectStore(METADATA_STORE).clear();
    await done;
  }

  static async destroy(name: string): Promise<void> {
    const request = indexedDB.deleteDatabase(name);
    await new Promise<void>((resolve, reject) => {
      request.onsuccess = () => resolve();
      request.onerror = () => reject(request.error ?? new Error("IndexedDB deletion failed"));
      request.onblocked = () =>
        reject(new Error(`IndexedDB deletion was blocked by an open connection: ${name}`));
    });
  }

  private readonly handleVersionChange = (): void => {
    // An upgrade and a deletion both replace the storage epoch underneath the
    // cached B-tree. Close promptly so the external operation is not blocked.
    this.invalidate();
    this.db.close();
  };

  private readonly handleUnexpectedClose = (): void => {
    this.invalidate();
  };

  private invalidate(): void {
    if (this.invalidated) return;
    this.invalidated = true;
    this.removeInvalidationListeners();
    this.onInvalidated?.(new IndexedDbStorageInvalidatedError(this.name));
  }

  private assertValid(): void {
    if (this.invalidated) throw new IndexedDbStorageInvalidatedError(this.name);
  }

  private removeInvalidationListeners(): void {
    this.db.removeEventListener("versionchange", this.handleVersionChange);
    this.db.removeEventListener("close", this.handleUnexpectedClose);
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
