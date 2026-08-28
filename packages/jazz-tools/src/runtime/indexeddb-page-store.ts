/** Durable IndexedDB metadata/page-store epoch, independent of browser IDB's version. */
export const INDEXEDDB_BTREE_FORMAT_VERSION = 2;
export const INDEXEDDB_BTREE_FORMAT_MAGIC = "jazz-idb-tree";
export const INDEXEDDB_BTREE_DATABASE_VERSION = 3;

// These names are part of the physical browser format. Do not rename them
// without introducing a new durable storage epoch and explicit migration.
export const INDEXEDDB_BTREE_PAGES_STORE = "pages";
export const INDEXEDDB_BTREE_METADATA_STORE = "metadata";
/** The epoch manifest is deliberately outside the ordinary tree metadata plane. */
export const INDEXEDDB_STORAGE_MANIFEST_STORE = "storage-manifest";
export const INDEXEDDB_STORAGE_MANIFEST_KEY = "epoch";
const CURRENT_METADATA_KEY = "current";
const MIN_PAGE_SIZE = 1024;
const MAX_PAGE_SIZE = 0x8000_0000;
export const INDEXEDDB_BTREE_PAGE_SIZE = 16 * 1024;
export const INDEXEDDB_STORAGE_EPOCH = 1;
export const INDEXEDDB_PAGE_CHECKSUM = "xxh3-64-le";
export const INDEXEDDB_PAGE_FORMAT_MAGIC = "IDBTREE\0";

/**
 * The closed Jazz schema-storage profile carried by a browser durable root.
 * These are opaque to IndexedDB: the Rust/JS layer that writes each family
 * owns its interpretation, while the adapter only proves that this root was
 * opened with the exact epoch-one inventory.
 */
export const JAZZ_EPOCH_1_STORAGE_CODEC_IDS = [
  "groove.ordered-kv.v1",
  "jazz.branch-key.v1",
  "jazz.catalogue.activation.v1",
  "jazz.catalogue.bootstrap-ready.v1",
  "jazz.catalogue.lens.v1",
  "jazz.catalogue.lineage.v1",
  "jazz.catalogue.physical-mapping.v1",
  "jazz.catalogue.schema.v1",
  "jazz.catalogue.write-pointer.v1",
  "jazz.result-member-key.v1",
  "jazz.result-row-source.v1",
  "jazz.subscription-program-fact-key.v1",
] as const;

/**
 * The entire browser physical-open contract. Keep this exact shape pinned:
 * unknown fields are decode-relevant until a later storage epoch says otherwise.
 */
export interface IndexedDbStorageManifest {
  storageEpoch: typeof INDEXEDDB_STORAGE_EPOCH;
  adapterId: "jazz-idb-tree";
  adapterFormatVersion: typeof INDEXEDDB_BTREE_FORMAT_VERSION;
  requiredCodecIds: typeof JAZZ_EPOCH_1_STORAGE_CODEC_IDS;
  pageSize: typeof INDEXEDDB_BTREE_PAGE_SIZE;
  pageChecksum: typeof INDEXEDDB_PAGE_CHECKSUM;
  pageFormatMagic: typeof INDEXEDDB_PAGE_FORMAT_MAGIC;
  pageFormatVersion: typeof INDEXEDDB_BTREE_FORMAT_VERSION;
}

export const INDEXEDDB_STORAGE_MANIFEST: IndexedDbStorageManifest = {
  storageEpoch: INDEXEDDB_STORAGE_EPOCH,
  adapterId: "jazz-idb-tree",
  adapterFormatVersion: INDEXEDDB_BTREE_FORMAT_VERSION,
  requiredCodecIds: JAZZ_EPOCH_1_STORAGE_CODEC_IDS,
  pageSize: INDEXEDDB_BTREE_PAGE_SIZE,
  pageChecksum: INDEXEDDB_PAGE_CHECKSUM,
  pageFormatMagic: INDEXEDDB_PAGE_FORMAT_MAGIC,
  pageFormatVersion: INDEXEDDB_BTREE_FORMAT_VERSION,
};

export interface IndexedDbBtreeMetadata {
  formatMagic: typeof INDEXEDDB_BTREE_FORMAT_MAGIC;
  formatVersion: typeof INDEXEDDB_BTREE_FORMAT_VERSION;
  pageSize: number;
  generation: number;
  rootPageId: number | null;
  nextPageId: number;
}

export interface IndexedDbPageCommit {
  /** The generation observed when the dirty snapshot was taken. */
  expectedGeneration: number;
  metadata: Omit<IndexedDbBtreeMetadata, "formatMagic" | "formatVersion" | "generation">;
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
    const request = indexedDB.open(name, INDEXEDDB_BTREE_DATABASE_VERSION);
    let rejectedPreSettlementDatabase = false;
    request.onupgradeneeded = (event) => {
      if (event.oldVersion !== 0) {
        // Do not commit even an auxiliary store into an alpha namespace. The
        // aborted upgrade leaves the prior version and every ordinary page
        // unchanged before the caller observes the unsupported-epoch error.
        rejectedPreSettlementDatabase = true;
        request.result.close();
        request.transaction?.abort();
        return;
      }
      const db = request.result;
      if (!db.objectStoreNames.contains(INDEXEDDB_BTREE_PAGES_STORE))
        db.createObjectStore(INDEXEDDB_BTREE_PAGES_STORE);
      if (!db.objectStoreNames.contains(INDEXEDDB_BTREE_METADATA_STORE))
        db.createObjectStore(INDEXEDDB_BTREE_METADATA_STORE);
      if (!db.objectStoreNames.contains(INDEXEDDB_STORAGE_MANIFEST_STORE)) {
        const manifest = db.createObjectStore(INDEXEDDB_STORAGE_MANIFEST_STORE);
        if (request.transaction) {
          manifest.put(INDEXEDDB_STORAGE_MANIFEST, INDEXEDDB_STORAGE_MANIFEST_KEY);
        }
      }
    };
    let db: IDBDatabase;
    try {
      db = await requestResult(request);
    } catch (error) {
      if (rejectedPreSettlementDatabase) {
        throw new Error("Missing or invalid IndexedDB storage epoch manifest");
      }
      throw error;
    }
    const store = new IndexedDbPageStore(db, name, onInvalidated);
    try {
      await store.assertStorageManifest();
      return store;
    } catch (error) {
      store.close();
      throw error;
    }
  }

  async metadata(): Promise<IndexedDbBtreeMetadata | null> {
    this.assertValid();
    const tx = this.db.transaction(INDEXEDDB_BTREE_METADATA_STORE, "readonly");
    const done = transactionDone(tx);
    const value = await requestResult(
      tx.objectStore(INDEXEDDB_BTREE_METADATA_STORE).get(CURRENT_METADATA_KEY),
    );
    await done;
    if (value === undefined) return null;
    assertMetadata(value);
    return value;
  }

  /** Read-only physical-open gate. It always runs before a caller gets a handle. */
  private async assertStorageManifest(): Promise<void> {
    const tx = this.db.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readonly");
    const done = transactionDone(tx);
    const value = await requestResult(
      tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE).get(INDEXEDDB_STORAGE_MANIFEST_KEY),
    );
    await done;
    assertStorageManifest(value);
  }

  async readPage(pageId: number): Promise<Uint8Array | null> {
    this.assertValid();
    assertPageId(pageId);
    const tx = this.db.transaction(INDEXEDDB_BTREE_PAGES_STORE, "readonly");
    const done = transactionDone(tx);
    const value = await requestResult(tx.objectStore(INDEXEDDB_BTREE_PAGES_STORE).get(pageId));
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
    assertCommit(commit);
    const tx = relaxedReadWriteTransaction(this.db, [
      INDEXEDDB_BTREE_PAGES_STORE,
      INDEXEDDB_BTREE_METADATA_STORE,
    ]);
    const done = transactionDone(tx);
    const pages = tx.objectStore(INDEXEDDB_BTREE_PAGES_STORE);
    const metadataStore = tx.objectStore(INDEXEDDB_BTREE_METADATA_STORE);
    const currentValue = await requestResult(metadataStore.get(CURRENT_METADATA_KEY));
    const currentGeneration = currentValue === undefined ? 0 : metadataGeneration(currentValue);

    if (currentGeneration !== commit.expectedGeneration) {
      tx.abort();
      await done.catch(() => undefined);
      throw new Error(
        `IndexedDB B-tree generation changed: expected ${commit.expectedGeneration}, found ${currentGeneration}`,
      );
    }

    const metadata: IndexedDbBtreeMetadata = {
      ...commit.metadata,
      formatMagic: INDEXEDDB_BTREE_FORMAT_MAGIC,
      formatVersion: INDEXEDDB_BTREE_FORMAT_VERSION,
      generation: currentGeneration + 1,
    };
    // Validate the exact metadata to be published before queuing a mutation.
    assertMetadata(metadata);

    try {
      if (
        metadata.rootPageId !== null &&
        !commit.pages.has(metadata.rootPageId) &&
        (await requestResult(pages.get(metadata.rootPageId))) === undefined
      ) {
        throw new Error(`IndexedDB B-tree root page ${metadata.rootPageId} is missing`);
      }
      for (const [pageId, bytes] of commit.pages) {
        if (bytes.byteLength > metadata.pageSize) {
          throw new Error(`IndexedDB B-tree page ${pageId} exceeds configured page size`);
        }
        pages.put(bytes.slice().buffer, pageId);
      }
      for (const pageId of commit.deletedPageIds ?? []) {
        pages.delete(pageId);
      }
      metadataStore.put(metadata, CURRENT_METADATA_KEY);
      await done;
      return metadata;
    } catch (error) {
      try {
        tx.abort();
      } catch {
        // The transaction may already have aborted because an IDB request failed.
      }
      await done.catch(() => undefined);
      throw error;
    }
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
    const tx = relaxedReadWriteTransaction(this.db, [
      INDEXEDDB_BTREE_PAGES_STORE,
      INDEXEDDB_BTREE_METADATA_STORE,
    ]);
    const done = transactionDone(tx);
    tx.objectStore(INDEXEDDB_BTREE_PAGES_STORE).clear();
    tx.objectStore(INDEXEDDB_BTREE_METADATA_STORE).clear();
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
    metadata.formatMagic !== INDEXEDDB_BTREE_FORMAT_MAGIC ||
    metadata.formatVersion !== INDEXEDDB_BTREE_FORMAT_VERSION ||
    !isPageSize(metadata.pageSize) ||
    metadata.pageSize !== INDEXEDDB_BTREE_PAGE_SIZE ||
    !Number.isSafeInteger(metadata.generation) ||
    Number(metadata.generation) < 1 ||
    (metadata.rootPageId !== null && !isPageId(metadata.rootPageId)) ||
    !Number.isSafeInteger(metadata.nextPageId) ||
    Number(metadata.nextPageId) < 0 ||
    (metadata.rootPageId !== null && Number(metadata.rootPageId) >= Number(metadata.nextPageId))
  ) {
    throw new Error("Invalid IndexedDB B-tree metadata");
  }
}

function assertCommit(commit: IndexedDbPageCommit): void {
  if (!Number.isSafeInteger(commit.expectedGeneration) || commit.expectedGeneration < 0) {
    throw new Error("Invalid IndexedDB B-tree expected generation");
  }
  const { pageSize, rootPageId, nextPageId } = commit.metadata;
  if (!isPageSize(pageSize) || pageSize !== INDEXEDDB_BTREE_PAGE_SIZE || !isPageId(nextPageId)) {
    throw new Error("Invalid IndexedDB B-tree commit metadata");
  }
  if (rootPageId !== null && (!isPageId(rootPageId) || rootPageId >= nextPageId)) {
    throw new Error("Invalid IndexedDB B-tree commit root page id");
  }
  const written = new Set<number>();
  for (const [pageId, bytes] of commit.pages) {
    if (!isPageId(pageId) || pageId >= nextPageId || bytes.byteLength > pageSize) {
      throw new Error("Invalid IndexedDB B-tree page commit");
    }
    written.add(pageId);
  }
  for (const pageId of commit.deletedPageIds ?? []) {
    if (!isPageId(pageId) || pageId >= nextPageId || pageId === rootPageId || written.has(pageId)) {
      throw new Error("Invalid IndexedDB B-tree deleted page commit");
    }
  }
}

function assertStorageManifest(value: unknown): asserts value is IndexedDbStorageManifest {
  if (!value || typeof value !== "object") {
    throw new Error("Missing or invalid IndexedDB storage epoch manifest");
  }
  const manifest = value as Partial<IndexedDbStorageManifest>;
  const keys = Object.keys(manifest).sort();
  const expectedKeys = Object.keys(INDEXEDDB_STORAGE_MANIFEST).sort();
  if (
    keys.length !== expectedKeys.length ||
    keys.some((key, index) => key !== expectedKeys[index]) ||
    manifest.storageEpoch !== INDEXEDDB_STORAGE_MANIFEST.storageEpoch ||
    manifest.adapterId !== INDEXEDDB_STORAGE_MANIFEST.adapterId ||
    manifest.adapterFormatVersion !== INDEXEDDB_STORAGE_MANIFEST.adapterFormatVersion ||
    manifest.pageSize !== INDEXEDDB_STORAGE_MANIFEST.pageSize ||
    manifest.pageChecksum !== INDEXEDDB_STORAGE_MANIFEST.pageChecksum ||
    manifest.pageFormatMagic !== INDEXEDDB_STORAGE_MANIFEST.pageFormatMagic ||
    manifest.pageFormatVersion !== INDEXEDDB_STORAGE_MANIFEST.pageFormatVersion ||
    !Array.isArray(manifest.requiredCodecIds) ||
    manifest.requiredCodecIds.length !== INDEXEDDB_STORAGE_MANIFEST.requiredCodecIds.length ||
    manifest.requiredCodecIds.some(
      (codec, index) => codec !== INDEXEDDB_STORAGE_MANIFEST.requiredCodecIds[index],
    )
  ) {
    throw new Error("Missing or invalid IndexedDB storage epoch manifest");
  }
}

function isPageSize(value: unknown): value is number {
  return (
    typeof value === "number" &&
    Number.isSafeInteger(value) &&
    value >= MIN_PAGE_SIZE &&
    value <= MAX_PAGE_SIZE &&
    Number.isInteger(Math.log2(value))
  );
}

function assertPageId(pageId: number): void {
  if (!isPageId(pageId)) throw new Error(`Invalid IndexedDB B-tree page id: ${pageId}`);
}

function isPageId(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}
