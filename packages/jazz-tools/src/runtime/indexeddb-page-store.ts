/** Durable IndexedDB metadata/page-store format, independent of browser IDB's schema version. */
export const INDEXEDDB_BTREE_FORMAT_VERSION = 1;
export const INDEXEDDB_BTREE_FORMAT_MAGIC = "jazz-idb-tree";
/**
 * Browser IndexedDB's object-store schema generation. This remains 3 because
 * it is a browser-managed database-schema number, not the Jazz page codec
 * version; generation 3 is the schema that contains the epoch manifest store.
 */
export const INDEXEDDB_BTREE_DATABASE_VERSION = 3;

// These names are part of the physical browser format. Do not rename them
// without introducing a new durable storage epoch and explicit migration.
export const INDEXEDDB_BTREE_PAGES_STORE = "pages";
export const INDEXEDDB_BTREE_METADATA_STORE = "metadata";
/** The epoch manifest is deliberately outside the ordinary tree metadata plane. */
export const INDEXEDDB_STORAGE_MANIFEST_STORE = "storage-manifest";
export const INDEXEDDB_STORAGE_MANIFEST_KEY = "epoch";
/**
 * A browser-runtime concern stored alongside (but deliberately not inside)
 * the durable adapter epoch manifest. It binds an explicitly named physical
 * IndexedDB root to the first logical Jazz owner that opens it.
 */
export const INDEXEDDB_BROWSER_RUNTIME_OWNER_KEY = "browser-runtime-owner";
/**
 * Opaque epoch held by the one live SharedWorker realm permitted to recover
 * foreground leases for this physical root. Its companion Web Lock supplies
 * liveness; this record fences a stale realm's later clean release.
 */
export const INDEXEDDB_BROWSER_WORKER_EPOCH_KEY = "browser-worker-epoch-v1";
export const INDEXEDDB_BROWSER_WORKER_EPOCH_FORMAT = "jazz-browser-worker-epoch-v1";
/**
 * The immutable NodeUuid for this physical browser replica.
 *
 * This is intentionally separate from the epoch/profile manifest: the profile
 * describes a shared format, while this value distinguishes two independent
 * stores that happen to use that same format and logical app/account scope.
 */
export const INDEXEDDB_REPLICA_NODE_KEY = "replica-node-v1";
export const INDEXEDDB_REPLICA_NODE_BYTES = 16;
/** Durable worker-owned pool for browser foreground TxId node leases. */
export const INDEXEDDB_FOREGROUND_NODE_LEASES_KEY = "foreground-node-leases-v1";
export const INDEXEDDB_FOREGROUND_NODE_LEASES_FORMAT = "jazz-foreground-node-leases-v1";
const MAX_TX_TIME = (1n << 64n) - 1n;
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
  "groove.large-value.v1",
  "groove.ordered-chunk-storage.v1",
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

export interface ForegroundNodeLease {
  /** Opaque CSPRNG lease token, valid only while its port remains attached. */
  leaseId: string;
  /** NodeUuid leased exclusively to this foreground runtime. */
  node: Uint8Array;
  /** HLC high-water persisted by the preceding clean lease holder. */
  confirmedTxTime: bigint;
}

/**
 * Narrow durable-lease state used by browser receipts. It deliberately
 * answers only the state of one known node, rather than exposing the private
 * foreground lease-pool representation or its other occupants.
 *
 * @internal
 */
export type ForegroundNodeLeaseNodeState = "active" | "reusable" | "retired" | "missing";

type StoredForegroundNodeLease = {
  leaseId: string;
  node: ArrayBuffer;
  confirmedTxTime: string;
};

type StoredForegroundNodeLeasePool = {
  format: typeof INDEXEDDB_FOREGROUND_NODE_LEASES_FORMAT;
  active: StoredForegroundNodeLease[];
  reusable: StoredForegroundNodeLease[];
  retired: ArrayBuffer[];
};

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
  private replicaNodeBytes: Uint8Array | null = null;
  private readonly invalidationListeners = new Set<
    (error: IndexedDbStorageInvalidatedError) => void
  >();

  private constructor(
    private readonly db: IDBDatabase,
    readonly name: string,
    initialInvalidationListener?: (error: IndexedDbStorageInvalidatedError) => void,
  ) {
    if (initialInvalidationListener) this.invalidationListeners.add(initialInvalidationListener);
    db.addEventListener("versionchange", this.handleVersionChange);
    db.addEventListener("close", this.handleUnexpectedClose);
  }

  /** Subscribe while a worker context uses this physical page-store handle. */
  onInvalidated(listener: (error: IndexedDbStorageInvalidatedError) => void): () => void {
    this.invalidationListeners.add(listener);
    return () => this.invalidationListeners.delete(listener);
  }

  static async open(
    name: string,
    options: {
      /** Stable, non-secret logical owner for an explicitly selected browser root. */
      owner?: string;
      onInvalidated?: (error: IndexedDbStorageInvalidatedError) => void;
    } = {},
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
          manifest.put(randomReplicaNodeBytes().buffer, INDEXEDDB_REPLICA_NODE_KEY);
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
    const store = new IndexedDbPageStore(db, name, options.onInvalidated);
    try {
      await store.assertStorageManifest();
      if (options.owner) await store.claimBrowserRuntimeOwner(options.owner);
      store.replicaNodeBytes = await store.readReplicaNodeBytes();
      return store;
    } catch (error) {
      store.close();
      throw error;
    }
  }

  /**
   * Return this physical store's durable transaction-node identity.
   *
   * A copy prevents callers from mutating the bytes held by the page-store
   * boundary after it has admitted the manifest-owned identity.
   */
  get replicaNode(): Uint8Array {
    if (!this.replicaNodeBytes) {
      throw new Error("IndexedDB replica node is unavailable before storage admission");
    }
    return this.replicaNodeBytes.slice();
  }

  /**
   * Atomically acquire a foreground TxId node lease.
   *
   * An `active` record left behind by an earlier worker process is never
   * reused: its holder may have minted identities after its last persistence
   * point. A fresh worker retires it before allocating a new or cleanly
   * returned identity. The current SharedWorker keeps live leases in memory,
   * so this recovery rule only applies during worker bootstrap.
   */
  async acquireForegroundNodeLease(recoverAbandoned = false): Promise<ForegroundNodeLease> {
    return await this.updateForegroundNodeLeasePool((pool) => {
      if (recoverAbandoned) {
        for (const active of pool.active) pool.retired.push(active.node);
        pool.active = [];
      }
      const reusable = pool.reusable.pop();
      const lease: StoredForegroundNodeLease = {
        leaseId: crypto.randomUUID(),
        node: reusable?.node ?? nodeBytesToBuffer(randomReplicaNodeBytes()),
        confirmedTxTime: reusable?.confirmedTxTime ?? "0",
      };
      pool.active.push(lease);
      return storedForegroundNodeLeaseToPublic(lease);
    });
  }

  /**
   * Atomically persist the runtime-observed HLC high-water before making a
   * foreground node reusable. An unknown lease is rejected fail-closed.
   */
  async returnForegroundNodeLease(leaseId: string, confirmedTxTime: bigint): Promise<void> {
    if (!isLeaseId(leaseId) || !isTxTime(confirmedTxTime)) {
      throw new Error("Invalid IndexedDB foreground node lease handoff");
    }
    await this.updateForegroundNodeLeasePool((pool) => {
      const index = pool.active.findIndex((lease) => lease.leaseId === leaseId);
      if (index < 0) throw new Error("Unknown IndexedDB foreground node lease");
      const [lease] = pool.active.splice(index, 1);
      if (!lease) throw new Error("Unknown IndexedDB foreground node lease");
      const previous = BigInt(lease.confirmedTxTime);
      pool.reusable.push({
        ...lease,
        // An old caller or a wall-clock rollback cannot lower a durable floor.
        confirmedTxTime: (confirmedTxTime > previous ? confirmedTxTime : previous).toString(),
      });
    });
  }

  /** Permanently retire a lease whose owner did not complete clean handoff. */
  async retireForegroundNodeLease(leaseId: string): Promise<void> {
    if (!isLeaseId(leaseId)) throw new Error("Invalid IndexedDB foreground node lease");
    await this.updateForegroundNodeLeasePool((pool) => {
      const index = pool.active.findIndex((lease) => lease.leaseId === leaseId);
      if (index < 0) return;
      const [lease] = pool.active.splice(index, 1);
      if (lease) pool.retired.push(lease.node);
    });
  }

  /** @internal Narrow browser-worker receipt primitive; see `ForegroundNodeLeaseNodeState`. */
  async foregroundNodeLeaseNodeState(node: Uint8Array): Promise<ForegroundNodeLeaseNodeState> {
    if (node.byteLength !== INDEXEDDB_REPLICA_NODE_BYTES) {
      throw new Error("Invalid IndexedDB foreground node identity");
    }
    this.assertValid();
    const tx = this.db.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readonly");
    const done = transactionDone(tx);
    try {
      const value = await requestResult(
        tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE).get(INDEXEDDB_FOREGROUND_NODE_LEASES_KEY),
      );
      await done;
      if (value === undefined) return "missing";
      const pool = decodeForegroundNodeLeasePool(value);
      const nodeKey = bytesKey(nodeBytesToBuffer(node));
      if (pool.active.some((lease) => bytesKey(lease.node) === nodeKey)) return "active";
      if (pool.reusable.some((lease) => bytesKey(lease.node) === nodeKey)) return "reusable";
      if (pool.retired.some((retired) => bytesKey(retired) === nodeKey)) return "retired";
      return "missing";
    } catch (error) {
      await done.catch(() => undefined);
      throw error;
    }
  }

  /**
   * Durably fence this live worker realm after it has acquired the matching
   * origin-wide Web Lock. A successor may replace an epoch only after that
   * lock proves the preceding realm is no longer alive.
   */
  async claimBrowserWorkerEpoch(epoch: string): Promise<void> {
    if (!isBrowserWorkerEpoch(epoch)) throw new Error("Invalid browser worker epoch");
    this.assertValid();
    const tx = this.db.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readwrite");
    const done = transactionDone(tx);
    tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE).put(
      { format: INDEXEDDB_BROWSER_WORKER_EPOCH_FORMAT, epoch },
      INDEXEDDB_BROWSER_WORKER_EPOCH_KEY,
    );
    await done;
  }

  /** Delete only this realm's epoch; a stale realm must never clear its successor. */
  async releaseBrowserWorkerEpoch(epoch: string): Promise<void> {
    if (!isBrowserWorkerEpoch(epoch)) throw new Error("Invalid browser worker epoch");
    this.assertValid();
    const tx = this.db.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readwrite");
    const done = transactionDone(tx);
    const store = tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE);
    const current = await requestResult(store.get(INDEXEDDB_BROWSER_WORKER_EPOCH_KEY));
    if (
      current &&
      typeof current === "object" &&
      (current as { format?: unknown; epoch?: unknown }).format ===
        INDEXEDDB_BROWSER_WORKER_EPOCH_FORMAT &&
      (current as { epoch?: unknown }).epoch === epoch
    ) {
      store.delete(INDEXEDDB_BROWSER_WORKER_EPOCH_KEY);
    }
    await done;
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

  /**
   * Claim the physical browser root exactly once. The marker intentionally
   * survives normal worker release and restart: a later incompatible auth
   * scope must fail before it receives a page-store handle or mutates data.
   * `destroy()` removes the whole root, including this marker, which is the
   * explicit way to transfer an intentionally named database to a new owner.
   */
  private async claimBrowserRuntimeOwner(owner: string): Promise<void> {
    // This is canonical, exact identity data, not a fixed-width digest. Do
    // not truncate or impose a small surrogate-size cap: that would either
    // reintroduce collisions or make an otherwise valid issuer/subject unable
    // to claim its browser root.
    if (typeof owner !== "string" || owner.length === 0) {
      throw new Error("Invalid browser IndexedDB runtime owner");
    }
    const tx = this.db.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readwrite");
    const done = transactionDone(tx);
    const manifest = tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE);
    const existing = await requestResult(manifest.get(INDEXEDDB_BROWSER_RUNTIME_OWNER_KEY));
    if (existing === undefined) {
      manifest.put(owner, INDEXEDDB_BROWSER_RUNTIME_OWNER_KEY);
      await done;
      return;
    }
    if (existing === owner) {
      await done;
      return;
    }
    tx.abort();
    await done.catch(() => undefined);
    throw new Error(
      `IndexedDB database ${this.name} is already owned by a different Jazz browser session; this indicates a wrong physical root or namespace derivation error. Reset this scoped database only if you intend to discard its cache`,
    );
  }

  private async readReplicaNodeBytes(): Promise<Uint8Array> {
    const tx = this.db.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readonly");
    const done = transactionDone(tx);
    const value = await requestResult(
      tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE).get(INDEXEDDB_REPLICA_NODE_KEY),
    );
    await done;
    if (!(value instanceof ArrayBuffer) && !ArrayBuffer.isView(value)) {
      throw new Error("Missing or invalid IndexedDB replica node identity");
    }
    const bytes =
      value instanceof ArrayBuffer
        ? new Uint8Array(value)
        : new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
    if (bytes.byteLength !== INDEXEDDB_REPLICA_NODE_BYTES) {
      throw new Error("Missing or invalid IndexedDB replica node identity");
    }
    return bytes.slice();
  }

  private async updateForegroundNodeLeasePool<T>(
    update: (pool: StoredForegroundNodeLeasePool) => T,
  ): Promise<T> {
    this.assertValid();
    const tx = relaxedReadWriteTransaction(this.db, [INDEXEDDB_STORAGE_MANIFEST_STORE]);
    const done = transactionDone(tx);
    const store = tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE);
    try {
      const value = await requestResult(store.get(INDEXEDDB_FOREGROUND_NODE_LEASES_KEY));
      const pool =
        value === undefined ? emptyForegroundNodeLeasePool() : decodeForegroundNodeLeasePool(value);
      const result = update(pool);
      assertForegroundNodeLeasePool(pool);
      store.put(pool, INDEXEDDB_FOREGROUND_NODE_LEASES_KEY);
      await done;
      return result;
    } catch (error) {
      try {
        tx.abort();
      } catch {
        // The transaction may already have completed/aborted after a request failure.
      }
      await done.catch(() => undefined);
      throw error;
    }
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
    // cached B-tree. Close the browser handle *before* notifying higher layers:
    // invalidation cleanup may await durable epoch work, while the pending IDB
    // delete/upgrade cannot progress until every old-version handle is closed.
    this.db.close();
    this.invalidate();
  };

  private readonly handleUnexpectedClose = (): void => {
    this.invalidate();
  };

  private invalidate(): void {
    if (this.invalidated) return;
    this.invalidated = true;
    this.removeInvalidationListeners();
    const error = new IndexedDbStorageInvalidatedError(this.name);
    for (const listener of this.invalidationListeners) listener(error);
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

function randomReplicaNodeBytes(): Uint8Array {
  const bytes = new Uint8Array(INDEXEDDB_REPLICA_NODE_BYTES);
  if (!globalThis.crypto?.getRandomValues) {
    throw new Error(
      "Browser storage requires cryptographic random values for its replica identity",
    );
  }
  globalThis.crypto.getRandomValues(bytes);
  return bytes;
}

function nodeBytesToBuffer(bytes: Uint8Array): ArrayBuffer {
  return bytes.slice().buffer as ArrayBuffer;
}

function emptyForegroundNodeLeasePool(): StoredForegroundNodeLeasePool {
  return {
    format: INDEXEDDB_FOREGROUND_NODE_LEASES_FORMAT,
    active: [],
    reusable: [],
    retired: [],
  };
}

function storedForegroundNodeLeaseToPublic(lease: StoredForegroundNodeLease): ForegroundNodeLease {
  return {
    leaseId: lease.leaseId,
    node: new Uint8Array(lease.node).slice(),
    confirmedTxTime: BigInt(lease.confirmedTxTime),
  };
}

function decodeForegroundNodeLeasePool(value: unknown): StoredForegroundNodeLeasePool {
  assertForegroundNodeLeasePool(value);
  return {
    format: value.format,
    active: value.active.map(copyStoredForegroundNodeLease),
    reusable: value.reusable.map(copyStoredForegroundNodeLease),
    retired: value.retired.map((node) => node.slice(0)),
  };
}

function copyStoredForegroundNodeLease(
  lease: StoredForegroundNodeLease,
): StoredForegroundNodeLease {
  return {
    leaseId: lease.leaseId,
    node: lease.node.slice(0),
    confirmedTxTime: lease.confirmedTxTime,
  };
}

function assertForegroundNodeLeasePool(
  value: unknown,
): asserts value is StoredForegroundNodeLeasePool {
  if (!value || typeof value !== "object") {
    throw new Error("Invalid IndexedDB foreground node lease pool");
  }
  const pool = value as Partial<StoredForegroundNodeLeasePool>;
  if (
    pool.format !== INDEXEDDB_FOREGROUND_NODE_LEASES_FORMAT ||
    !Array.isArray(pool.active) ||
    !Array.isArray(pool.reusable) ||
    !Array.isArray(pool.retired)
  ) {
    throw new Error("Invalid IndexedDB foreground node lease pool");
  }
  const nodes = new Set<string>();
  const leaseIds = new Set<string>();
  for (const lease of [...pool.active, ...pool.reusable]) {
    assertStoredForegroundNodeLease(lease);
    const nodeKey = bytesKey(lease.node);
    if (leaseIds.has(lease.leaseId) || nodes.has(nodeKey)) {
      throw new Error("Invalid IndexedDB foreground node lease pool");
    }
    leaseIds.add(lease.leaseId);
    nodes.add(nodeKey);
  }
  for (const node of pool.retired) {
    const nodeKey = isNodeBuffer(node) ? bytesKey(node) : null;
    if (!nodeKey || nodes.has(nodeKey)) {
      throw new Error("Invalid IndexedDB foreground node lease pool");
    }
    nodes.add(nodeKey);
  }
}

function assertStoredForegroundNodeLease(
  value: unknown,
): asserts value is StoredForegroundNodeLease {
  if (!value || typeof value !== "object") {
    throw new Error("Invalid IndexedDB foreground node lease");
  }
  const lease = value as Partial<StoredForegroundNodeLease>;
  if (
    !isLeaseId(lease.leaseId) ||
    !isNodeBuffer(lease.node) ||
    !isCanonicalNonNegativeBigintString(lease.confirmedTxTime) ||
    !isTxTime(BigInt(lease.confirmedTxTime))
  ) {
    throw new Error("Invalid IndexedDB foreground node lease");
  }
}

function isNodeBuffer(value: unknown): value is ArrayBuffer {
  return value instanceof ArrayBuffer && value.byteLength === INDEXEDDB_REPLICA_NODE_BYTES;
}

function isLeaseId(value: unknown): value is string {
  return (
    typeof value === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)
  );
}

function isBrowserWorkerEpoch(value: unknown): value is string {
  return (
    typeof value === "string" &&
    /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value)
  );
}

function isCanonicalNonNegativeBigintString(value: unknown): value is string {
  return typeof value === "string" && /^(0|[1-9][0-9]*)$/.test(value);
}

function isTxTime(value: bigint): boolean {
  return value >= 0n && value <= MAX_TX_TIME;
}

function bytesKey(bytes: ArrayBuffer): string {
  return Array.from(new Uint8Array(bytes), (byte) => byte.toString(16).padStart(2, "0")).join("");
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
