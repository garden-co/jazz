import {
  SQLITE_BROWSER_DATABASE_VERSION,
  SQLITE_BROWSER_ROOT_KEY,
  decodeSqliteBrowserRoot,
  encodeSqliteBrowserRoot,
  sqliteBrowserDescriptor,
  sqliteReplicaKey,
  type SqliteBrowserRoot,
} from "./browser-sqlite-storage.js";
import {
  acquireBrowserPhysicalDatabaseEpoch,
  claimBrowserStorageAdmissionOwnership,
  claimBrowserReclamationOwnership,
  type BrowserPhysicalDatabaseEpoch,
} from "./browser-physical-database-epoch.js";
import { loadWasmModule } from "./wasm-loader.js";
import type { RuntimeSourcesConfig } from "./context.js";
import { UnsupportedStorageCodecsError } from "./unsupported-storage-codecs-error.js";

export { UnsupportedStorageCodecsError };

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

/** The upstream metadata layout declares the exact SQLite file codec it owns. */
export const JAZZ_EPOCH_1_STORAGE_CODEC_IDS = ["jazz.sqlite-storage.v2"] as const;

/**
 * The entire browser physical-open contract. Keep this exact shape pinned:
 * unknown fields are decode-relevant until a later storage epoch says otherwise.
 */
export interface IndexedDbStorageManifest {
  storageEpoch: typeof INDEXEDDB_STORAGE_EPOCH;
  adapterId: "jazz-idb-tree";
  adapterFormatVersion: typeof INDEXEDDB_BTREE_FORMAT_VERSION;
  requiredCodecIds: readonly string[];
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
  private reclamationOwnership: (() => boolean) | null = null;
  private readonly treeClaims = new Set<number>();
  private nextTreeToken = 0;
  private readonly treeTransactions = new Set<Promise<unknown>>();
  private ownershipRevision = 0;
  private admissionLive: (() => boolean) | null = null;
  private ownedEpoch: BrowserPhysicalDatabaseEpoch | null = null;
  private borrowedEpoch: BrowserPhysicalDatabaseEpoch | null = null;
  private closing: Promise<void> | null = null;
  private sqliteRoot: SqliteBrowserRoot | null = null;
  private sqliteOpening: Promise<unknown> | null = null;
  private sqliteRuntime: { close(): Promise<unknown> } | null = null;
  private sqliteReserved = false;
  private sqliteRetiring: Promise<void> | null = null;
  private runtimeSources?: RuntimeSourcesConfig;

  /** One independently opened tree per exclusive owner; IdbTree clones share it. */
  claimTreeOwnership(): number {
    if (this.sqliteRoot)
      throw new Error("Groove page operations are unsupported on SQLite browser storage");
    this.assertValid();
    if (this.reclamationOwnership && (this.treeClaims.size > 0 || this.treeTransactions.size > 0)) {
      throw new Error("IndexedDB reclamation owner already has a tree or pending transaction");
    }
    if (this.nextTreeToken === Number.MAX_SAFE_INTEGER)
      throw new Error("IndexedDB tree token space exhausted");
    const token = ++this.nextTreeToken;
    this.treeClaims.add(token);
    return token;
  }

  releaseTreeOwnership(token: number): void {
    this.treeClaims.delete(token);
  }

  isTreeOwnershipActive(token: number): boolean {
    return (
      !this.invalidated &&
      this.treeClaims.has(token) &&
      (this.reclamationOwnership === null || this.reclamationOwnership())
    );
  }

  /** Revoke cached handles first, then drain every already-started tree commit. */
  retireTreeOwnership(): Promise<void> {
    if (this.sqliteRetiring) return this.sqliteRetiring;
    this.treeClaims.clear();
    this.ownershipRevision++;
    const retiring = (async () => {
      await this.sqliteOpening?.catch(() => undefined);
      const runtime = this.sqliteRuntime;
      // A failed close may still own the SQLite file. Keep both the runtime
      // and its reservation until a later retirement closes it successfully.
      await runtime?.close();
      this.sqliteRuntime = null;
      await Promise.allSettled(this.treeTransactions);
      this.sqliteReserved = false;
    })();
    this.sqliteRetiring = retiring;
    return retiring.finally(() => {
      if (this.sqliteRetiring === retiring) this.sqliteRetiring = null;
    });
  }

  get canReclaimObsoletePages(): boolean {
    return (
      !this.invalidated && this.treeClaims.size === 1 && this.reclamationOwnership?.() === true
    );
  }

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
      epoch?: BrowserPhysicalDatabaseEpoch;
      runtimeSources?: RuntimeSourcesConfig;
      /** SQLite uses this store only for ownership, identity and lease records. */
      format?: "sqlite";
    } = {},
  ): Promise<IndexedDbPageStore> {
    if (options.format === "sqlite" && (!options.owner || !name))
      throw new Error("SQLite browser storage requires a name and exact owner");
    const epoch = options.epoch ?? (await acquireBrowserPhysicalDatabaseEpoch(name));
    let live: () => boolean;
    let request: IDBOpenDBRequest;
    try {
      live = claimBrowserStorageAdmissionOwnership(epoch, name);
      request = indexedDB.open(
        name,
        options.format === "sqlite"
          ? SQLITE_BROWSER_DATABASE_VERSION
          : INDEXEDDB_BTREE_DATABASE_VERSION,
      );
    } catch (error) {
      if (!options.epoch) await epoch.release();
      throw error;
    }
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
          const node = randomReplicaNodeBytes();
          manifest.put(node.buffer, INDEXEDDB_REPLICA_NODE_KEY);
          if (options.format === "sqlite") {
            manifest.put(
              encodeSqliteBrowserRoot({
                name,
                owner: options.owner!,
                node: sqliteReplicaKey(node),
                state: "opening",
              }),
              SQLITE_BROWSER_ROOT_KEY,
            );
            manifest.put(options.owner!, INDEXEDDB_BROWSER_RUNTIME_OWNER_KEY);
          }
        }
      }
    };
    let db: IDBDatabase;
    try {
      db = await requestResult(request);
    } catch (error) {
      if (!options.epoch) await epoch.release();
      if (rejectedPreSettlementDatabase) {
        throw new Error("Missing or invalid IndexedDB storage epoch manifest");
      }
      throw error;
    }
    const store = new IndexedDbPageStore(db, name, options.onInvalidated);
    store.admissionLive = live;
    store.runtimeSources = options.runtimeSources;
    store.ownedEpoch = options.epoch ? null : epoch;
    store.borrowedEpoch = options.epoch ?? null;
    try {
      await store.admitStorage();
      if (options.owner) {
        const tx = db.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readonly");
        const done = transactionDone(tx);
        const owner = await requestResult(
          tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE).get(INDEXEDDB_BROWSER_RUNTIME_OWNER_KEY),
        );
        await done;
        if (owner !== undefined && owner !== options.owner) {
          throw new Error(
            `IndexedDB database ${name} is already owned by a different Jazz browser session`,
          );
        }
      }
      if (options.format === "sqlite") await store.admitSqliteStorage(options.owner!);
      if (options.owner) await store.claimBrowserRuntimeOwner(options.owner);
      return store;
    } catch (error) {
      await store.close();
      throw error;
    }
  }

  /** Reserve the sole SQLite owner before opening its VFS file. */
  async openSqliteRuntime<T extends { flush(): Promise<unknown>; close(): Promise<unknown> }>(
    open: (storage: ReturnType<typeof sqliteBrowserDescriptor>) => Promise<T>,
  ): Promise<T> {
    this.assertValid();
    if (!this.sqliteRoot || this.sqliteRoot.state === "resetting")
      throw new Error("SQLite browser storage is not admitted");
    if (this.sqliteReserved || this.sqliteRetiring)
      throw new Error("SQLite browser storage already has a runtime owner");
    this.sqliteReserved = true;
    const revision = this.ownershipRevision;
    const assertOwner = () => {
      this.assertValid();
      if (revision !== this.ownershipRevision)
        throw new Error("SQLite runtime ownership was retired");
    };
    const opening = (async () => {
      let runtime: T | undefined;
      try {
        runtime = await open(sqliteBrowserDescriptor(this.sqliteRoot!));
        assertOwner();
        // A ready sidecar promises that this exact file reached IndexedDB.
        await runtime.flush();
        assertOwner();
        if (this.sqliteRoot!.state === "opening") {
          const root: SqliteBrowserRoot = { ...this.sqliteRoot!, state: "ready" };
          const tx = this.db.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readwrite", {
            durability: "strict",
          });
          const done = transactionDone(tx);
          tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE).put(
            encodeSqliteBrowserRoot(root),
            SQLITE_BROWSER_ROOT_KEY,
          );
          await done;
          this.sqliteRoot = root;
        }
        assertOwner();
        this.sqliteRuntime = runtime;
        return runtime;
      } catch (error) {
        if (runtime) {
          this.sqliteRuntime = runtime;
          try {
            await runtime.close();
            this.sqliteRuntime = null;
          } catch (cleanupError) {
            // Publishing failed, but cleanup has not released the file.
            // Explicit close/retirement retries this same runtime.
            throw new AggregateError(
              [error, cleanupError],
              "SQLite runtime open failed and its cleanup could not close the file",
              { cause: error },
            );
          }
        }
        this.sqliteReserved = false;
        throw error;
      }
    })();
    this.sqliteOpening = opening;
    try {
      return await opening;
    } finally {
      if (this.sqliteOpening === opening) this.sqliteOpening = null;
    }
  }

  private async admitSqliteStorage(owner: string): Promise<void> {
    this.replicaNodeBytes = await this.readReplicaNodeBytes();
    const tx = this.db.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readonly");
    const done = transactionDone(tx);
    const value = await requestResult(
      tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE).get(SQLITE_BROWSER_ROOT_KEY),
    );
    await done;
    this.sqliteRoot = decodeSqliteBrowserRoot(value, this.name, owner, this.replicaNodeBytes);
    if (this.sqliteRoot.state !== "resetting") return;
    // A crash after deleting the file must never reuse its old transaction node.
    const wasm = await loadWasmModule(this.runtimeSources);
    await wasm.WasmDb.destroyBrowserStorage(sqliteBrowserDescriptor(this.sqliteRoot), this.name);
    this.assertValid();
    const node = randomReplicaNodeBytes();
    const root: SqliteBrowserRoot = {
      name: this.name,
      owner,
      node: sqliteReplicaKey(node),
      state: "opening",
    };
    const reset = this.db.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readwrite", {
      durability: "strict",
    });
    const resetDone = transactionDone(reset);
    const plane = reset.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE);
    plane.clear();
    plane.put(INDEXEDDB_STORAGE_MANIFEST, INDEXEDDB_STORAGE_MANIFEST_KEY);
    plane.put(encodeSqliteBrowserRoot(root), SQLITE_BROWSER_ROOT_KEY);
    plane.put(nodeBytesToBuffer(node), INDEXEDDB_REPLICA_NODE_KEY);
    plane.put(owner, INDEXEDDB_BROWSER_RUNTIME_OWNER_KEY);
    await resetDone;
    this.replicaNodeBytes = node;
    this.sqliteRoot = root;
  }

  /** The caller retains the live physical Web Lock after closing this handle. */
  private async destroySqliteFile(): Promise<void> {
    const request = indexedDB.open(this.name, SQLITE_BROWSER_DATABASE_VERSION);
    request.onupgradeneeded = () => {
      const probe = request.result;
      request.transaction?.abort();
      probe.close();
    };
    const db = await requestResult(request);
    try {
      const tx = db.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readwrite", {
        durability: "strict",
      });
      const done = transactionDone(tx);
      const plane = tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE);
      const value = await requestResult(plane.get(SQLITE_BROWSER_ROOT_KEY));
      const root = decodeSqliteBrowserRoot(
        value,
        this.name,
        this.sqliteRoot!.owner,
        this.replicaNodeBytes!,
      );
      root.state = "resetting";
      plane.put(encodeSqliteBrowserRoot(root), SQLITE_BROWSER_ROOT_KEY);
      await done;
      this.sqliteRoot = root;
      const wasm = await loadWasmModule(this.runtimeSources);
      await wasm.WasmDb.destroyBrowserStorage(sqliteBrowserDescriptor(root), this.name);
    } finally {
      db.close();
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
  async claimBrowserWorkerEpoch(
    epoch: string,
    ownership?: BrowserPhysicalDatabaseEpoch,
  ): Promise<void> {
    if (!isBrowserWorkerEpoch(epoch)) throw new Error("Invalid browser worker epoch");
    this.assertValid();
    if (ownership && (ownership.id !== epoch || this.treeClaims.size > 0)) {
      throw new Error("IndexedDB reclamation ownership must precede tree construction");
    }
    const revision = this.ownershipRevision;
    const proof = ownership ? claimBrowserReclamationOwnership(ownership, this.name) : null;
    const tx = this.db.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readwrite");
    const done = transactionDone(tx);
    tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE).put(
      { format: INDEXEDDB_BROWSER_WORKER_EPOCH_FORMAT, epoch },
      INDEXEDDB_BROWSER_WORKER_EPOCH_KEY,
    );
    await done;
    this.assertValid();
    if (revision !== this.ownershipRevision) {
      throw new Error("IndexedDB worker ownership was released while claiming");
    }
    this.reclamationOwnership = proof;
  }

  /** Delete only this realm's epoch; a stale realm must never clear its successor. */
  async releaseBrowserWorkerEpoch(epoch: string): Promise<void> {
    this.treeClaims.clear();
    this.ownershipRevision++;
    this.reclamationOwnership = null;
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
    if (this.sqliteRoot)
      throw new Error("Groove page operations are unsupported on SQLite browser storage");
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

  /** Refuse an incompatible root before ownership, leases or file access. */
  private async admitStorage(): Promise<void> {
    const tx = this.db.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readonly");
    const done = transactionDone(tx);
    const value = await requestResult(
      tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE).get(INDEXEDDB_STORAGE_MANIFEST_KEY),
    );
    await done;
    assertStorageManifest(value);
    this.replicaNodeBytes = await this.readReplicaNodeBytes();
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
    const tx = this.sqliteRoot
      ? this.db.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readwrite", { durability: "strict" })
      : relaxedReadWriteTransaction(this.db, [INDEXEDDB_STORAGE_MANIFEST_STORE]);
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
    return (await this.readPages([pageId]))[0]!;
  }

  /**
   * Read several pages in one readonly transaction, one slot per requested id
   * in request order. A cold B-tree scan asks for a whole tree level at once,
   * so this replaces one transaction per page with one per level.
   */
  async readPages(pageIds: readonly number[]): Promise<(Uint8Array | null)[]> {
    this.assertValid();
    for (const pageId of pageIds) assertPageId(pageId);
    if (pageIds.length === 0) return [];
    const tx = this.db.transaction(INDEXEDDB_BTREE_PAGES_STORE, "readonly");
    const done = transactionDone(tx);
    const store = tx.objectStore(INDEXEDDB_BTREE_PAGES_STORE);
    let values: unknown[];
    try {
      values = await Promise.all(pageIds.map((pageId) => requestResult(store.get(pageId))));
    } catch (error) {
      await done.catch(() => undefined);
      throw error;
    }
    await done;
    return values.map((value, index) => pageBytes(pageIds[index]!, value));
  }

  async commit(commit: IndexedDbPageCommit, treeToken?: number): Promise<IndexedDbBtreeMetadata> {
    this.assertValid();
    assertCommit(commit);
    if (treeToken !== undefined && !this.isTreeOwnershipActive(treeToken)) {
      throw new Error("IndexedDB tree ownership has expired");
    }
    const requiresOwnership = (commit.deletedPageIds?.length ?? 0) > 0;
    if (requiresOwnership && !this.canReclaimObsoletePages) {
      throw new Error("IndexedDB page reclamation ownership is not active");
    }
    const tx = relaxedReadWriteTransaction(this.db, [
      INDEXEDDB_BTREE_PAGES_STORE,
      INDEXEDDB_BTREE_METADATA_STORE,
    ]);
    const done = transactionDone(tx);
    this.treeTransactions.add(done);
    void done.then(
      () => this.treeTransactions.delete(done),
      () => this.treeTransactions.delete(done),
    );
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
      this.assertValid();
      if (
        metadata.rootPageId !== null &&
        !commit.pages.has(metadata.rootPageId) &&
        (await requestResult(pages.get(metadata.rootPageId))) === undefined
      ) {
        throw new Error(`IndexedDB B-tree root page ${metadata.rootPageId} is missing`);
      }
      if (treeToken !== undefined && !this.isTreeOwnershipActive(treeToken)) {
        throw new Error("IndexedDB tree ownership expired before publication");
      }
      if (requiresOwnership && !this.canReclaimObsoletePages) {
        throw new Error("IndexedDB page reclamation ownership expired before publication");
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
    treeToken?: number,
  ): Promise<IndexedDbBtreeMetadata> {
    if (pageIds.length !== pageBytes.length) {
      return Promise.reject(new Error("IDBTree page ids and page bytes have different lengths"));
    }
    const operation = this.commit(
      {
        expectedGeneration,
        metadata: {
          pageSize,
          rootPageId: rootPageId < 0 ? null : rootPageId,
          nextPageId,
        },
        pages: new Map(pageIds.map((pageId, index) => [pageId, pageBytes[index]!])),
        deletedPageIds,
      },
      treeToken,
    );
    return operation;
  }

  close(): Promise<void> {
    if (this.closing) return this.closing;
    this.treeClaims.clear();
    this.invalidated = true;
    this.ownershipRevision++;
    this.reclamationOwnership = null;
    this.removeInvalidationListeners();
    if (!this.sqliteRoot) this.db.close();
    this.closing = (async () => {
      await this.retireTreeOwnership();
      this.db.close();
      if (this.ownedEpoch) {
        await this.ownedEpoch.release();
        this.ownedEpoch = null;
      }
    })().catch((error) => {
      // Calls remain fenced by invalidated, but close itself is retryable.
      // The physical Web Lock stays held until the file owner retires.
      this.closing = null;
      throw error;
    });
    return this.closing;
  }

  async clear(): Promise<void> {
    if (this.sqliteRoot)
      throw new Error("Groove page operations are unsupported on SQLite browser storage");
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
    const epoch = await acquireBrowserPhysicalDatabaseEpoch(name);
    let db: IDBDatabase | undefined;
    try {
      const live = claimBrowserStorageAdmissionOwnership(epoch, name);
      let missing = false;
      const request = indexedDB.open(name);
      request.onupgradeneeded = () => {
        missing = true;
        const probe = request.result;
        request.transaction?.abort();
        probe.close();
      };
      try {
        db = await requestResult(request);
      } catch (error) {
        if (!missing) throw error;
      }
      if (db?.version === SQLITE_BROWSER_DATABASE_VERSION) {
        const tx = db.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readonly");
        const done = transactionDone(tx);
        const plane = tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE);
        const [owner, value] = await Promise.all([
          requestResult(plane.get(INDEXEDDB_BROWSER_RUNTIME_OWNER_KEY)),
          requestResult(plane.get(SQLITE_BROWSER_ROOT_KEY)),
        ]);
        await done;
        if (value !== undefined) {
          const store = new IndexedDbPageStore(db, name);
          store.admissionLive = live;
          store.borrowedEpoch = epoch;
          store.replicaNodeBytes = await store.readReplicaNodeBytes();
          if (typeof owner !== "string" || !owner)
            throw new Error("Invalid SQLite browser storage owner");
          store.sqliteRoot = decodeSqliteBrowserRoot(value, name, owner, store.replicaNodeBytes);
          await store.destroyUnderOwner(epoch);
        } else {
          db.close();
          await IndexedDbPageStore.deletePhysicalRoot(name);
        }
      } else {
        db?.close();
        await IndexedDbPageStore.deletePhysicalRoot(name);
      }
    } finally {
      db?.close();
      await epoch.release();
    }
  }

  /** Broker-private reset capability: the exact store and its borrowed epoch. */
  async destroyUnderOwner(epoch: BrowserPhysicalDatabaseEpoch): Promise<void> {
    if (this.borrowedEpoch !== epoch || !this.admissionLive?.()) {
      throw new Error("IndexedDB deletion requires this store's live physical owner");
    }
    await this.close();
    if (!this.admissionLive()) throw new Error("IndexedDB physical owner expired before deletion");
    if (this.sqliteRoot) await this.destroySqliteFile();
    await IndexedDbPageStore.deletePhysicalRoot(this.name);
  }

  private static async deletePhysicalRoot(name: string): Promise<void> {
    const request = indexedDB.deleteDatabase(name);
    await new Promise<void>((resolve, reject) => {
      request.onsuccess = () => resolve();
      request.onerror = () => reject(request.error ?? new Error("IndexedDB deletion failed"));
      // deleteDatabase cannot be cancelled. A blocked request must retain the
      // lock until it settles, rather than deleting a later owner's root.
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
    void this.close();
    const error = new IndexedDbStorageInvalidatedError(this.name);
    for (const listener of this.invalidationListeners) listener(error);
  }

  private assertValid(): void {
    if (this.invalidated) throw new IndexedDbStorageInvalidatedError(this.name);
    if (this.admissionLive && !this.admissionLive()) {
      throw new Error("IndexedDB admission ownership has expired");
    }
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

function pageBytes(pageId: number, value: unknown): Uint8Array | null {
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

function assertStorageManifest(
  value: unknown,
  expected: IndexedDbStorageManifest = INDEXEDDB_STORAGE_MANIFEST,
): asserts value is IndexedDbStorageManifest {
  if (!value || typeof value !== "object") {
    throw new Error("Missing or invalid IndexedDB storage epoch manifest");
  }
  const manifest = value as Partial<IndexedDbStorageManifest>;
  const keys = Object.keys(manifest).sort();
  const expectedKeys = Object.keys(expected).sort();
  if (
    keys.length !== expectedKeys.length ||
    keys.some((key, index) => key !== expectedKeys[index]) ||
    manifest.storageEpoch !== expected.storageEpoch ||
    manifest.adapterId !== expected.adapterId ||
    manifest.adapterFormatVersion !== expected.adapterFormatVersion ||
    manifest.pageSize !== expected.pageSize ||
    manifest.pageChecksum !== expected.pageChecksum ||
    manifest.pageFormatMagic !== expected.pageFormatMagic ||
    manifest.pageFormatVersion !== expected.pageFormatVersion ||
    !Array.isArray(manifest.requiredCodecIds) ||
    !manifest.requiredCodecIds.every((codec: unknown) => typeof codec === "string")
  ) {
    throw new Error("Missing or invalid IndexedDB storage epoch manifest");
  }
  const found: readonly string[] = manifest.requiredCodecIds;
  const expectedCodecs: readonly string[] = expected.requiredCodecIds;
  if (
    found.length !== expectedCodecs.length ||
    found.some((codec, index) => codec !== expectedCodecs[index])
  ) {
    // A well-formed epoch manifest whose codec-family inventory differs from
    // the one this build reads. Raised before any page is decoded; the root
    // is left untouched. The message keeps the generic manifest-rejection
    // prefix so existing matches on it still hold.
    const missing = expectedCodecs.filter((codec) => !found.includes(codec));
    const unknown = found.filter((codec) => !expectedCodecs.includes(codec));
    throw new UnsupportedStorageCodecsError(
      missing,
      unknown,
      `Missing or invalid IndexedDB storage epoch manifest: unsupported storage format: ` +
        `this epoch-${expected.storageEpoch} root lacks codec families ${JSON.stringify(missing)} ` +
        `required by this build and declares ${JSON.stringify(unknown)} that this build does not read`,
    );
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
