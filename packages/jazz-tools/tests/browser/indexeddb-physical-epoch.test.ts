import { acquireBrowserPhysicalDatabaseEpoch } from "../../src/runtime/browser-physical-database-epoch.js";
/**
 * Real-browser physical epoch receipts for the raw IndexedDB page store.
 * This intentionally bypasses MemoryPageStore and the SharedWorker: it writes
 * the committed page-v1 fixture through the browser's own IndexedDB API, then
 * exercises the production adapter against that exact namespace.
 */
import { afterEach, describe, expect, it } from "vitest";
import pageV1LeafHex from "../../../../crates/idb-tree/fixtures/page-v1-leaf.hex?raw";
import {
  INDEXEDDB_BTREE_DATABASE_VERSION,
  INDEXEDDB_BTREE_FORMAT_MAGIC,
  INDEXEDDB_BTREE_FORMAT_VERSION,
  INDEXEDDB_BTREE_METADATA_STORE,
  INDEXEDDB_BTREE_PAGE_SIZE,
  INDEXEDDB_BTREE_PAGES_STORE,
  INDEXEDDB_STORAGE_MANIFEST,
  INDEXEDDB_EPOCH_1_STORAGE_MANIFEST,
  INDEXEDDB_STORAGE_ADMISSION_KEY,
  INDEXEDDB_STORAGE_MANIFEST_KEY,
  INDEXEDDB_STORAGE_MANIFEST_STORE,
  INDEXEDDB_REPLICA_NODE_BYTES,
  INDEXEDDB_REPLICA_NODE_KEY,
  IndexedDbPageStore,
} from "../../src/runtime/indexeddb-page-store.js";

const databaseNames: string[] = [];

afterEach(async () => {
  await Promise.all(databaseNames.splice(0).map((name) => IndexedDbPageStore.destroy(name)));
});

describe("IndexedDB physical epoch", () => {
  it("reclaims pages only during one live Web Lock epoch and reopens the published closure", async () => {
    const name = databaseName();
    const epoch = await acquireBrowserPhysicalDatabaseEpoch(name);
    const store = await IndexedDbPageStore.open(name, { epoch });
    try {
      await store.claimBrowserWorkerEpoch(epoch.id, epoch);
      store.claimTreeOwnership();
      expect(store.canReclaimObsoletePages).toBe(true);
      await expect(acquireBrowserPhysicalDatabaseEpoch(name)).rejects.toThrow("active in another");
      await store.commit({
        expectedGeneration: 0,
        metadata: { pageSize: INDEXEDDB_BTREE_PAGE_SIZE, rootPageId: 1, nextPageId: 2 },
        pages: new Map([[1, new Uint8Array([1])]]),
      });
      await store.commit({
        expectedGeneration: 1,
        metadata: { pageSize: INDEXEDDB_BTREE_PAGE_SIZE, rootPageId: 2, nextPageId: 3 },
        pages: new Map([[2, new Uint8Array([2])]]),
        deletedPageIds: [1],
      });
      expect(await store.readPage(1)).toBeNull();
      expect(await store.readPage(2)).toEqual(new Uint8Array([2]));
      const release = store.releaseBrowserWorkerEpoch(epoch.id);
      expect(store.canReclaimObsoletePages).toBe(false);
      await release;
    } finally {
      await store.close();
      await epoch.release();
    }
    const reopened = await IndexedDbPageStore.open(name);
    try {
      expect(reopened.canReclaimObsoletePages).toBe(false);
      expect((await reopened.metadata())?.rootPageId).toBe(2);
      expect(await reopened.readPage(2)).toEqual(new Uint8Array([2]));
      expect(await reopened.readPage(1)).toBeNull();
    } finally {
      await reopened.close();
    }
  });

  it("excludes independent openers until close releases the physical root", async () => {
    const name = databaseName();
    const first = await IndexedDbPageStore.open(name);
    const node = first.replicaNode;
    await expect(IndexedDbPageStore.open(name)).rejects.toThrow("active in another");
    await first.close();
    const second = await IndexedDbPageStore.open(name);
    expect(second.replicaNode).toEqual(node);
    await second.close();
  });

  it("preserves a replica node across reopen and replaces it only after physical reset", async () => {
    const name = databaseName();
    const first = await IndexedDbPageStore.open(name);
    const firstNode = first.replicaNode;
    await first.close();

    const reopened = await IndexedDbPageStore.open(name);
    expect(reopened.replicaNode).toEqual(firstNode);
    await reopened.close();

    await IndexedDbPageStore.destroy(name);
    const reset = await IndexedDbPageStore.open(name);
    try {
      expect(reset.replicaNode).not.toEqual(firstNode);
    } finally {
      await reset.close();
    }
  });

  it("migrates an empty exact E1 root through WASM without creating tree metadata", async () => {
    const name = databaseName();
    const raw = await createRawEpochDatabase(name);
    const tx = raw.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readwrite");
    tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE).put(
      INDEXEDDB_EPOCH_1_STORAGE_MANIFEST,
      INDEXEDDB_STORAGE_MANIFEST_KEY,
    );
    tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE).put(
      epochOneReplicaNode().buffer,
      INDEXEDDB_REPLICA_NODE_KEY,
    );
    await transactionDone(tx);
    raw.close();
    const store = await IndexedDbPageStore.open(name);
    expect(await store.metadata()).toBeNull();
    expect(store.storageAdmission()[1]?.slice(0, 5)).toEqual(new Uint8Array([74, 83, 65, 49, 1]));
    await store.close();
    const reopened = await IndexedDbPageStore.open(name);
    expect(await reopened.metadata()).toBeNull();
    expect(reopened.replicaNode).toEqual(epochOneReplicaNode());
    await reopened.close();
  });

  it("rejects non-Jazz legacy pages through WASM without publishing admission", async () => {
    const name = databaseName();
    const page = hexBytes(pageV1LeafHex);
    await installEpochOneFixture(name, INDEXEDDB_EPOCH_1_STORAGE_MANIFEST, page);
    await expect(IndexedDbPageStore.open(name)).rejects.toThrow();
    expect(await rawPage(name, 1)).toEqual(page);
    const raw = await openRawDatabase(name);
    const tx = raw.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readonly");
    const plane = tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE);
    expect(await requestResult(plane.get(INDEXEDDB_STORAGE_MANIFEST_KEY))).toEqual(
      INDEXEDDB_EPOCH_1_STORAGE_MANIFEST,
    );
    expect(await requestResult(plane.get(INDEXEDDB_STORAGE_ADMISSION_KEY))).toBeUndefined();
    await transactionDone(tx);
    raw.close();
    // A failed preflight relinquishes the lock; the same root reaches the
    // same corruption rejection, not a stranded physical-owner conflict.
    await expect(IndexedDbPageStore.open(name)).rejects.not.toThrow("active in another");
  });

  it("rejects corrupt, unknown, and extra epoch-manifest fields before a page mutation", async () => {
    for (const manifest of [
      undefined,
      { ...INDEXEDDB_STORAGE_MANIFEST, storageEpoch: 99 },
      { ...INDEXEDDB_STORAGE_MANIFEST, requiredCodecIds: ["unknown.codec"] },
      { ...INDEXEDDB_STORAGE_MANIFEST, adapterFormatVersion: 2 },
      { ...INDEXEDDB_STORAGE_MANIFEST, pageFormatVersion: 2 },
      { ...INDEXEDDB_STORAGE_MANIFEST, futureDecodeParameter: "unknown" },
    ]) {
      const name = databaseName();
      const page = new Uint8Array([0x99]);
      await installEpochOneFixture(name, manifest, page);
      await expect(IndexedDbPageStore.open(name)).rejects.toThrow(
        "Missing or invalid IndexedDB storage epoch manifest",
      );
      expect(await rawPage(name, 1)).toEqual(page);
    }
  });

  it("fails closed for a torn or old epoch-one namespace without its replica node", async () => {
    for (const replicaNode of [null, new Uint8Array(INDEXEDDB_REPLICA_NODE_BYTES - 1)]) {
      const name = databaseName();
      const page = new Uint8Array([0x99]);
      await installEpochOneFixture(name, INDEXEDDB_STORAGE_MANIFEST, page, replicaNode);
      await expect(IndexedDbPageStore.open(name)).rejects.toThrow(
        "Missing or invalid IndexedDB replica node identity",
      );
      expect(await rawPage(name, 1)).toEqual(page);
    }

    const tornName = databaseName();
    const torn = await createRawEpochDatabase(tornName);
    torn.close();
    await expect(IndexedDbPageStore.open(tornName)).rejects.toThrow(
      "Missing or invalid IndexedDB storage epoch manifest",
    );
  });

  it("keeps a stale-generation page outside the committed transaction", async () => {
    const name = databaseName();
    const store = await IndexedDbPageStore.open(name);
    await store.commit({
      expectedGeneration: 0,
      metadata: { pageSize: INDEXEDDB_BTREE_PAGE_SIZE, rootPageId: 1, nextPageId: 2 },
      pages: new Map([[1, new Uint8Array([1])]]),
    });
    await expect(
      store.commit({
        expectedGeneration: 0,
        metadata: { pageSize: INDEXEDDB_BTREE_PAGE_SIZE, rootPageId: 2, nextPageId: 3 },
        pages: new Map([[2, new Uint8Array([2])]]),
      }),
    ).rejects.toThrow("expected 0, found 1");
    expect(await rawPage(name, 2)).toBeNull();
    await store.close();
  });
});

function databaseName(): string {
  const name = `jazz-indexeddb-physical-epoch-${crypto.randomUUID()}`;
  databaseNames.push(name);
  return name;
}

async function installEpochOneFixture(
  name: string,
  manifest: unknown,
  page: Uint8Array,
  replicaNode: Uint8Array | null = epochOneReplicaNode(),
): Promise<void> {
  const db = await createRawEpochDatabase(name);
  const tx = db.transaction(
    [INDEXEDDB_BTREE_PAGES_STORE, INDEXEDDB_BTREE_METADATA_STORE, INDEXEDDB_STORAGE_MANIFEST_STORE],
    "readwrite",
  );
  tx.objectStore(INDEXEDDB_BTREE_PAGES_STORE).put(page.slice().buffer, 1);
  tx.objectStore(INDEXEDDB_BTREE_METADATA_STORE).put(
    {
      formatMagic: INDEXEDDB_BTREE_FORMAT_MAGIC,
      formatVersion: INDEXEDDB_BTREE_FORMAT_VERSION,
      pageSize: INDEXEDDB_BTREE_PAGE_SIZE,
      generation: 1,
      rootPageId: 1,
      nextPageId: 2,
    },
    "current",
  );
  if (manifest !== undefined) {
    tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE).put(manifest, INDEXEDDB_STORAGE_MANIFEST_KEY);
  }
  if (replicaNode !== null) {
    tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE).put(
      replicaNode.slice().buffer,
      INDEXEDDB_REPLICA_NODE_KEY,
    );
  }
  await transactionDone(tx);
  db.close();
}

function epochOneReplicaNode(): Uint8Array {
  return Uint8Array.from({ length: INDEXEDDB_REPLICA_NODE_BYTES }, (_, index) => index + 1);
}

async function rawPage(name: string, pageId: number): Promise<Uint8Array | null> {
  const db = await openRawDatabase(name);
  const tx = db.transaction(INDEXEDDB_BTREE_PAGES_STORE, "readonly");
  const value = await requestResult(tx.objectStore(INDEXEDDB_BTREE_PAGES_STORE).get(pageId));
  await transactionDone(tx);
  db.close();
  return value === undefined ? null : new Uint8Array(value as ArrayBuffer);
}

function openRawDatabase(name: string): Promise<IDBDatabase> {
  return requestResult(indexedDB.open(name, INDEXEDDB_BTREE_DATABASE_VERSION));
}

function createRawEpochDatabase(name: string): Promise<IDBDatabase> {
  const request = indexedDB.open(name, INDEXEDDB_BTREE_DATABASE_VERSION);
  request.onupgradeneeded = () => {
    const db = request.result;
    db.createObjectStore(INDEXEDDB_BTREE_PAGES_STORE);
    db.createObjectStore(INDEXEDDB_BTREE_METADATA_STORE);
    db.createObjectStore(INDEXEDDB_STORAGE_MANIFEST_STORE);
  };
  return requestResult(request);
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
    tx.onerror = () => reject(tx.error ?? new Error("IndexedDB transaction failed"));
    tx.onabort = () => reject(tx.error ?? new Error("IndexedDB transaction aborted"));
  });
}

function hexBytes(value: string): Uint8Array {
  const trimmed = value.trim();
  return Uint8Array.from(
    Array.from({ length: trimmed.length / 2 }, (_, index) =>
      Number.parseInt(trimmed.slice(index * 2, index * 2 + 2), 16),
    ),
  );
}
