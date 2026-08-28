/**
 * Real-browser physical epoch receipts for the raw IndexedDB page store.
 * This intentionally bypasses MemoryPageStore and the SharedWorker: it writes
 * the committed page-v2 fixture through the browser's own IndexedDB API, then
 * exercises the production adapter against that exact namespace.
 */
import { afterEach, describe, expect, it } from "vitest";
import pageV2LeafHex from "../../../../crates/idb-tree/fixtures/page-v2-leaf.hex?raw";
import {
  INDEXEDDB_BTREE_DATABASE_VERSION,
  INDEXEDDB_BTREE_FORMAT_MAGIC,
  INDEXEDDB_BTREE_FORMAT_VERSION,
  INDEXEDDB_BTREE_METADATA_STORE,
  INDEXEDDB_BTREE_PAGE_SIZE,
  INDEXEDDB_BTREE_PAGES_STORE,
  INDEXEDDB_STORAGE_MANIFEST,
  INDEXEDDB_STORAGE_MANIFEST_KEY,
  INDEXEDDB_STORAGE_MANIFEST_STORE,
  IndexedDbPageStore,
} from "../../src/runtime/indexeddb-page-store.js";

const databaseNames: string[] = [];

afterEach(async () => {
  await Promise.all(databaseNames.splice(0).map((name) => IndexedDbPageStore.destroy(name)));
});

describe("IndexedDB physical epoch", () => {
  it("opens a manually committed epoch-one page-v2 fixture read-only, writes current data, and reopens", async () => {
    const name = databaseName();
    const page = hexBytes(pageV2LeafHex);
    await installEpochOneFixture(name, INDEXEDDB_STORAGE_MANIFEST, page);

    let store = await IndexedDbPageStore.open(name);
    expect(await store.metadata()).toMatchObject({ generation: 1, rootPageId: 1, nextPageId: 2 });
    expect(await store.readPage(1)).toEqual(page);
    await store.commit({
      expectedGeneration: 1,
      metadata: { pageSize: INDEXEDDB_BTREE_PAGE_SIZE, rootPageId: 1, nextPageId: 3 },
      pages: new Map([[2, new Uint8Array([0xca, 0xfe])]]),
    });
    store.close();

    store = await IndexedDbPageStore.open(name);
    expect(await store.readPage(1)).toEqual(page);
    expect(await store.readPage(2)).toEqual(new Uint8Array([0xca, 0xfe]));
    expect((await store.metadata())?.generation).toBe(2);
    store.close();
  });

  it("rejects corrupt, unknown, and extra epoch-manifest fields before a page mutation", async () => {
    for (const manifest of [
      undefined,
      { ...INDEXEDDB_STORAGE_MANIFEST, storageEpoch: 2 },
      { ...INDEXEDDB_STORAGE_MANIFEST, requiredCodecIds: ["unknown.codec"] },
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
    store.close();
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
  await transactionDone(tx);
  db.close();
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
