import { indexedDB as fakeIndexedDb } from "fake-indexeddb";
import { readFile } from "node:fs/promises";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
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
} from "./indexeddb-page-store.js";

const databaseNames: string[] = [];

describe("IndexedDbPageStore", () => {
  beforeEach(() => {
    Object.defineProperty(globalThis, "indexedDB", {
      configurable: true,
      value: fakeIndexedDb,
    });
  });

  afterEach(async () => {
    await Promise.all(databaseNames.splice(0).map((name) => IndexedDbPageStore.destroy(name)));
  });

  it("atomically commits opaque pages with the root generation", async () => {
    const name = databaseName();
    const store = await IndexedDbPageStore.open(name);

    const metadata = await store.commit({
      expectedGeneration: 0,
      metadata: { pageSize: INDEXEDDB_BTREE_PAGE_SIZE, rootPageId: 7, nextPageId: 8 },
      pages: new Map([
        [7, new Uint8Array([1, 2, 3])],
        [3, new Uint8Array([4, 5])],
      ]),
    });

    expect(metadata.generation).toBe(1);
    expect(await store.metadata()).toEqual(metadata);
    expect(await store.readPage(7)).toEqual(new Uint8Array([1, 2, 3]));
    expect(await store.readPage(3)).toEqual(new Uint8Array([4, 5]));
    store.close();
  });

  it("persists only supplied dirty pages and can delete retired pages", async () => {
    const name = databaseName();
    let store = await IndexedDbPageStore.open(name);
    await store.commit({
      expectedGeneration: 0,
      metadata: { pageSize: INDEXEDDB_BTREE_PAGE_SIZE, rootPageId: 1, nextPageId: 3 },
      pages: new Map([
        [1, new Uint8Array([1])],
        [2, new Uint8Array([2])],
      ]),
    });
    await store.commit({
      expectedGeneration: 1,
      metadata: { pageSize: INDEXEDDB_BTREE_PAGE_SIZE, rootPageId: 1, nextPageId: 3 },
      pages: new Map([[1, new Uint8Array([9])]]),
      deletedPageIds: [2],
    });
    store.close();

    store = await IndexedDbPageStore.open(name);
    expect(await store.readPage(1)).toEqual(new Uint8Array([9]));
    expect(await store.readPage(2)).toBeNull();
    expect((await store.metadata())?.generation).toBe(2);
    store.close();
  });

  it("rejects a stale generation instead of overwriting a newer root", async () => {
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
    expect(await store.readPage(2)).toBeNull();
    expect((await store.metadata())?.rootPageId).toBe(1);
    store.close();
  });

  it("fails invalid commits before they can leave orphan pages", async () => {
    const name = databaseName();
    const store = await IndexedDbPageStore.open(name);
    await expect(
      store.commit({
        expectedGeneration: 0,
        metadata: { pageSize: 1025, rootPageId: 1, nextPageId: 2 },
        pages: new Map([[1, new Uint8Array([1])]]),
      }),
    ).rejects.toThrow("Invalid IndexedDB B-tree commit metadata");
    await expect(store.readPage(1)).resolves.toBeNull();
    await expect(store.metadata()).resolves.toBeNull();

    await expect(
      store.commit({
        expectedGeneration: 0,
        metadata: { pageSize: INDEXEDDB_BTREE_PAGE_SIZE, rootPageId: 2, nextPageId: 3 },
        pages: new Map([[1, new Uint8Array([1])]]),
      }),
    ).rejects.toThrow("root page 2 is missing");
    await expect(store.readPage(1)).resolves.toBeNull();
    await expect(store.metadata()).resolves.toBeNull();

    await expect(
      store.commit({
        expectedGeneration: 0,
        metadata: { pageSize: 4_294_967_297, rootPageId: 1, nextPageId: 2 },
        pages: new Map([[1, new Uint8Array([1])]]),
      }),
    ).rejects.toThrow("Invalid IndexedDB B-tree commit metadata");
    await expect(store.readPage(1)).resolves.toBeNull();
    store.close();
  });

  it("does not allow a commit to delete its published root", async () => {
    const name = databaseName();
    const store = await IndexedDbPageStore.open(name);
    await store.commit({
      expectedGeneration: 0,
      metadata: { pageSize: INDEXEDDB_BTREE_PAGE_SIZE, rootPageId: 1, nextPageId: 2 },
      pages: new Map([[1, new Uint8Array([1])]]),
    });
    await expect(
      store.commit({
        expectedGeneration: 1,
        metadata: { pageSize: INDEXEDDB_BTREE_PAGE_SIZE, rootPageId: 1, nextPageId: 2 },
        pages: new Map(),
        deletedPageIds: [1],
      }),
    ).rejects.toThrow("Invalid IndexedDB B-tree deleted page commit");
    await expect(store.readPage(1)).resolves.toEqual(new Uint8Array([1]));
    await expect(store.metadata()).resolves.toMatchObject({ generation: 1, rootPageId: 1 });
    store.close();
  });

  it("pins the raw metadata record and IndexedDB namespace", async () => {
    const name = databaseName();
    const store = await IndexedDbPageStore.open(name);
    await store.commit({
      expectedGeneration: 0,
      metadata: { pageSize: INDEXEDDB_BTREE_PAGE_SIZE, rootPageId: 1, nextPageId: 2 },
      pages: new Map([[1, new Uint8Array([1])]]),
    });
    const raw = await openRawDatabase(name);
    expect(raw.version).toBe(INDEXEDDB_BTREE_DATABASE_VERSION);
    expect([...raw.objectStoreNames]).toEqual([
      INDEXEDDB_BTREE_METADATA_STORE,
      INDEXEDDB_BTREE_PAGES_STORE,
      INDEXEDDB_STORAGE_MANIFEST_STORE,
    ]);
    const manifestTx = raw.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readonly");
    expect(
      await requestResult(
        manifestTx
          .objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE)
          .get(INDEXEDDB_STORAGE_MANIFEST_KEY),
      ),
    ).toEqual(INDEXEDDB_STORAGE_MANIFEST);
    const tx = raw.transaction(INDEXEDDB_BTREE_METADATA_STORE, "readonly");
    const value = await requestResult(
      tx.objectStore(INDEXEDDB_BTREE_METADATA_STORE).get("current"),
    );
    expect(value).toEqual({
      formatMagic: INDEXEDDB_BTREE_FORMAT_MAGIC,
      formatVersion: INDEXEDDB_BTREE_FORMAT_VERSION,
      pageSize: INDEXEDDB_BTREE_PAGE_SIZE,
      generation: 1,
      rootPageId: 1,
      nextPageId: 2,
    });
    raw.close();
    store.close();
  });

  it("rejects missing, unknown, and inconsistent manifests before touching pages", async () => {
    for (const manifest of [
      undefined,
      { ...INDEXEDDB_STORAGE_MANIFEST, storageEpoch: 99 },
      { ...INDEXEDDB_STORAGE_MANIFEST, pageChecksum: "none" },
      { ...INDEXEDDB_STORAGE_MANIFEST, adapterFormatVersion: 2 },
      { ...INDEXEDDB_STORAGE_MANIFEST, pageFormatVersion: 2 },
      { ...INDEXEDDB_STORAGE_MANIFEST, futureDecodeParameter: "unknown" },
    ]) {
      const name = databaseName();
      await installRawEpochOneFixture(name, manifest);
      await expect(IndexedDbPageStore.open(name)).rejects.toThrow(
        "Missing or invalid IndexedDB storage epoch manifest",
      );
      const raw = await openRawDatabase(name);
      const tx = raw.transaction(INDEXEDDB_BTREE_PAGES_STORE, "readonly");
      expect(await requestResult(tx.objectStore(INDEXEDDB_BTREE_PAGES_STORE).get(1))).toEqual(
        new Uint8Array([1]).buffer,
      );
      raw.close();
    }
  });

  it("does not mutate a pre-settlement version-two namespace when rejecting its missing manifest", async () => {
    const name = databaseName();
    const raw = await createLegacyVersionTwoDatabase(name);
    const tx = raw.transaction(INDEXEDDB_BTREE_PAGES_STORE, "readwrite");
    tx.objectStore(INDEXEDDB_BTREE_PAGES_STORE).put(new Uint8Array([7]).buffer, 1);
    await transactionDone(tx);
    raw.close();

    await expect(IndexedDbPageStore.open(name)).rejects.toThrow(
      "Missing or invalid IndexedDB storage epoch manifest",
    );
    const reopened = await openRawDatabase(name);
    try {
      expect(reopened.version).toBe(2);
      expect([...reopened.objectStoreNames]).toEqual([
        INDEXEDDB_BTREE_METADATA_STORE,
        INDEXEDDB_BTREE_PAGES_STORE,
      ]);
      const check = reopened.transaction(INDEXEDDB_BTREE_PAGES_STORE, "readonly");
      expect(await requestResult(check.objectStore(INDEXEDDB_BTREE_PAGES_STORE).get(1))).toEqual(
        new Uint8Array([7]).buffer,
      );
    } finally {
      reopened.close();
    }
  });

  it("accepts the typed-array wasm bridge without re-encoding page bodies", async () => {
    const name = databaseName();
    const store = await IndexedDbPageStore.open(name);
    const metadata = await store.commitPages(
      0,
      INDEXEDDB_BTREE_PAGE_SIZE,
      4,
      5,
      [4],
      [new Uint8Array([8, 6, 7, 5, 3, 0, 9])],
      [],
    );
    expect(metadata).toMatchObject({ generation: 1, rootPageId: 4, nextPageId: 5 });
    expect(await store.readPage(4)).toEqual(new Uint8Array([8, 6, 7, 5, 3, 0, 9]));
    store.close();
  });

  it("persists the Rust-owned v1 page fixture byte-for-byte", async () => {
    const fixture = hexBytes(
      await readFile(
        new URL("../../../../crates/idb-tree/fixtures/page-v1-leaf.hex", import.meta.url),
        "utf8",
      ),
    );
    const name = databaseName();
    const store = await IndexedDbPageStore.open(name);
    await store.commit({
      expectedGeneration: 0,
      metadata: { pageSize: INDEXEDDB_BTREE_PAGE_SIZE, rootPageId: 1, nextPageId: 2 },
      pages: new Map([[1, fixture]]),
    });
    expect(await store.readPage(1)).toEqual(fixture);
    store.close();
  });

  it("invalidates an open store when its IndexedDB database is externally deleted", async () => {
    const name = databaseName();
    const invalidations: Error[] = [];
    const store = await IndexedDbPageStore.open(name, (error) => invalidations.push(error));
    await store.commit({
      expectedGeneration: 0,
      metadata: { pageSize: INDEXEDDB_BTREE_PAGE_SIZE, rootPageId: 1, nextPageId: 2 },
      pages: new Map([[1, new Uint8Array([1])]]),
    });

    await IndexedDbPageStore.destroy(name);

    expect(invalidations).toHaveLength(1);
    expect(invalidations[0]?.message).toContain(name);
    await expect(store.metadata()).rejects.toThrow("storage was invalidated");
    await expect(store.readPage(1)).rejects.toThrow("storage was invalidated");
    await expect(
      store.commit({
        expectedGeneration: 1,
        metadata: { pageSize: INDEXEDDB_BTREE_PAGE_SIZE, rootPageId: 1, nextPageId: 2 },
        pages: new Map([[1, new Uint8Array([2])]]),
      }),
    ).rejects.toThrow("storage was invalidated");
  });
});

function databaseName(): string {
  const name = `jazz-indexeddb-page-store-${crypto.randomUUID()}`;
  databaseNames.push(name);
  return name;
}

function openRawDatabase(name: string): Promise<IDBDatabase> {
  return requestResult(fakeIndexedDb.open(name));
}

async function installRawEpochOneFixture(name: string, manifest: unknown): Promise<void> {
  const raw = await createRawEpochDatabase(name);
  const tx = raw.transaction(
    [INDEXEDDB_BTREE_PAGES_STORE, INDEXEDDB_BTREE_METADATA_STORE, INDEXEDDB_STORAGE_MANIFEST_STORE],
    "readwrite",
  );
  tx.objectStore(INDEXEDDB_BTREE_PAGES_STORE).put(new Uint8Array([1]).buffer, 1);
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
  raw.close();
}

function createRawEpochDatabase(name: string): Promise<IDBDatabase> {
  const request = fakeIndexedDb.open(name, INDEXEDDB_BTREE_DATABASE_VERSION);
  request.onupgradeneeded = () => {
    const db = request.result;
    db.createObjectStore(INDEXEDDB_BTREE_PAGES_STORE);
    db.createObjectStore(INDEXEDDB_BTREE_METADATA_STORE);
    db.createObjectStore(INDEXEDDB_STORAGE_MANIFEST_STORE);
  };
  return requestResult(request);
}

function createLegacyVersionTwoDatabase(name: string): Promise<IDBDatabase> {
  const request = fakeIndexedDb.open(name, 2);
  request.onupgradeneeded = () => {
    const db = request.result;
    db.createObjectStore(INDEXEDDB_BTREE_PAGES_STORE);
    db.createObjectStore(INDEXEDDB_BTREE_METADATA_STORE);
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
  if (trimmed.length % 2 !== 0) throw new Error("hex fixture must have even length");
  return Uint8Array.from(
    Array.from({ length: trimmed.length / 2 }, (_, index) =>
      Number.parseInt(trimmed.slice(index * 2, index * 2 + 2), 16),
    ),
  );
}
