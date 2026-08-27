import { indexedDB as fakeIndexedDb } from "fake-indexeddb";
import { readFile } from "node:fs/promises";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  INDEXEDDB_BTREE_DATABASE_VERSION,
  INDEXEDDB_BTREE_FORMAT_MAGIC,
  INDEXEDDB_BTREE_FORMAT_VERSION,
  INDEXEDDB_BTREE_METADATA_STORE,
  INDEXEDDB_BTREE_PAGES_STORE,
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
      metadata: { pageSize: 16 * 1024, rootPageId: 7, nextPageId: 8 },
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
      metadata: { pageSize: 4096, rootPageId: 1, nextPageId: 3 },
      pages: new Map([
        [1, new Uint8Array([1])],
        [2, new Uint8Array([2])],
      ]),
    });
    await store.commit({
      expectedGeneration: 1,
      metadata: { pageSize: 4096, rootPageId: 1, nextPageId: 3 },
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
      metadata: { pageSize: 4096, rootPageId: 1, nextPageId: 2 },
      pages: new Map([[1, new Uint8Array([1])]]),
    });

    await expect(
      store.commit({
        expectedGeneration: 0,
        metadata: { pageSize: 4096, rootPageId: 2, nextPageId: 3 },
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
        metadata: { pageSize: 4096, rootPageId: 2, nextPageId: 3 },
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
      metadata: { pageSize: 4096, rootPageId: 1, nextPageId: 2 },
      pages: new Map([[1, new Uint8Array([1])]]),
    });
    await expect(
      store.commit({
        expectedGeneration: 1,
        metadata: { pageSize: 4096, rootPageId: 1, nextPageId: 2 },
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
      metadata: { pageSize: 4096, rootPageId: 1, nextPageId: 2 },
      pages: new Map([[1, new Uint8Array([1])]]),
    });
    const raw = await openRawDatabase(name);
    expect(raw.version).toBe(INDEXEDDB_BTREE_DATABASE_VERSION);
    expect([...raw.objectStoreNames]).toEqual([
      INDEXEDDB_BTREE_METADATA_STORE,
      INDEXEDDB_BTREE_PAGES_STORE,
    ]);
    const tx = raw.transaction(INDEXEDDB_BTREE_METADATA_STORE, "readonly");
    const value = await requestResult(
      tx.objectStore(INDEXEDDB_BTREE_METADATA_STORE).get("current"),
    );
    expect(value).toEqual({
      formatMagic: INDEXEDDB_BTREE_FORMAT_MAGIC,
      formatVersion: INDEXEDDB_BTREE_FORMAT_VERSION,
      pageSize: 4096,
      generation: 1,
      rootPageId: 1,
      nextPageId: 2,
    });
    raw.close();
    store.close();
  });

  it("accepts the typed-array wasm bridge without re-encoding page bodies", async () => {
    const name = databaseName();
    const store = await IndexedDbPageStore.open(name);
    const metadata = await store.commitPages(
      0,
      4096,
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

  it("persists the Rust-owned v2 page fixture byte-for-byte", async () => {
    const fixture = hexBytes(
      await readFile(
        new URL("../../../../crates/idb-tree/fixtures/page-v2-leaf.hex", import.meta.url),
        "utf8",
      ),
    );
    const name = databaseName();
    const store = await IndexedDbPageStore.open(name);
    await store.commit({
      expectedGeneration: 0,
      metadata: { pageSize: 4096, rootPageId: 1, nextPageId: 2 },
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
      metadata: { pageSize: 4096, rootPageId: 1, nextPageId: 2 },
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
        metadata: { pageSize: 4096, rootPageId: 1, nextPageId: 2 },
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

function requestResult<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error("IndexedDB request failed"));
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
