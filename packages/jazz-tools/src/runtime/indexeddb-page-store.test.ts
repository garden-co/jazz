import { indexedDB as fakeIndexedDb } from "fake-indexeddb";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { IndexedDbPageStore } from "./indexeddb-page-store.js";

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
