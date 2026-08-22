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
      metadata: { pageSize: 16 * 1024, rootPageId: 7, totalPages: 8 },
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
      metadata: { pageSize: 4096, rootPageId: 1, totalPages: 3 },
      pages: new Map([
        [1, new Uint8Array([1])],
        [2, new Uint8Array([2])],
      ]),
    });
    await store.commit({
      expectedGeneration: 1,
      metadata: { pageSize: 4096, rootPageId: 1, totalPages: 3 },
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
      metadata: { pageSize: 4096, rootPageId: 1, totalPages: 2 },
      pages: new Map([[1, new Uint8Array([1])]]),
    });

    await expect(
      store.commit({
        expectedGeneration: 0,
        metadata: { pageSize: 4096, rootPageId: 2, totalPages: 3 },
        pages: new Map([[2, new Uint8Array([2])]]),
      }),
    ).rejects.toThrow("expected 0, found 1");
    expect(await store.readPage(2)).toBeNull();
    expect((await store.metadata())?.rootPageId).toBe(1);
    store.close();
  });
});

function databaseName(): string {
  const name = `jazz-indexeddb-page-store-${crypto.randomUUID()}`;
  databaseNames.push(name);
  return name;
}
