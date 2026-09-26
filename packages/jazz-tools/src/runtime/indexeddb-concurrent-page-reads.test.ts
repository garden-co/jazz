import { IDBFactory, IDBObjectStore } from "fake-indexeddb";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { acquireBrowserPhysicalDatabaseEpoch } from "./browser-physical-database-epoch.js";
import {
  INDEXEDDB_BTREE_PAGES_STORE,
  INDEXEDDB_BTREE_PAGE_SIZE,
  IndexedDbPageStore,
} from "./indexeddb-page-store.js";

const opened: { store: IndexedDbPageStore; release?: () => Promise<void> }[] = [];

beforeEach(() => vi.stubGlobal("indexedDB", new IDBFactory()));
afterEach(async () => {
  for (const { store, release } of opened.splice(0)) {
    store.close();
    await release?.();
    await IndexedDbPageStore.destroy(store.name);
  }
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

async function fixture(owned = true) {
  const store = await IndexedDbPageStore.open(`concurrent-pages-${crypto.randomUUID()}`);
  const cleanup: (typeof opened)[number] = { store };
  opened.push(cleanup);
  let token: number | undefined;
  if (owned) {
    const epoch = await acquireBrowserPhysicalDatabaseEpoch(store.name, {
      async request(_name, _options, callback) {
        return await callback({});
      },
    });
    cleanup.release = () => epoch.release();
    await store.claimBrowserWorkerEpoch(epoch.id, epoch);
    token = store.claimTreeOwnership();
  }
  await store.commit({
    expectedGeneration: 0,
    metadata: { pageSize: INDEXEDDB_BTREE_PAGE_SIZE, rootPageId: 1, nextPageId: 4 },
    pages: new Map([1, 2, 3].map((id) => [id, new Uint8Array([id])])),
  });
  return { store, token };
}

function countPageGets(abortFirst = false) {
  const ids: number[] = [];
  const get = IDBObjectStore.prototype.get;
  vi.spyOn(IDBObjectStore.prototype, "get").mockImplementation(
    function (this: IDBObjectStore, key) {
      const request = get.call(this, key);
      if (this.name === INDEXEDDB_BTREE_PAGES_STORE && this.transaction.mode === "readonly") {
        ids.push(key as number);
        if (abortFirst && ids.length === 1) queueMicrotask(() => this.transaction.abort());
      }
      return request;
    },
  );
  return ids;
}

describe("concurrent IndexedDB page reads", () => {
  it("shares pending reads across batches and point reads with independent result bytes", async () => {
    const { store } = await fixture();
    const ids = countPageGets();
    const [first, second, third] = await Promise.all([
      store.readPages([3, 1, 3, 9]),
      store.readPages([1, 2, 9]),
      store.readPage(3),
    ]);
    expect(first).toEqual([new Uint8Array([3]), new Uint8Array([1]), new Uint8Array([3]), null]);
    expect(second).toEqual([new Uint8Array([1]), new Uint8Array([2]), null]);
    expect(third).toEqual(new Uint8Array([3]));
    expect([...ids].sort()).toEqual([1, 2, 3, 9]);
    first[0]![0] = 99;
    second[0]![0] = 88;
    expect(first[2]).toEqual(new Uint8Array([3]));
    expect(third).toEqual(new Uint8Array([3]));
    expect(first[1]).toEqual(new Uint8Array([1]));
    // Sharing ends with the transaction; this is not a second page cache.
    expect(await store.readPage(3)).toEqual(new Uint8Array([3]));
    expect(ids.filter((id) => id === 3)).toHaveLength(2);
  });

  it("preserves write ordering without requiring the earlier reader to be awaited", async () => {
    const { store } = await fixture();
    const before = store.readPage(1);
    const write = store.commit({
      expectedGeneration: 1,
      metadata: { pageSize: INDEXEDDB_BTREE_PAGE_SIZE, rootPageId: 1, nextPageId: 4 },
      pages: new Map([[1, new Uint8Array([7])]]),
    });
    const after = store.readPage(1);
    await write;
    expect(await after).toEqual(new Uint8Array([7]));
    expect(await before).toEqual(new Uint8Array([1]));
  });

  it("does not reuse a pending read across clear", async () => {
    const { store } = await fixture();
    const before = store.readPage(1);
    const cleared = store.clear();
    const after = store.readPage(1);
    await cleared;
    expect(await after).toBeNull();
    expect(await before).toEqual(new Uint8Array([1]));
  });

  it("rejects all waiters after an aborted transaction and retries with fresh I/O", async () => {
    const { store } = await fixture();
    const ids = countPageGets(true);
    const results = await Promise.allSettled([store.readPage(1), store.readPage(1)]);
    expect(results.map((result) => result.status)).toEqual(["rejected", "rejected"]);
    expect(ids).toEqual([1]);
    expect(await store.readPage(1)).toEqual(new Uint8Array([1]));
    expect(ids).toEqual([1, 1]);
  });

  it("keeps independent transactions for stores without exclusive tree ownership", async () => {
    const { store } = await fixture(false);
    const ids = countPageGets();
    const values = await Promise.all([store.readPage(1), store.readPage(1)]);
    expect(values).toEqual([new Uint8Array([1]), new Uint8Array([1])]);
    expect(ids).toEqual([1, 1]);
  });

  it("does not share a predecessor tree's pending read after ownership handoff", async () => {
    const { store, token } = await fixture();
    const ids = countPageGets();
    const before = store.readPage(1);
    store.releaseTreeOwnership(token!);
    store.claimTreeOwnership();
    const after = store.readPage(1);
    expect(await after).toEqual(new Uint8Array([1]));
    expect(await before).toEqual(new Uint8Array([1]));
    expect(ids).toEqual([1, 1]);
  });
});
