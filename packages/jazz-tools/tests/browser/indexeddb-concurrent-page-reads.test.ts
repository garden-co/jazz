import { afterEach, expect, it, vi } from "vitest";
import { acquireBrowserPhysicalDatabaseEpoch } from "../../src/runtime/browser-physical-database-epoch.js";
import {
  INDEXEDDB_BTREE_PAGES_STORE,
  INDEXEDDB_BTREE_PAGE_SIZE,
  IndexedDbPageStore,
} from "../../src/runtime/indexeddb-page-store.js";

// Real IndexedDB + Web Locks: requests finish in the browser even when the
// first caller never awaits its promise. No Jazz/WASM artifact is used here.
const cleanups: (() => Promise<void>)[] = [];
afterEach(async () => {
  vi.restoreAllMocks();
  for (const cleanup of cleanups.splice(0)) await cleanup();
});

async function fixture() {
  const name = `concurrent-page-reads-${crypto.randomUUID()}`;
  const store = await IndexedDbPageStore.open(name);
  const epoch = await acquireBrowserPhysicalDatabaseEpoch(name);
  cleanups.push(async () => {
    store.close();
    await epoch.release();
    await IndexedDbPageStore.destroy(name);
  });
  await store.claimBrowserWorkerEpoch(epoch.id, epoch);
  store.claimTreeOwnership();
  await store.commit({
    expectedGeneration: 0,
    metadata: { pageSize: INDEXEDDB_BTREE_PAGE_SIZE, rootPageId: 1, nextPageId: 65 },
    pages: new Map(Array.from({ length: 64 }, (_, i) => [i + 1, new Uint8Array([i + 1])])),
  });
  return store;
}

it("overlapping cold page batches issue one browser get per page", async () => {
  const store = await fixture();
  const gets: number[] = [];
  const original = IDBObjectStore.prototype.get;
  vi.spyOn(IDBObjectStore.prototype, "get").mockImplementation(
    function (this: IDBObjectStore, key) {
      if (this.name === INDEXEDDB_BTREE_PAGES_STORE && this.transaction.mode === "readonly") {
        gets.push(key as number);
      }
      return original.call(this, key);
    },
  );
  const ids = Array.from({ length: 64 }, (_, i) => i + 1);
  const first = store.readPages(ids);
  const second = store.readPages([...ids].reverse());
  const third = store.readPages([1, 1, 65]);
  const [a, b, c] = await Promise.all([first, second, third]);
  expect(a).toEqual(ids.map((id) => new Uint8Array([id])));
  expect(b).toEqual([...ids].reverse().map((id) => new Uint8Array([id])));
  expect(c).toEqual([new Uint8Array([1]), new Uint8Array([1]), null]);
  expect(gets).toHaveLength(65);
  expect(new Set(gets).size).toBe(65);
  a[0]![0] = 99;
  c[0]![0] = 88;
  expect(b[63]).toEqual(new Uint8Array([1]));
  expect(c[1]).toEqual(new Uint8Array([1]));
});

it("a write finishes before the first caller awaits and later reads see its new bytes", async () => {
  const store = await fixture();
  const oldRead = store.readPage(1);
  const write = store.commit({
    expectedGeneration: 1,
    metadata: { pageSize: INDEXEDDB_BTREE_PAGE_SIZE, rootPageId: 1, nextPageId: 65 },
    pages: new Map([[1, new Uint8Array([7])]]),
  });
  const newRead = store.readPage(1);
  await write;
  expect(await newRead).toEqual(new Uint8Array([7]));
  expect(await oldRead).toEqual(new Uint8Array([1]));
  const beforeReset = store.readPage(1);
  const reset = store.clear();
  const afterReset = store.readPage(1);
  await reset;
  expect(await afterReset).toBeNull();
  expect(await beforeReset).toEqual(new Uint8Array([7]));
});

it("an aborted shared browser transaction rejects every waiter and can be retried", async () => {
  const store = await fixture();
  const original = IDBObjectStore.prototype.get;
  let gets = 0;
  const hook = vi
    .spyOn(IDBObjectStore.prototype, "get")
    .mockImplementation(function (this: IDBObjectStore, key) {
      const request = original.call(this, key);
      if (this.name === INDEXEDDB_BTREE_PAGES_STORE && this.transaction.mode === "readonly") {
        if (++gets === 1) queueMicrotask(() => this.transaction.abort());
      }
      return request;
    });
  const values = await Promise.allSettled([store.readPage(1), store.readPage(1)]);
  expect(values.map((value) => value.status)).toEqual(["rejected", "rejected"]);
  expect(gets).toBe(1);
  hook.mockRestore();
  expect(await store.readPage(1)).toEqual(new Uint8Array([1]));
});
