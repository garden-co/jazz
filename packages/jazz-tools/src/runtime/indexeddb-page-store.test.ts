import { acquireBrowserPhysicalDatabaseEpoch } from "./browser-physical-database-epoch.js";
import { IDBFactory, IDBObjectStore, indexedDB as fakeIndexedDb } from "fake-indexeddb";
import { readFile } from "node:fs/promises";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  INDEXEDDB_BTREE_DATABASE_VERSION,
  INDEXEDDB_BROWSER_RUNTIME_OWNER_KEY,
  INDEXEDDB_BROWSER_WORKER_EPOCH_KEY,
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
} from "./indexeddb-page-store.js";
import type { IndexedDbAdmissionView } from "./indexeddb-page-store.js";
import { loadWasmModule } from "./wasm-loader.js";
import admissionGolden from "../../fixtures/browser-storage-admission-v1.json";

async function ownReclamation(store: IndexedDbPageStore) {
  const epoch = await acquireBrowserPhysicalDatabaseEpoch(store.name, {
    async request(_name, _options, callback) {
      return await callback({});
    },
  });
  await store.claimBrowserWorkerEpoch(epoch.id, epoch);
  const treeToken = store.claimTreeOwnership();
  return Object.assign(epoch, { treeToken });
}

const databaseNames: string[] = [];

describe("IndexedDbPageStore", () => {
  beforeEach(() => {
    const held = new Set<string>();
    vi.stubGlobal("navigator", {
      locks: {
        async request<T>(
          name: string,
          _options: unknown,
          callback: (lock: object | null) => Promise<T>,
        ) {
          await Promise.resolve();
          if (held.has(name)) return await callback(null);
          held.add(name);
          try {
            return await callback({});
          } finally {
            held.delete(name);
          }
        },
      },
    });
    Object.defineProperty(globalThis, "indexedDB", {
      configurable: true,
      value: fakeIndexedDb,
    });
  });

  afterEach(async () => {
    await Promise.all(databaseNames.splice(0).map((name) => IndexedDbPageStore.destroy(name)));
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("publishes fresh admission without scanning and rejects missing or corrupt receipts on reopen", async () => {
    const name = databaseName();
    const first = await IndexedDbPageStore.open(name);
    const admitted = first.storageAdmission();
    expect(admitted[1]?.slice(0, 5)).toEqual(new Uint8Array([74, 83, 65, 49, 0]));
    await first.commit({
      expectedGeneration: 0,
      metadata: { pageSize: INDEXEDDB_BTREE_PAGE_SIZE, rootPageId: 1, nextPageId: 2 },
      pages: new Map([[1, new Uint8Array([7])]]),
    });
    await first.close();
    const reopened = await IndexedDbPageStore.open(name);
    expect(reopened.storageAdmission()).toEqual(admitted);
    expect(await reopened.readPage(1)).toEqual(new Uint8Array([7]));
    await reopened.close();
    for (const receipt of [undefined, new Uint8Array([74, 83, 65, 49, 0])]) {
      const raw = await openRawDatabase(name);
      const tx = raw.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readwrite");
      const plane = tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE);
      if (receipt === undefined) plane.delete(INDEXEDDB_STORAGE_ADMISSION_KEY);
      else plane.put(receipt, INDEXEDDB_STORAGE_ADMISSION_KEY);
      await transactionDone(tx);
      raw.close();
      await expect(IndexedDbPageStore.open(name)).rejects.toThrow("storage admission receipt");
      const verify = await openRawDatabase(name);
      const pages = verify.transaction(INDEXEDDB_BTREE_PAGES_STORE, "readonly");
      expect(await requestResult(pages.objectStore(INDEXEDDB_BTREE_PAGES_STORE).get(1))).toEqual(
        new Uint8Array([7]).buffer,
      );
      await transactionDone(pages);
      verify.close();
    }
  });

  it("keeps legacy admission read-only, revokes the view, and releases the lock on failure", async () => {
    const name = databaseName();
    await installEmptyEpochOneFixture(name);
    let retained: IndexedDbAdmissionView | undefined;
    const wasm = await loadWasmModule();
    const validate = wasm.WasmDb.preflightBrowserEpochOne;
    const preflight = vi
      .spyOn(wasm.WasmDb, "preflightBrowserEpochOne")
      .mockImplementation(async (handle) => {
        retained = handle as IndexedDbAdmissionView;
        expect(await retained.metadata()).toBeNull();
        expect("commitPages" in retained).toBe(false);
        await expect(IndexedDbPageStore.open(name)).rejects.toThrow("active in another");
        await validate(handle);
        throw new Error("interrupted after legacy preflight");
      });
    await expect(IndexedDbPageStore.open(name)).rejects.toThrow(
      "interrupted after legacy preflight",
    );
    expect(() => retained!.metadata()).toThrow("invalidated");
    const raw = await openRawDatabase(name);
    const tx = raw.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readonly");
    expect(
      await requestResult(
        tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE).get(INDEXEDDB_STORAGE_MANIFEST_KEY),
      ),
    ).toEqual(INDEXEDDB_EPOCH_1_STORAGE_MANIFEST);
    expect(
      await requestResult(
        tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE).get(INDEXEDDB_STORAGE_ADMISSION_KEY),
      ),
    ).toBeUndefined();
    await transactionDone(tx);
    raw.close();
    preflight.mockImplementation(async (handle) => {
      retained = handle as IndexedDbAdmissionView;
      await validate(handle);
    });
    const admitted = await IndexedDbPageStore.open(name);
    expect(() => retained!.metadata()).toThrow("admission view has expired");
    expect(admitted.storageAdmission()[1]?.slice(0, 5)).toEqual(
      new Uint8Array([74, 83, 65, 49, 1]),
    );
    await admitted.close();
  });

  it("rolls back the E2 manifest when atomic receipt publication fails and retries from E1", async () => {
    const name = databaseName();
    await installEmptyEpochOneFixture(name);
    const put = IDBObjectStore.prototype.put;
    const failure = vi
      .spyOn(IDBObjectStore.prototype, "put")
      .mockImplementation(function (this: IDBObjectStore, value, key) {
        if (key === INDEXEDDB_STORAGE_ADMISSION_KEY) throw new Error("injected admission failure");
        return put.call(this, value, key);
      });
    await expect(IndexedDbPageStore.open(name)).rejects.toThrow("injected admission failure");
    failure.mockRestore();
    const raw = await openRawDatabase(name);
    const tx = raw.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readonly");
    const plane = tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE);
    expect(await requestResult(plane.get(INDEXEDDB_STORAGE_MANIFEST_KEY))).toEqual(
      INDEXEDDB_EPOCH_1_STORAGE_MANIFEST,
    );
    expect(await requestResult(plane.get(INDEXEDDB_STORAGE_ADMISSION_KEY))).toBeUndefined();
    await transactionDone(tx);
    raw.close();
    const retry = await IndexedDbPageStore.open(name);
    expect(retry.storageAdmission()[1]?.[4]).toBe(1);
    await retry.close();
  });

  it("consumes a separate store claim once without consuming the later reclamation claim", async () => {
    const name = databaseName();
    const epoch = await acquireBrowserPhysicalDatabaseEpoch(name);
    const first = await IndexedDbPageStore.open(name, { epoch });
    await expect(IndexedDbPageStore.open(name, { epoch })).rejects.toThrow("unclaimed live");
    await first.claimBrowserWorkerEpoch(epoch.id, epoch);
    first.claimTreeOwnership();
    expect(first.canReclaimObsoletePages).toBe(true);
    await first.releaseBrowserWorkerEpoch(epoch.id);
    await first.close();
    await expect(IndexedDbPageStore.open(name, { epoch })).rejects.toThrow("unclaimed live");
    await epoch.release();
    const next = await IndexedDbPageStore.open(name);
    await next.close();
  });

  it("pins complete browser JSA1 bytes against Rust and rejects changed source, target, tag and trailing bytes", async () => {
    const wasm = await loadWasmModule();
    for (const completed of [false, true]) {
      const name = databaseName();
      if (completed) await installEmptyEpochOneFixture(name);
      const store = await IndexedDbPageStore.open(name);
      const expected = hexBytes(completed ? admissionGolden.completed : admissionGolden.fresh);
      expect(store.storageAdmission()[1]).toEqual(expected);
      expect(wasm.WasmDb.__browserStorageAdmissionReceipt(completed)).toEqual(expected);
      await store.close();
      const wrongProfile = expected.slice();
      wrongProfile[14] = 99;
      const wrongTarget = expected.slice();
      wrongTarget[wrongTarget.length - 1] = 1;
      const wrongTag = expected.slice();
      wrongTag[4] = 7;
      for (const invalid of [
        wrongProfile,
        wrongTarget,
        wrongTag,
        new Uint8Array([...expected, 0]),
      ]) {
        const raw = await openRawDatabase(name);
        const tx = raw.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readwrite");
        tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE).put(
          invalid,
          INDEXEDDB_STORAGE_ADMISSION_KEY,
        );
        await transactionDone(tx);
        raw.close();
        await expect(IndexedDbPageStore.open(name)).rejects.toThrow("storage admission receipt");
        const verify = await openRawDatabase(name);
        const read = verify.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readonly");
        expect(
          await requestResult(
            read.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE).get(INDEXEDDB_STORAGE_ADMISSION_KEY),
          ),
        ).toEqual(invalid);
        await transactionDone(read);
        verify.close();
      }
    }
  });

  it("rejects destructive opens while a writer owns the root and retains a blocked deletion lock", async () => {
    const name = databaseName();
    const store = await IndexedDbPageStore.open(name);
    const node = store.replicaNode;
    await expect(IndexedDbPageStore.destroy(name)).rejects.toThrow("active in another");
    expect(store.replicaNode).toEqual(node);
    await store.close();
    const blocker = await openRawDatabase(name);
    let blocked!: () => void;
    const observedBlock = new Promise<void>((resolve) => {
      blocked = resolve;
    });
    // A raw browser handle models a debugger/older external client, not a
    // second Jazz writer. It deliberately keeps its connection on versionchange.
    blocker.onversionchange = () => blocked();
    const deleting = IndexedDbPageStore.destroy(name);
    await observedBlock;
    await expect(IndexedDbPageStore.open(name)).rejects.toThrow("active in another");
    blocker.close();
    await deleting;
    const next = await IndexedDbPageStore.open(name);
    expect(next.replicaNode).not.toEqual(node);
    await next.close();
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
    await store.close();
  });

  it("reads several pages in request order, with null for absent ids", async () => {
    const name = databaseName();
    const store = await IndexedDbPageStore.open(name);
    await store.commit({
      expectedGeneration: 0,
      metadata: { pageSize: INDEXEDDB_BTREE_PAGE_SIZE, rootPageId: 7, nextPageId: 8 },
      pages: new Map([
        [7, new Uint8Array([1, 2, 3])],
        [3, new Uint8Array([4, 5])],
      ]),
    });

    expect(await store.readPages([3, 5, 7, 3])).toEqual([
      new Uint8Array([4, 5]),
      null,
      new Uint8Array([1, 2, 3]),
      new Uint8Array([4, 5]),
    ]);
    expect(await store.readPages([])).toEqual([]);
    await expect(store.readPages([1, -1])).rejects.toThrow();
    store.close();
  });

  it("pins an explicit browser owner across release/reopen and rejects another owner before mutation", async () => {
    const name = databaseName();
    const alice = await IndexedDbPageStore.open(name, { owner: "app:alice" });
    await alice.commit({
      expectedGeneration: 0,
      metadata: { pageSize: INDEXEDDB_BTREE_PAGE_SIZE, rootPageId: 1, nextPageId: 2 },
      pages: new Map([[1, new Uint8Array([7])]]),
    });
    await alice.close();

    // Normal worker release/restart preserves ownership and permits the same
    // logical account to reclaim its physical root.
    const reopened = await IndexedDbPageStore.open(name, { owner: "app:alice" });
    expect(await reopened.readPage(1)).toEqual(new Uint8Array([7]));
    await reopened.close();

    // The rejected claim must not get a page-store handle or modify the
    // existing tree. This is the planted sensitivity oracle: removing the
    // owner comparison makes this assertion fail.
    await expect(IndexedDbPageStore.open(name, { owner: "app:bob" })).rejects.toThrow(
      "already owned by a different Jazz browser session",
    );
    const verify = await IndexedDbPageStore.open(name, { owner: "app:alice" });
    expect(await verify.readPage(1)).toEqual(new Uint8Array([7]));
    expect((await verify.metadata())?.generation).toBe(1);
    await verify.close();

    const raw = await openRawDatabase(name);
    const tx = raw.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readonly");
    expect(
      await requestResult(
        tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE).get(INDEXEDDB_BROWSER_RUNTIME_OWNER_KEY),
      ),
    ).toBe("app:alice");
    await transactionDone(tx);
    raw.close();
  });

  it("fences a stale browser worker epoch from clearing its successor", async () => {
    const name = databaseName();
    const first = await IndexedDbPageStore.open(name, { owner: "app:alice" });
    const firstEpoch = "11111111-1111-4111-8111-111111111111";
    const successorEpoch = "22222222-2222-4222-8222-222222222222";
    await first.claimBrowserWorkerEpoch(firstEpoch);
    await first.claimBrowserWorkerEpoch(successorEpoch);

    // Planted positive: a late clean-up from a dead/replaced realm must not
    // erase the durable fence now owned by the successor.
    await first.releaseBrowserWorkerEpoch(firstEpoch);
    const raw = await openRawDatabase(name);
    const tx = raw.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readonly");
    expect(
      await requestResult(
        tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE).get(INDEXEDDB_BROWSER_WORKER_EPOCH_KEY),
      ),
    ).toEqual({ format: "jazz-browser-worker-epoch-v1", epoch: successorEpoch });
    await transactionDone(tx);
    raw.close();

    await first.releaseBrowserWorkerEpoch(successorEpoch);
    await first.close();
  });

  it("retains an exact long canonical owner marker rather than requiring a lossy digest", async () => {
    const name = databaseName();
    const owner = JSON.stringify({
      version: 1,
      appId: "long-owner-app",
      env: "dev",
      auth: {
        kind: "principal",
        authMode: "external",
        user: JSON.stringify(["https://issuer.example", "principal-".repeat(300)]),
      },
    });
    expect(owner.length).toBeGreaterThan(1024);

    const store = await IndexedDbPageStore.open(name, { owner });
    await store.close();
    await expect(IndexedDbPageStore.open(name, { owner: `${owner}:other` })).rejects.toThrow(
      "already owned by a different Jazz browser session",
    );
  });

  it("transfers an explicit browser database only after explicit destruction", async () => {
    const name = databaseName();
    const first = await IndexedDbPageStore.open(name, { owner: "app:alice" });
    await first.close();
    await expect(IndexedDbPageStore.open(name, { owner: "app:bob" })).rejects.toThrow(
      "already owned by a different Jazz browser session",
    );
    await IndexedDbPageStore.destroy(name);
    const transferred = await IndexedDbPageStore.open(name, { owner: "app:bob" });
    await transferred.close();
  });

  it("persists only supplied dirty pages and can delete retired pages", async () => {
    const name = databaseName();
    let store = await IndexedDbPageStore.open(name);
    const epoch = await ownReclamation(store);
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
    await store.close();

    store = await IndexedDbPageStore.open(name);
    expect(await store.readPage(1)).toEqual(new Uint8Array([9]));
    expect(await store.readPage(2)).toBeNull();
    expect((await store.metadata())?.generation).toBe(2);
    await store.close();
    await epoch.release();
  });

  it("requires live single-tree ownership and fences a queued retirement on release", async () => {
    const store = await IndexedDbPageStore.open(databaseName());
    await store.commit({
      expectedGeneration: 0,
      metadata: { pageSize: INDEXEDDB_BTREE_PAGE_SIZE, rootPageId: 1, nextPageId: 3 },
      pages: new Map([
        [1, new Uint8Array([1])],
        [2, new Uint8Array([2])],
      ]),
    });
    const retirement = {
      expectedGeneration: 1,
      metadata: { pageSize: INDEXEDDB_BTREE_PAGE_SIZE, rootPageId: 1, nextPageId: 3 },
      pages: new Map<number, Uint8Array>(),
      deletedPageIds: [2],
    };
    expect(store.canReclaimObsoletePages).toBe(false);
    await expect(store.commit(retirement)).rejects.toThrow("ownership is not active");
    const epoch = await ownReclamation(store);
    expect(store.canReclaimObsoletePages).toBe(true);
    expect(() => store.claimTreeOwnership()).toThrow("already has a tree");
    await store.retireTreeOwnership();
    expect(store.canReclaimObsoletePages).toBe(false);
    store.claimTreeOwnership();
    const pending = store.commit(retirement);
    const release = epoch.release();
    expect(store.canReclaimObsoletePages).toBe(false);
    await expect(pending).rejects.toThrow("ownership expired before publication");
    await release;
    expect(await store.readPage(2)).toEqual(new Uint8Array([2]));
    expect((await store.metadata())?.generation).toBe(1);
    await store.close();
  });

  it("revokes exact tree generations and drains stale non-deletion commits before a successor", async () => {
    const store = await IndexedDbPageStore.open(databaseName());
    const epoch = await ownReclamation(store);
    const oldToken = epoch.treeToken;
    await store.commitPages(
      0,
      INDEXEDDB_BTREE_PAGE_SIZE,
      1,
      2,
      [1],
      [new Uint8Array([1])],
      [],
      oldToken,
    );
    const pending = store.commitPages(
      1,
      INDEXEDDB_BTREE_PAGE_SIZE,
      2,
      3,
      [2],
      [new Uint8Array([2])],
      [],
      oldToken,
    );
    const rejected = expect(pending).rejects.toThrow("tree ownership expired");
    const retirement = store.retireTreeOwnership();
    expect(store.isTreeOwnershipActive(oldToken)).toBe(false);
    expect(() => store.claimTreeOwnership()).toThrow("pending transaction");
    await retirement;
    await rejected;
    const successor = store.claimTreeOwnership();
    store.releaseTreeOwnership(oldToken); // late destructor must not retire the successor
    expect(store.isTreeOwnershipActive(successor)).toBe(true);
    expect(store.canReclaimObsoletePages).toBe(true);
    await expect(
      store.commitPages(
        1,
        INDEXEDDB_BTREE_PAGE_SIZE,
        3,
        4,
        [3],
        [new Uint8Array([3])],
        [],
        oldToken,
      ),
    ).rejects.toThrow("tree ownership has expired");
    await store.commitPages(
      1,
      INDEXEDDB_BTREE_PAGE_SIZE,
      4,
      5,
      [4],
      [new Uint8Array([4])],
      [1],
      successor,
    );
    expect((await store.metadata())?.rootPageId).toBe(4);
    expect(await store.readPage(2)).toBeNull();
    expect(await store.readPage(3)).toBeNull();
    await store.close();
    await epoch.release();
  });

  it("rejects forged, mismatched and multiply consumed reclamation proofs", async () => {
    const name = databaseName();
    const first = await IndexedDbPageStore.open(name);
    const epoch = await ownReclamation(first);
    await expect(first.claimBrowserWorkerEpoch(epoch.id, epoch)).rejects.toThrow();
    await expect(
      first.claimBrowserWorkerEpoch(epoch.id, {
        id: epoch.id,
        release: async () => undefined,
      }),
    ).rejects.toThrow();
    const other = await IndexedDbPageStore.open(databaseName());
    await expect(other.claimBrowserWorkerEpoch(epoch.id, epoch)).rejects.toThrow();
    const release = first.releaseBrowserWorkerEpoch(epoch.id);
    expect(first.canReclaimObsoletePages).toBe(false);
    await release;
    await first.close();
    await other.close();
    await epoch.release();
  });

  it("does not reactivate a pending ownership claim after close or release", async () => {
    for (const close of [false, true]) {
      const store = await IndexedDbPageStore.open(databaseName());
      const epoch = await acquireBrowserPhysicalDatabaseEpoch(store.name, {
        async request(_name, _options, callback) {
          return await callback({});
        },
      });
      const claim = store.claimBrowserWorkerEpoch(epoch.id, epoch);
      const rejected = expect(claim).rejects.toThrow();
      if (close) {
        await store.close();
      } else {
        await store.releaseBrowserWorkerEpoch(epoch.id);
      }
      await rejected;
      expect(store.canReclaimObsoletePages).toBe(false);
      await store.close();
      await epoch.release();
    }
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
    await store.close();
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
    await store.close();
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
    await store.close();
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
    expect(
      await requestResult(
        manifestTx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE).get(INDEXEDDB_REPLICA_NODE_KEY),
      ),
    ).toEqual(store.replicaNode.buffer);
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
    await store.close();
  });

  it("persists one random node per physical replica, not per logical database name", async () => {
    const logicalName = "same-app-and-author";
    const firstFactory = new IDBFactory();
    const secondFactory = new IDBFactory();

    const firstNode = await withIndexedDbFactory(firstFactory, async () => {
      const store = await IndexedDbPageStore.open(logicalName);
      const node = store.replicaNode;
      expect(node).toHaveLength(INDEXEDDB_REPLICA_NODE_BYTES);
      node.fill(0);
      expect(store.replicaNode).not.toEqual(node);
      await store.close();

      const reopened = await IndexedDbPageStore.open(logicalName);
      try {
        expect(reopened.replicaNode).toEqual(store.replicaNode);
        return reopened.replicaNode;
      } finally {
        await reopened.close();
      }
    });
    const secondNode = await withIndexedDbFactory(secondFactory, async () => {
      const store = await IndexedDbPageStore.open(logicalName);
      try {
        return store.replicaNode;
      } finally {
        await store.close();
      }
    });

    // TxId is exactly (HLC time, node), so equal clocks could alias only if
    // these independently durable physical identities were equal.
    expect(secondNode).not.toEqual(firstNode);
  });

  it("excludes an independent first opener and retains the identity after release", async () => {
    const name = databaseName();
    const first = await IndexedDbPageStore.open(name);
    const firstNode = first.replicaNode;
    await expect(IndexedDbPageStore.open(name)).rejects.toThrow("active in another");
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

  it("leases distinct foreground nodes concurrently and reuses only a clean handoff", async () => {
    const name = databaseName();
    const store = await IndexedDbPageStore.open(name);
    const [first, second] = await Promise.all([
      store.acquireForegroundNodeLease(),
      store.acquireForegroundNodeLease(),
    ]);
    expect(first.node).not.toEqual(second.node);

    await store.returnForegroundNodeLease(first.leaseId, 123456789n);
    const reused = await store.acquireForegroundNodeLease();
    expect(reused.node).toEqual(first.node);
    expect(reused.confirmedTxTime).toBe(123456789n);
    await store.retireForegroundNodeLease(second.leaseId);
    await store.retireForegroundNodeLease(reused.leaseId);
    await store.close();
  });

  it("never lowers a returned foreground floor and rejects values native u64 cannot seed", async () => {
    const name = databaseName();
    const store = await IndexedDbPageStore.open(name);
    const first = await store.acquireForegroundNodeLease();
    await store.returnForegroundNodeLease(first.leaseId, 99n);
    const second = await store.acquireForegroundNodeLease();
    await store.returnForegroundNodeLease(second.leaseId, 1n);
    const continued = await store.acquireForegroundNodeLease();
    expect(continued.confirmedTxTime).toBe(99n);
    await expect(store.returnForegroundNodeLease(continued.leaseId, 1n << 64n)).rejects.toThrow(
      "Invalid IndexedDB foreground node lease handoff",
    );
    await store.retireForegroundNodeLease(continued.leaseId);
    await store.close();
  });

  it("retires an abandoned foreground lease on worker restart", async () => {
    const name = databaseName();
    let store = await IndexedDbPageStore.open(name);
    const abandoned = await store.acquireForegroundNodeLease();
    await store.close();

    store = await IndexedDbPageStore.open(name);
    const replacement = await store.acquireForegroundNodeLease(true);
    expect(replacement.node).not.toEqual(abandoned.node);
    await store.retireForegroundNodeLease(replacement.leaseId);
    await store.close();
  });

  it("rejects a missing or malformed physical replica node before touching pages", async () => {
    for (const replicaNode of [null, new Uint8Array(INDEXEDDB_REPLICA_NODE_BYTES - 1)]) {
      const name = databaseName();
      await installRawEpochOneFixture(name, INDEXEDDB_STORAGE_MANIFEST, replicaNode);
      await expect(IndexedDbPageStore.open(name)).rejects.toThrow(
        "Missing or invalid IndexedDB replica node identity",
      );
      const raw = await openRawDatabase(name);
      const tx = raw.transaction(INDEXEDDB_BTREE_PAGES_STORE, "readonly");
      expect(await requestResult(tx.objectStore(INDEXEDDB_BTREE_PAGES_STORE).get(1))).toEqual(
        new Uint8Array([1]).buffer,
      );
      raw.close();
    }
  });

  it("rejects missing, unknown, and inconsistent manifests before touching pages", async () => {
    for (const manifest of [
      undefined,
      { ...INDEXEDDB_STORAGE_MANIFEST, storageEpoch: 99 },
      { ...INDEXEDDB_STORAGE_MANIFEST, pageChecksum: "none" },
      { ...INDEXEDDB_STORAGE_MANIFEST, adapterFormatVersion: 2 },
      { ...INDEXEDDB_STORAGE_MANIFEST, pageFormatVersion: 2 },
      { ...INDEXEDDB_STORAGE_MANIFEST, requiredCodecIds: ["groove.ordered-kv.v1"] },
      {
        ...INDEXEDDB_STORAGE_MANIFEST,
        requiredCodecIds: [...INDEXEDDB_STORAGE_MANIFEST.requiredCodecIds, "jazz.future.v2"],
      },
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
    await store.close();
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
    await store.close();
  });

  it("invalidates an open store when its IndexedDB database is externally deleted", async () => {
    const name = databaseName();
    const invalidations: Error[] = [];
    const store = await IndexedDbPageStore.open(name, {
      onInvalidated: (error) => invalidations.push(error),
    });
    await store.commit({
      expectedGeneration: 0,
      metadata: { pageSize: INDEXEDDB_BTREE_PAGE_SIZE, rootPageId: 1, nextPageId: 2 },
      pages: new Map([[1, new Uint8Array([1])]]),
    });

    await requestResult(fakeIndexedDb.deleteDatabase(name));

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

async function installEmptyEpochOneFixture(name: string): Promise<void> {
  const raw = await createRawEpochDatabase(name);
  const tx = raw.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readwrite");
  const plane = tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE);
  plane.put(INDEXEDDB_EPOCH_1_STORAGE_MANIFEST, INDEXEDDB_STORAGE_MANIFEST_KEY);
  plane.put(epochOneReplicaNode().buffer, INDEXEDDB_REPLICA_NODE_KEY);
  await transactionDone(tx);
  raw.close();
}

async function installRawEpochOneFixture(
  name: string,
  manifest: unknown,
  replicaNode: Uint8Array | null = epochOneReplicaNode(),
): Promise<void> {
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
  if (replicaNode !== null) {
    tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE).put(
      replicaNode.slice().buffer,
      INDEXEDDB_REPLICA_NODE_KEY,
    );
  }
  await transactionDone(tx);
  raw.close();
}

function epochOneReplicaNode(): Uint8Array {
  return Uint8Array.from({ length: INDEXEDDB_REPLICA_NODE_BYTES }, (_, index) => index + 1);
}

async function withIndexedDbFactory<T>(factory: IDBFactory, run: () => Promise<T>): Promise<T> {
  const previous = Object.getOwnPropertyDescriptor(globalThis, "indexedDB");
  Object.defineProperty(globalThis, "indexedDB", { configurable: true, value: factory });
  try {
    return await run();
  } finally {
    if (previous) Object.defineProperty(globalThis, "indexedDB", previous);
    else Reflect.deleteProperty(globalThis, "indexedDB");
  }
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
