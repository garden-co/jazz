import { IDBFactory } from "fake-indexeddb";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import fixture from "../../fixtures/browser-sqlite-root-v2.json";
import {
  acquireBrowserPhysicalDatabaseEpoch,
  type BrowserPhysicalDatabaseEpoch,
} from "./browser-physical-database-epoch.js";
import {
  decodeSqliteBrowserRoot,
  encodeSqliteBrowserRoot,
  sqliteBrowserDescriptor,
  SQLITE_BROWSER_ROOT_KEY,
  type SqliteBrowserRoot,
} from "./browser-sqlite-storage.js";
import {
  IndexedDbPageStore,
  INDEXEDDB_STORAGE_MANIFEST_STORE,
  INDEXEDDB_STORAGE_MANIFEST_KEY,
} from "./indexeddb-page-store.js";

const mocks = vi.hoisted(() => ({ destroy: vi.fn(), load: vi.fn() }));
vi.mock("./wasm-loader.js", () => ({ loadWasmModule: mocks.load }));

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}
const stores: IndexedDbPageStore[] = [];
const epochs: BrowserPhysicalDatabaseEpoch[] = [];
async function open(name = "notes", owner = "alice") {
  const epoch = await acquireBrowserPhysicalDatabaseEpoch(name);
  epochs.push(epoch);
  const store = await IndexedDbPageStore.open(name, { owner, epoch, format: "sqlite" });
  stores.push(store);
  return { store, epoch };
}
function runtime() {
  return {
    flush: vi.fn(async (): Promise<void> => undefined),
    close: vi.fn(async (): Promise<void> => undefined),
  };
}
async function release(handle: Awaited<ReturnType<typeof open>>) {
  await handle.store.close();
  await handle.epoch.release();
}
function request<T>(r: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    r.onsuccess = () => resolve(r.result);
    r.onerror = () => reject(r.error);
  });
}
async function manifest(name = "notes", key = SQLITE_BROWSER_ROOT_KEY) {
  const db = await request(indexedDB.open(name));
  try {
    const tx = db.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readonly");
    return await request(tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE).get(key));
  } finally {
    db.close();
  }
}

beforeEach(() => {
  const held = new Set<string>();
  vi.stubGlobal("indexedDB", new IDBFactory());
  vi.stubGlobal("navigator", {
    locks: {
      async request(
        name: string,
        _options: unknown,
        callback: (lock: object | null) => Promise<unknown>,
      ) {
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
  mocks.destroy.mockReset().mockResolvedValue(undefined);
  mocks.load.mockReset().mockResolvedValue({ WasmDb: { destroyBrowserStorage: mocks.destroy } });
});
afterEach(async () => {
  await Promise.all(stores.splice(0).map((store) => store.close().catch(() => undefined)));
  await Promise.all(epochs.splice(0).map((epoch) => epoch.release()));
  vi.unstubAllGlobals();
});

describe("SQLite browser root", () => {
  it("pins the explicit byte format and rejects substituted scope, node and noncanonical bytes", () => {
    for (const state of ["opening", "ready", "resetting"] as const) {
      const root: SqliteBrowserRoot = {
        name: "notes",
        owner: "alice",
        node: "01".repeat(16),
        state,
      };
      const bytes = encodeSqliteBrowserRoot(root);
      expect(Buffer.from(bytes).toString("hex")).toBe(fixture[state]);
      expect(decodeSqliteBrowserRoot(bytes, "notes", "alice", new Uint8Array(16).fill(1))).toEqual(
        root,
      );
      expect(() =>
        decodeSqliteBrowserRoot(bytes, "other", "alice", new Uint8Array(16).fill(1)),
      ).toThrow("manifest");
      expect(() =>
        decodeSqliteBrowserRoot(bytes, "notes", "bob", new Uint8Array(16).fill(1)),
      ).toThrow("manifest");
      expect(() =>
        decodeSqliteBrowserRoot(bytes, "notes", "alice", new Uint8Array(16).fill(2)),
      ).toThrow("manifest");
      expect(() =>
        decodeSqliteBrowserRoot(
          new Uint8Array([...bytes, 32]),
          "notes",
          "alice",
          new Uint8Array(16).fill(1),
        ),
      ).toThrow("manifest");
    }
  });

  it("keeps two physical roots for Alice separate and reopens each exact file and node", async () => {
    const a = await open("first");
    const b = await open("second");
    const paths: string[] = [];
    for (const h of [a, b])
      await h.store.openSqliteRuntime(async (storage) => {
        expect(storage.requireExisting).toBe(false);
        paths.push(storage.path);
        return runtime();
      });
    expect(paths[0]).not.toBe(paths[1]);
    const node = a.store.replicaNode;
    await release(a);
    const again = await open("first");
    expect(again.store.replicaNode).toEqual(node);
    await again.store.openSqliteRuntime(async (storage) => {
      expect(storage).toEqual({ backend: "indexeddb", path: paths[0], requireExisting: true });
      return runtime();
    });
  });

  it("rejects Bob before loading WASM or modifying Alice's admitted root", async () => {
    const alice = await open();
    await release(alice);
    const before = await manifest();
    await expect(open("notes", "bob")).rejects.toThrow("different Jazz browser session");
    expect(await manifest()).toEqual(before);
    expect(mocks.load).not.toHaveBeenCalled();
  });

  it("refuses an old Groove root without upgrading its manifest", async () => {
    const legacy = await IndexedDbPageStore.open("notes", { owner: "alice" });
    stores.push(legacy);
    await legacy.close();
    const raw = await request(indexedDB.open("notes"));
    const tx = raw.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readwrite");
    const plane = tx.objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE);
    const old = await request(plane.get(INDEXEDDB_STORAGE_MANIFEST_KEY));
    old.requiredCodecIds = ["groove.ordered-kv.v1"];
    await request(plane.put(old, INDEXEDDB_STORAGE_MANIFEST_KEY));
    raw.close();
    const before = await manifest("notes", INDEXEDDB_STORAGE_MANIFEST_KEY);
    await expect(open()).rejects.toThrow("storage epoch manifest");
    expect(await manifest("notes", INDEXEDDB_STORAGE_MANIFEST_KEY)).toEqual(before);
    const db = await request(indexedDB.open("notes"));
    expect(db.version).toBe(3);
    db.close();
    expect(mocks.load).not.toHaveBeenCalled();
  });

  it("does not publish a ready root before the file flush completes", async () => {
    const { store } = await open();
    const flush = deferred<void>();
    const db = runtime();
    db.flush.mockImplementation(() => flush.promise);
    const opening = store.openSqliteRuntime(async () => db);
    await vi.waitFor(() => expect(db.flush).toHaveBeenCalledOnce());
    expect(JSON.parse(new TextDecoder().decode(await manifest())).at(-1)).toBe("opening");
    flush.resolve();
    await opening;
    expect(JSON.parse(new TextDecoder().decode(await manifest())).at(-1)).toBe("ready");
  });

  it("reserves one runtime before awaiting open and closes it before releasing the physical lock", async () => {
    const h = await open();
    const pending = deferred<ReturnType<typeof runtime>>();
    const first = h.store.openSqliteRuntime(() => pending.promise);
    const rejected = expect(first).rejects.toThrow("invalidated");
    const other = vi.fn(async () => runtime());
    await expect(h.store.openSqliteRuntime(other)).rejects.toThrow("already has a runtime owner");
    expect(other).not.toHaveBeenCalled();
    const closing = h.store.close();
    await expect(acquireBrowserPhysicalDatabaseEpoch("notes")).rejects.toThrow("active in another");
    const db = runtime();
    const closed = deferred<void>();
    db.close.mockImplementation(() => closed.promise);
    pending.resolve(db);
    await vi.waitFor(() => expect(db.close).toHaveBeenCalledOnce());
    await expect(acquireBrowserPhysicalDatabaseEpoch("notes")).rejects.toThrow("active in another");
    closed.resolve();
    await closing;
    await rejected;
    await h.epoch.release();
    await open();
  });

  it("keeps failed first flushes unready and closes the failed runtime before retry", async () => {
    const { store } = await open();
    const failed = runtime();
    failed.flush.mockRejectedValue(new Error("flush failed"));
    await expect(store.openSqliteRuntime(async () => failed)).rejects.toThrow("flush failed");
    expect(failed.close).toHaveBeenCalledOnce();
    await store.openSqliteRuntime(async (storage) => {
      expect(storage.requireExisting).toBe(false);
      return runtime();
    });
  });

  it("persists reset intent before deleting the VFS file and rotates identities after an interrupted reset", async () => {
    const h = await open();
    const node = h.store.replicaNode;
    const oldLease = await h.store.acquireForegroundNodeLease();
    await h.store.openSqliteRuntime(async () => runtime());
    mocks.destroy.mockImplementationOnce(async () => {
      expect(JSON.parse(new TextDecoder().decode(await manifest())).at(-1)).toBe("resetting");
      throw new Error("VFS delete interrupted");
    });
    await expect(h.store.destroyUnderOwner(h.epoch)).rejects.toThrow("VFS delete interrupted");
    await h.epoch.release();
    const recovered = await open();
    expect(recovered.store.replicaNode).not.toEqual(node);
    expect(await recovered.store.foregroundNodeLeaseNodeState(oldLease.node)).toBe("missing");
    await recovered.store.openSqliteRuntime(async (storage) => {
      expect(storage.requireExisting).toBe(false);
      expect(storage.path).toBe(mocks.destroy.mock.calls[0]![0].path);
      return runtime();
    });
    expect(mocks.destroy).toHaveBeenCalledTimes(2);
    expect(mocks.destroy.mock.calls[0]).toEqual(mocks.destroy.mock.calls[1]);
  });

  it("preserves a clean lease high-water through SQLite sidecar reopen", async () => {
    const h = await open();
    const lease = await h.store.acquireForegroundNodeLease();
    await h.store.returnForegroundNodeLease(lease.leaseId, 123n);
    await release(h);
    const next = await open();
    const returned = await next.store.acquireForegroundNodeLease();
    expect(returned.node).toEqual(lease.node);
    expect(returned.confirmedTxTime).toBe(123n);
  });

  it("deletes the bound SQLite file before deleting the sidecar", async () => {
    const h = await open();
    let descriptor: ReturnType<typeof sqliteBrowserDescriptor> | undefined;
    const db = runtime();
    await h.store.openSqliteRuntime(async (storage) => {
      descriptor = storage;
      return db;
    });
    mocks.destroy.mockImplementation(async (storage, owner) => {
      expect(db.close).toHaveBeenCalledOnce();
      expect(storage.path).toBe(descriptor!.path);
      expect(owner).toBe("notes");
      expect(JSON.parse(new TextDecoder().decode(await manifest())).at(-1)).toBe("resetting");
    });
    await h.store.destroyUnderOwner(h.epoch);
    await h.epoch.release();
    const next = await open();
    await next.store.openSqliteRuntime(async (storage) => {
      expect(storage.path).toBe(descriptor!.path);
      return runtime();
    });
  });
  it("keeps the runtime reservation until concurrent retirement callers finish closing", async () => {
    const { store } = await open();
    const db = runtime();
    const closed = deferred<void>();
    db.close.mockImplementation(() => closed.promise);
    await store.openSqliteRuntime(async () => db);
    const retiring = store.retireTreeOwnership();
    const alsoRetiring = store.retireTreeOwnership();
    await vi.waitFor(() => expect(db.close).toHaveBeenCalledOnce());
    await expect(store.openSqliteRuntime(async () => runtime())).rejects.toThrow(
      "already has a runtime owner",
    );
    closed.resolve();
    await Promise.all([retiring, alsoRetiring]);
    await store.openSqliteRuntime(async (storage) => {
      expect(storage.requireExisting).toBe(true);
      return runtime();
    });
  });

  it("static reset deletes the admitted SQLite file and preserves reset intent on failure", async () => {
    const h = await open();
    const node = h.store.replicaNode;
    await h.store.openSqliteRuntime(async () => runtime());
    await release(h);
    mocks.destroy.mockRejectedValueOnce(new Error("delete interrupted"));
    await expect(IndexedDbPageStore.destroy("notes")).rejects.toThrow("delete interrupted");
    expect(JSON.parse(new TextDecoder().decode(await manifest())).at(-1)).toBe("resetting");
    await IndexedDbPageStore.destroy("notes");
    expect(mocks.destroy).toHaveBeenCalledTimes(2);
    expect(mocks.destroy.mock.calls[0]).toEqual(mocks.destroy.mock.calls[1]);
    const next = await open();
    expect(next.store.replicaNode).not.toEqual(node);
  });

  it("retains its owned physical lock after a close failure until a retry succeeds", async () => {
    const store = await IndexedDbPageStore.open("notes", { owner: "alice", format: "sqlite" });
    stores.push(store);
    const db = runtime();
    db.close.mockRejectedValueOnce(new Error("final flush failed"));
    await store.openSqliteRuntime(async () => db);
    await expect(store.close()).rejects.toThrow("final flush failed");
    await expect(open().then(() => undefined)).rejects.toThrow("active in another");
    await store.close();
    expect(db.close).toHaveBeenCalledTimes(2);
    await open();
  });

  it("rotates the node when reset deleted the file but sidecar deletion was interrupted", async () => {
    const h = await open();
    const node = h.store.replicaNode;
    await h.store.openSqliteRuntime(async () => runtime());
    const deletion = vi.spyOn(indexedDB, "deleteDatabase").mockImplementationOnce(() => {
      throw new Error("sidecar delete interrupted");
    });
    try {
      await expect(h.store.destroyUnderOwner(h.epoch)).rejects.toThrow(
        "sidecar delete interrupted",
      );
    } finally {
      deletion.mockRestore();
    }
    expect(mocks.destroy).toHaveBeenCalledOnce();
    await h.epoch.release();
    const recovered = await open();
    expect(recovered.store.replicaNode).not.toEqual(node);
    expect(mocks.destroy).toHaveBeenCalledTimes(2);
  });
  it("can reset an absent root repeatedly without retaining the probe connection", async () => {
    await IndexedDbPageStore.destroy("missing");
    await IndexedDbPageStore.destroy("missing");
    const h = await open("missing");
    await h.store.openSqliteRuntime(async () => runtime());
    expect(mocks.destroy).not.toHaveBeenCalled();
  });

  it("closes the reset probe when an external deletion removed the sidecar", async () => {
    const h = await open();
    await h.store.close();
    await request(indexedDB.deleteDatabase("notes"));
    await expect(h.store.destroyUnderOwner(h.epoch)).rejects.toThrow();
    await h.epoch.release();
    await IndexedDbPageStore.destroy("notes");
    expect(mocks.destroy).not.toHaveBeenCalled();
  });
});
