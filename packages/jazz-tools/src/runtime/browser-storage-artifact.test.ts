import { createHash } from "node:crypto";
import * as idb from "fake-indexeddb";
import { afterEach, beforeEach, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import {
  IndexedDbPageStore,
  INDEXEDDB_STORAGE_MANIFEST_STORE,
  INDEXEDDB_STORAGE_MANIFEST_KEY,
} from "./indexeddb-page-store.js";
import { SQLITE_BROWSER_ROOT_KEY } from "./browser-sqlite-storage.js";
import {
  encodeSchema,
  encodeCellsForRow,
  NativeRuntimeAdapter,
} from "./native-runtime/native-runtime-adapter.js";
import { openConfig } from "./native-runtime/native-codec.js";
import { loadWasmModuleForTest } from "./testing/wasm-runtime-test-utils.js";
import { testAuthorBytes } from "./testing/account-fixtures.js";

const stores: IndexedDbPageStore[] = [];
beforeEach(() => {
  for (const [name, value] of Object.entries(idb))
    if (name.startsWith("IDB")) vi.stubGlobal(name, value);
  vi.stubGlobal("indexedDB", new idb.IDBFactory());
  const held = new Set<string>();
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
});
afterEach(async () => {
  await Promise.all(stores.splice(0).map((store) => store.close().catch(() => {})));
  vi.unstubAllGlobals();
});
function request<T>(value: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    value.onsuccess = () => resolve(value.result);
    value.onerror = () => reject(value.error);
  });
}

function committed(transaction: IDBTransaction): Promise<void> {
  return new Promise((resolve, reject) => {
    transaction.oncomplete = () => resolve();
    transaction.onabort = transaction.onerror = () => reject(transaction.error);
  });
}

it("reopens Alice's real SQLite file through the upstream metadata and filename layout", async () => {
  const name = "layout-real-wasm-alice";
  const app = s.defineApp({ notes: s.table({ title: s.string() }, {}) });
  const author = testAuthorBytes("layout-real-wasm:alice");
  const wasm = await loadWasmModuleForTest();
  const expectedPath = `/jazz-${createHash("sha256").update(name).digest("hex").slice(0, 32)}.sqlite`;
  const open = async () => {
    const store = await IndexedDbPageStore.open(name, { owner: "alice", format: "sqlite" });
    stores.push(store);
    const node = store.replicaNode;
    let descriptor: { path: string; requireExisting: boolean } | undefined;
    const db = await store.openSqliteRuntime((storage) => {
      descriptor = storage;
      return wasm.WasmDb.openBrowser(
        storage,
        encodeSchema(app.wasmSchema),
        openConfig(node, author, 1, true),
        name,
      );
    });
    return { store, node, db, descriptor: descriptor! };
  };
  const first = await open();
  expect(first.descriptor).toMatchObject({ path: expectedPath, requireExisting: false });
  const write = first.db.insert(
    "notes",
    encodeCellsForRow(app.wasmSchema.notes!, { title: { type: "Text", value: "survives reopen" } }),
  );
  await write.wait("local");
  write.close();
  const raw = await request(indexedDB.open(name));
  expect(raw.version).toBe(3);
  expect(Array.from(raw.objectStoreNames)).toEqual(["metadata", "pages", "storage-manifest"]);
  const plane = raw
    .transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readonly")
    .objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE);
  const [manifest, owner] = await Promise.all([
    request(plane.get(INDEXEDDB_STORAGE_MANIFEST_KEY)),
    request(plane.get(SQLITE_BROWSER_ROOT_KEY)),
  ]);
  expect(manifest).toMatchObject({ storageEpoch: 1, requiredCodecIds: ["jazz.sqlite-storage.v2"] });
  expect(JSON.parse(new TextDecoder().decode(owner))).toMatchObject([
    "jazz.browser.sqlite-root.v2",
    name,
    "alice",
    expect.any(String),
    "ready",
  ]);
  raw.close();
  await first.store.close();
  const reopened = await open();
  expect(reopened.node).toEqual(first.node);
  expect(reopened.descriptor).toMatchObject({ path: expectedPath, requireExisting: true });
  const runtime = NativeRuntimeAdapter.fromDb(
    reopened.db,
    app.wasmSchema,
    reopened.node,
    author,
    1,
    true,
  );
  await expect(runtime.query(JSON.stringify({ table: "notes" }))).resolves.toEqual([
    {
      table: "notes",
      id: expect.any(String),
      values: [{ type: "Text", value: "survives reopen" }],
    },
  ]);
  await runtime.close();
  await reopened.store.close();
  // A different owner can replace or lose the file while this realm is idle.
  // Touch actual IndexedDB blocks, so reopening cannot pass from a VFS cache.
  const vfs = await request(indexedDB.open("jazz-idb", 1));
  const header = await request(
    vfs.transaction("blocks").objectStore("blocks").get([expectedPath, 0]),
  );
  const damaged = { ...header, data: new Uint8Array(header.data) };
  damaged.data.fill(0x55, 0, 16);
  const overwrite = vfs.transaction("blocks", "readwrite");
  overwrite.objectStore("blocks").put(damaged);
  await committed(overwrite);
  await expect(open().then(() => undefined)).rejects.toThrow("file is not a database");
  await stores.at(-1)!.close();
  expect(
    await request(vfs.transaction("blocks").objectStore("blocks").get([expectedPath, 0])),
  ).toEqual(damaged);
  // Simulate loss of the admitted file while retaining its ready owner record.
  const remove = vfs.transaction("blocks", "readwrite");
  remove
    .objectStore("blocks")
    .delete(IDBKeyRange.bound([expectedPath, 0], [expectedPath, Infinity]));
  await committed(remove);
  vfs.close();
  const before = await request(indexedDB.open(name));
  const persistedOwner = await request(
    before
      .transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readonly")
      .objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE)
      .get(SQLITE_BROWSER_ROOT_KEY),
  );
  before.close();
  await expect(open().then(() => undefined)).rejects.toThrow(
    "Admitted SQLite browser file is missing",
  );
  await stores.at(-1)!.close();
  const after = await request(indexedDB.open(name));
  expect(
    await request(
      after
        .transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readonly")
        .objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE)
        .get(SQLITE_BROWSER_ROOT_KEY),
    ),
  ).toEqual(persistedOwner);
  after.close();
  await IndexedDbPageStore.destroy(name);
});
