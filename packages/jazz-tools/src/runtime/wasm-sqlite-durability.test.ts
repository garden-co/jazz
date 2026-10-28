import * as idb from "fake-indexeddb";
import { createHash } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { mergePermissionsIntoWasmSchema } from "../schema-permissions.js";
import {
  encodeCellsForRow,
  encodeSchema,
  NativeRuntimeAdapter,
} from "./native-runtime/native-runtime-adapter.js";
import { BrowserWorkerTransportPump } from "./native-runtime/browser-worker-transport.js";
import { openConfig } from "./native-runtime/native-codec.js";
import { testAuthorBytes } from "./testing/account-fixtures.js";
import { allowAll } from "./testing/allow-all.js";
import { hasJazzWasmBuild, loadWasmModuleForTest } from "./testing/wasm-runtime-test-utils.js";

const app = s.defineApp({ notes: s.table({ text: s.string() }, {}) });

beforeAll(() => {
  for (const [name, value] of Object.entries(idb)) {
    if (name.startsWith("IDB")) vi.stubGlobal(name, value);
  }
  vi.stubGlobal("indexedDB", new idb.IDBFactory());
});
afterAll(() => vi.unstubAllGlobals());

// Read the backing store, not the VFS's in-memory image: a cached query cannot
// establish that an earlier successful Local wait survived the failed commit.
async function persistedBlocks(path: string) {
  const database = await new Promise<IDBDatabase>((resolve, reject) => {
    const request = indexedDB.open("jazz-idb");
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
  try {
    const blocks = await new Promise<Array<{ offset: number; data: Uint8Array }>>(
      (resolve, reject) => {
        const transaction = database.transaction("blocks", "readonly");
        const request = transaction
          .objectStore("blocks")
          .getAll(idb.IDBKeyRange.bound([path, 0], [path, Number.MAX_SAFE_INTEGER]));
        transaction.oncomplete = () => resolve(request.result);
        transaction.onabort = () => reject(transaction.error);
      },
    );
    // Compare every persisted byte without asking the assertion reporter to
    // traverse or print millions of individual array elements on a failure.
    return blocks.map(({ offset, data }) => ({
      offset,
      bytes: data.byteLength,
      sha256: createHash("sha256").update(data).digest("hex"),
    }));
  } finally {
    database.close();
  }
}

describe.skipIf(!hasJazzWasmBuild())("SQLite browser durability", () => {
  it.each([
    { upstream: false, fail: false },
    { upstream: true, fail: false },
    { upstream: false, fail: true },
    { upstream: true, fail: true },
  ])(
    "a tab's Local wait observes worker persistence (upstream=$upstream, fail=$fail)",
    async ({ upstream: connected, fail }) => {
      const wasm = await loadWasmModuleForTest();
      const schema = mergePermissionsIntoWasmSchema(app.wasmSchema, allowAll(app));
      const author = testAuthorBytes(`alice-worker-durability-${connected}-${fail}`);
      const workerNode = new Uint8Array(16).fill(81);
      const tabNode = new Uint8Array(16).fill(82);
      const storage = { backend: "indexeddb", path: `/alice-worker-${connected}-${fail}.sqlite` };
      const workerDb = await wasm.WasmDb.openBrowser(
        storage,
        encodeSchema(schema),
        openConfig(workerNode, author),
        "alice-worker-durability",
      );
      const worker = NativeRuntimeAdapter.fromDb(workerDb, schema, workerNode, author, 1, false);
      const tabDb = wasm.WasmDb.openMemory(encodeSchema(schema), openConfig(tabNode, author));
      const alice = NativeRuntimeAdapter.fromDb(tabDb, schema, tabNode, author, 1, false);
      alice.setNonDurableClient();
      // An attached authority holds final Fate pending; the worker must still
      // acknowledge Local only after persistence. Offline it sends local Fate.
      const authority = connected ? await workerDb.connectUpstream() : undefined;
      const downstream = await worker.acceptPeer();
      const upstream = await alice.connectUpstreamPeer();
      await workerDb.flush();
      const before = await persistedBlocks(storage.path);
      let failedPuts = 0;
      const originalPut = idb.IDBObjectStore.prototype.put;
      const put = vi.spyOn(idb.IDBObjectStore.prototype, "put").mockImplementation(function (
        this: IDBObjectStore,
        value: unknown,
        key?: IDBValidKey,
      ) {
        if (fail && failedPuts === 0 && Reflect.get(value as object, "path") === storage.path) {
          failedPuts += 1;
          this.transaction.abort();
          throw new DOMException("injected worker quota failure", "QuotaExceededError");
        }
        return originalPut.call(this, value, key);
      });
      let workerPump: BrowserWorkerTransportPump;
      let tabPump: BrowserWorkerTransportPump;
      const errors: Error[] = [];
      const onError = (error: unknown) => {
        const failure = error instanceof Error ? error : new Error(String(error));
        errors.push(failure);
        // The browser follower receives this as the worker's terminal `error`
        // event. Local failure is transport state, not an authority rejection.
        alice.reportLocalRelayError(failure);
        workerPump.close();
        tabPump.close();
      };
      workerPump = new BrowserWorkerTransportPump(
        worker,
        downstream,
        (frames) => tabPump.receive(frames),
        onError,
      );
      tabPump = new BrowserWorkerTransportPump(
        alice,
        upstream,
        (frames) => workerPump.receive(frames),
        onError,
      );
      try {
        const inserted = alice.insert("notes", {
          text: { type: "Text", value: "persist before acknowledging" },
        });
        if (inserted.kind !== "committed") throw new Error("expected a committed insert");
        const tx = await inserted.txId;
        let outcome: { ok: boolean; error?: unknown } | undefined;
        void alice.waitForTransaction(tx, "local").then(
          () => {
            outcome = { ok: true };
          },
          (error: unknown) => {
            outcome = { ok: false, error };
          },
        );
        await vi.waitFor(() => expect(outcome).toBeDefined());
        expect(outcome?.ok).toBe(!fail);
        if (fail) {
          expect(failedPuts).toBe(1);
          expect(outcome?.error).toMatchObject({
            message: expect.stringContaining("browser storage"),
          });
          await expect(alice.waitForTransaction(tx, "local")).rejects.toThrow("browser storage");
          await expect(alice.flushLocalSettlements()).rejects.toThrow("browser storage");
          await expect(workerDb.flush()).rejects.toBeDefined();
          expect(await persistedBlocks(storage.path)).toEqual(before);
        } else {
          expect(errors).toEqual([]);
          // Inspect real IndexedDB bytes at the moment Local has resolved.
          expect(await persistedBlocks(storage.path)).not.toEqual(before);
          await alice.flushLocalSettlements();
        }
      } finally {
        put.mockRestore();
        workerPump.close();
        tabPump.close();
        authority?.close();
        alice.reportLocalRelayError(new Error("fixture closed"));
        await alice.close();
        await worker.close().catch(() => undefined);
        await wasm.WasmDb.destroyBrowserStorage(storage, "alice-worker-durability");
      }
    },
  );

  it.each(["put", "commit"] as const)(
    "does not report Local after a background IndexedDB %s fails",
    async (failure) => {
      const wasm = await loadWasmModuleForTest();
      const storage = { backend: "indexeddb", path: `/alice-durability-${failure}.sqlite` };
      const db = await wasm.WasmDb.openBrowser(
        storage,
        encodeSchema(app.wasmSchema),
        openConfig(new Uint8Array(16).fill(73), testAuthorBytes("alice-durability")),
        "alice-durability",
      );
      const insert = (text: string) =>
        db.insert(
          "notes",
          encodeCellsForRow(app.wasmSchema.notes!, { text: { type: "Text", value: text } }),
        );
      const seed = insert("already durable");
      await seed.wait("local");
      await db.flush();
      const durableSeed = await persistedBlocks(storage.path);
      expect(durableSeed.length).toBeGreaterThan(0);

      const originalPut = idb.IDBObjectStore.prototype.put;
      let failedPuts = 0;
      let failedCommits = 0;
      let failedTransaction: IDBTransaction | undefined;
      const originalCommit = idb.IDBTransaction.prototype.commit;
      const commit = vi
        .spyOn(idb.IDBTransaction.prototype, "commit")
        .mockImplementation(function (this: IDBTransaction) {
          if (this === failedTransaction && failure === "commit") {
            failedCommits += 1;
            // The final request aborts before automatic commit below.
          } else originalCommit.call(this);
        });
      const put = vi.spyOn(idb.IDBObjectStore.prototype, "put").mockImplementation(function (
        this: IDBObjectStore,
        value: unknown,
        key?: IDBValidKey,
      ) {
        if (
          failedPuts === 0 &&
          this.name === "blocks" &&
          Reflect.get(value as object, "path") === storage.path
        ) {
          failedPuts += 1;
          failedTransaction = this.transaction;
          if (failure === "put") {
            this.transaction.abort();
            throw new DOMException("injected IndexedDB quota failure", "QuotaExceededError");
          }
        }
        return originalPut.call(this, value, key);
      });
      const originalDelete = idb.IDBObjectStore.prototype.delete;
      const truncate = vi
        .spyOn(idb.IDBObjectStore.prototype, "delete")
        .mockImplementation(function (this: IDBObjectStore, key: IDBValidKey | IDBKeyRange) {
          const request = originalDelete.call(this, key);
          if (this.transaction === failedTransaction && failure === "commit") {
            // Abort after the last queued request, before automatic commit. The
            // installed fake-indexeddb v5 mishandles {once, capture} listeners
            // on errors from outstanding requests; aborting those re-enters a
            // Rust FnOnce callback and interrupts the fixture's own rollback.
            const transaction = this.transaction;
            request.addEventListener("success", () => transaction.abort());
          }
          return request;
        });
      const write = insert("must not be acknowledged");
      try {
        const result = await write.wait("local").then(
          () => ({ ok: true }),
          (error: unknown) => ({ ok: false, error }),
        );
        // Drain the background queue even on the unfixed implementation,
        // whose Local wait resolves without waiting for IndexedDB.
        const barrier = await db.flush().then(
          () => ({ ok: true }),
          (error: unknown) => ({ ok: false, error }),
        );
        expect(failedPuts).toBe(1);
        expect(failedCommits).toBe(failure === "commit" ? 1 : 0);
        expect(result).toMatchObject({ ok: false });
        expect(barrier).toMatchObject({ ok: false });
        put.mockRestore();
        commit.mockRestore();
        truncate.mockRestore();
        // A successful later barrier cannot repair the discarded dirty blocks.
        await expect(db.flush()).rejects.toBeDefined();
        await expect(write.wait("local")).rejects.toBeDefined();
        expect(await persistedBlocks(storage.path)).toEqual(durableSeed);

        // The latch belongs to Alice's failed file, not every file in the VFS.
        const healthyStorage = { backend: "indexeddb", path: `/bob-durability-${failure}.sqlite` };
        const healthy = await wasm.WasmDb.openBrowser(
          healthyStorage,
          encodeSchema(app.wasmSchema),
          openConfig(new Uint8Array(16).fill(74), testAuthorBytes("bob-durability")),
          "bob-durability",
        );
        try {
          const healthyWrite = healthy.insert(
            "notes",
            encodeCellsForRow(app.wasmSchema.notes!, {
              text: { type: "Text", value: "unrelated durable write" },
            }),
          );
          await healthyWrite.wait("local");
          healthyWrite.close();
        } finally {
          await healthy.close();
          await wasm.WasmDb.destroyBrowserStorage(healthyStorage, "bob-durability");
        }

        await expect(db.close()).rejects.toBeDefined();
        write.close();
        seed.close();
        await expect(
          wasm.WasmDb.openBrowser(
            storage,
            encodeSchema(app.wasmSchema),
            openConfig(new Uint8Array(16).fill(73), testAuthorBytes("alice-durability")),
            "alice-durability",
          ),
        ).rejects.toBeDefined();
        // A failed reset must not clear the file's earlier persistence error.
        let failedDeletes = 0;
        const deletion = vi
          .spyOn(idb.IDBObjectStore.prototype, "delete")
          .mockImplementation(function (this: IDBObjectStore, key: IDBValidKey | IDBKeyRange) {
            if (
              this.name === "blocks" &&
              key instanceof idb.IDBKeyRange &&
              Array.isArray(key.lower) &&
              key.lower[0] === storage.path
            ) {
              failedDeletes += 1;
              this.transaction.abort();
              throw new DOMException("injected IndexedDB delete failure", "UnknownError");
            }
            return originalDelete.call(this, key);
          });
        try {
          await expect(
            wasm.WasmDb.destroyBrowserStorage(storage, "alice-durability"),
          ).rejects.toBeDefined();
          expect(failedDeletes).toBe(1);
        } finally {
          deletion.mockRestore();
        }
        expect(await persistedBlocks(storage.path)).toEqual(durableSeed);
        await expect(
          wasm.WasmDb.openBrowser(
            storage,
            encodeSchema(app.wasmSchema),
            openConfig(new Uint8Array(16).fill(73), testAuthorBytes("alice-durability")),
            "alice-durability",
          ),
        ).rejects.toBeDefined();
        // Only an explicit successful reset may discard the failed cache/latch.
        await wasm.WasmDb.destroyBrowserStorage(storage, "alice-durability");
        const fresh = await wasm.WasmDb.openBrowser(
          storage,
          encodeSchema(app.wasmSchema),
          openConfig(new Uint8Array(16).fill(75), testAuthorBytes("alice-durability")),
          "alice-durability",
        );
        try {
          const freshWrite = fresh.insert(
            "notes",
            encodeCellsForRow(app.wasmSchema.notes!, {
              text: { type: "Text", value: "after explicit reset" },
            }),
          );
          await freshWrite.wait("local");
          freshWrite.close();
        } finally {
          await fresh.close();
        }
      } finally {
        put.mockRestore();
        commit.mockRestore();
        truncate.mockRestore();
        write.close();
        seed.close();
        await db.close().catch(() => undefined);
        await wasm.WasmDb.destroyBrowserStorage(storage, "alice-durability");
      }
    },
  );
});
