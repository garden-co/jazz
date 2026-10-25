import * as idb from "fake-indexeddb";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { encodeCellsForRow, encodeSchema } from "./native-runtime/native-runtime-adapter.js";
import { openConfig } from "./native-runtime/native-codec.js";
import { testAuthorBytes } from "./testing/account-fixtures.js";
import { hasJazzWasmBuild, loadWasmModuleForTest } from "./testing/wasm-runtime-test-utils.js";

const app = s.defineApp({ notes: s.table({ text: s.string() }, {}) });

beforeAll(() => {
  for (const [name, value] of Object.entries(idb)) {
    if (name.startsWith("IDB")) vi.stubGlobal(name, value);
  }
  vi.stubGlobal("indexedDB", new idb.IDBFactory());
});
afterAll(() => vi.unstubAllGlobals());

describe.skipIf(!hasJazzWasmBuild())("SQLite browser durability", () => {
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
      expect(seed.writeState()).toMatchObject({ durability: "Local" });

      const originalPut = idb.IDBObjectStore.prototype.put;
      let failedPuts = 0;
      let failedTransaction: IDBTransaction | undefined;
      const originalCommit = idb.IDBTransaction.prototype.commit;
      const commit = vi
        .spyOn(idb.IDBTransaction.prototype, "commit")
        .mockImplementation(function (this: IDBTransaction) {
          if (this === failedTransaction && failure === "commit") this.abort();
          else originalCommit.call(this);
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
      const write = insert("must not be acknowledged");
      try {
        const result = await write.wait("local").then(
          () => ({ ok: true }),
          (error: unknown) => ({ ok: false, error }),
        );
        expect(failedPuts).toBe(1);
        expect(result).toMatchObject({ ok: false });
        expect(write.writeState()).toMatchObject({ durability: "None" });
        put.mockRestore();
        commit.mockRestore();
        // A successful later barrier cannot repair the discarded dirty blocks.
        await expect(db.flush()).rejects.toBeDefined();
        await expect(write.wait("local")).rejects.toBeDefined();
        expect(write.writeState()).toMatchObject({ durability: "None" });
        expect(seed.writeState()).toMatchObject({ durability: "Local" });

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
          expect(healthyWrite.writeState()).toMatchObject({ durability: "Local" });
          healthyWrite.close();
        } finally {
          await healthy.close();
          await wasm.WasmDb.destroyBrowserStorage(healthyStorage, "bob-durability");
        }

        await expect(db.close()).rejects.toBeDefined();
        await expect(
          wasm.WasmDb.openBrowser(
            { ...storage, requireExisting: true },
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
          expect(freshWrite.writeState()).toMatchObject({ durability: "Local" });
          freshWrite.close();
        } finally {
          await fresh.close();
        }
      } finally {
        put.mockRestore();
        commit.mockRestore();
        write.close();
        seed.close();
        await db.close().catch(() => undefined);
        await wasm.WasmDb.destroyBrowserStorage(storage, "alice-durability");
      }
    },
  );
});
