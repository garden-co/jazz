/// <reference types="vite/client" />
import { afterEach, expect, it } from "vitest";
import { schema as s } from "../../src/schema-namespace.js";
import { mergePermissionsIntoWasmSchema } from "../../src/schema-permissions.js";
import { IndexedDbPageStore } from "../../src/runtime/indexeddb-page-store.js";
import {
  acquireBrowserPhysicalDatabaseEpoch,
  type BrowserPhysicalDatabaseEpoch,
} from "../../src/runtime/browser-physical-database-epoch.js";
import { loadWasmModule } from "../../src/runtime/wasm-loader.js";
import {
  openConfig,
  queryFromTable,
  PostcardReader,
  readNativeRowBatch,
} from "../../src/runtime/native-runtime/native-codec.js";
import { encodeSchema } from "../../src/runtime/native-runtime/schema-codec.js";
import { encodeCellsForRow } from "../../src/runtime/native-runtime/native-runtime-adapter.js";
import wasmUrl from "../../../../crates/jazz-wasm/pkg/jazz_wasm_bg.wasm?url";
import type { WasmDb } from "jazz-wasm";

const app = s.defineApp({ notes: s.table({ text: s.string() }, {}) });
const permissions = s.definePermissions(app, ({ policy }) => [
  policy.notes.allowRead.always(),
  policy.notes.allowInsert.always(),
]);
const schema = mergePermissionsIntoWasmSchema(app.wasmSchema, permissions);
const sources = { wasmUrl };
type Handle = {
  store: IndexedDbPageStore;
  epoch: BrowserPhysicalDatabaseEpoch;
  db: WasmDb;
  owner: string;
};
const handles: Handle[] = [];
const names = new Set<string>();
function name() {
  const name = `sqlite-browser-${crypto.randomUUID()}`;
  names.add(name);
  return name;
}
async function open(name: string, owner = "alice") {
  const epoch = await acquireBrowserPhysicalDatabaseEpoch(name);
  let store: IndexedDbPageStore | undefined;
  try {
    store = await IndexedDbPageStore.open(name, {
      owner,
      epoch,
      format: "sqlite",
      runtimeSources: sources,
    });
    await store.claimBrowserWorkerEpoch(epoch.id, epoch);
    const wasm = await loadWasmModule(sources);
    const config = openConfig(
      store.replicaNode,
      new TextEncoder().encode('["https://issuer.example","alice"]'),
      1,
      false,
    );
    const db = await store.openSqliteRuntime((storage) =>
      wasm.WasmDb.openBrowser(storage, encodeSchema(schema), config, owner),
    );
    const handle = { store, epoch, db, owner };
    handles.push(handle);
    return handle;
  } catch (error) {
    await store?.close();
    await epoch.release();
    throw error;
  }
}
async function close(handle: Handle) {
  await handle.store.close();
  await handle.epoch.release();
}
function insert(db: WasmDb, text: string) {
  return db.insert(
    "notes",
    encodeCellsForRow(schema.notes!, { text: { type: "Text", value: text } }),
    undefined,
  );
}
async function count(db: WasmDb) {
  const bytes = await db.all(
    queryFromTable("notes"),
    { tier: "local-only" },
    undefined,
    undefined,
    undefined,
  );
  expect(bytes).toBeInstanceOf(Uint8Array);
  return new PostcardReader(bytes as Uint8Array)
    .readVec(readNativeRowBatch)
    .reduce((sum, batch) => sum + batch.rows.length, 0);
}
afterEach(async () => {
  await Promise.all(handles.splice(0).map(close));
  for (const name of names) await IndexedDbPageStore.destroy(name);
  names.clear();
});

it("uses the admitted physical root for real SQLite persistence, owner isolation and reset", async () => {
  const firstName = name();
  const secondName = name();
  const first = await open(firstName);
  const second = await open(secondName);
  const view = first.db.registerSchema(encodeSchema(schema));
  await insert(view, "hello").wait("local");
  expect(await count(first.db)).toBe(1);
  expect(await count(second.db)).toBe(0);
  const node = first.store.replicaNode;
  await close(first);
  expect(() => insert(view, "late")).toThrow("closed");
  await expect(open(firstName, "bob")).rejects.toThrow("different Jazz browser session");
  const reopened = await open(firstName);
  expect(reopened.store.replicaNode).toEqual(node);
  expect(await count(reopened.db)).toBe(1);
  await reopened.store.destroyUnderOwner(reopened.epoch);
  await reopened.epoch.release();
  const reset = await open(firstName);
  expect(reset.store.replicaNode).not.toEqual(node);
  expect(await count(reset.db)).toBe(0);
  expect(await count(second.db)).toBe(0);
});

it("refuses a ready root whose real SQLite file was deleted instead of recycling its node", async () => {
  const physical = name();
  const first = await open(physical);
  const node = first.store.replicaNode;
  await insert(first.db, "persisted").wait("local");
  await close(first);
  const wasm = await loadWasmModule(sources);
  const hex = Array.from(node, (byte) => byte.toString(16).padStart(2, "0")).join("");
  // Deliberate physical corruption: normal reset also retires the sidecar node.
  await wasm.WasmDb.destroyBrowserStorage(
    { backend: "indexeddb", path: `/jazz-sqlite-v1-${hex}.sqlite` },
    "alice",
  );
  await expect(open(physical)).rejects.toThrow("file is missing");
  await IndexedDbPageStore.destroy(physical);
  const recovered = await open(physical);
  expect(recovered.store.replicaNode).not.toEqual(node);
  expect(await count(recovered.db)).toBe(0);
});

it("admits, reopens and resets the physical root through the production SharedWorker", async () => {
  const physical = name();
  const seeded = await open(physical);
  await insert(seeded.db, "before worker").wait("local");
  const node = seeded.store.replicaNode;
  await close(seeded);
  const workerName = `sqlite-worker-${crypto.randomUUID()}`;
  async function connect() {
    const worker = new SharedWorker(
      new URL("../../src/worker/jazz-broker-worker.ts", import.meta.url),
      { type: "module", name: workerName },
    );
    const messages: Array<{
      type: string;
      id?: number;
      error?: { message: string };
      resetId?: number;
    }> = [];
    worker.port.addEventListener("message", (event) => {
      messages.push(event.data);
      if (event.data.type === "storage-reset")
        worker.port.postMessage({ type: "storage-reset-observed", resetId: event.data.resetId });
    });
    worker.port.start();
    worker.port.postMessage({
      type: "connect-runtime",
      tabId: crypto.randomUUID(),
      fingerprint: "sqlite-storage-test",
      options: {
        schema,
        dbName: physical,
        storageOwner: "alice",
        runtimeSources: sources,
        author: new TextEncoder().encode('["https://issuer.example","alice"]'),
        initialSyncFlushEvery: 1,
        appId: "sqlite-storage-test",
        authSessionKey: "alice",
        authJson: "{}",
        sessionClaims: {},
      },
    });
    await expect
      .poll(() => messages.find((m) => m.type === "runtime-ready" || m.type === "runtime-error"), {
        timeout: 10000,
      })
      .toEqual({ type: "runtime-ready" });
    return {
      async request(type: "close" | "delete-storage") {
        const id = 1;
        worker.port.postMessage({ type, id, releaseContext: true });
        await expect
          .poll(() => messages.find((m) => m.type === "result" && m.id === id), { timeout: 10000 })
          .toEqual({ type: "result", id });
        worker.port.close();
      },
    };
  }
  await (await connect()).request("close");
  const reopened = await open(physical);
  expect(reopened.store.replicaNode).toEqual(node);
  expect(await count(reopened.db)).toBe(1);
  await close(reopened);
  await (await connect()).request("delete-storage");
  const reset = await open(physical);
  expect(reset.store.replicaNode).not.toEqual(node);
  expect(await count(reset.db)).toBe(0);
});

it("admits SDK foreground writes through the SharedWorker and recovers them after both runtimes close", async () => {
  const { NativeRuntimeAdapter } =
    await import("../../src/runtime/native-runtime/native-runtime-adapter.js");
  const { MessagePortBrowserFollowerConnection } =
    await import("../../src/runtime/native-runtime/browser-follower-connection.js");
  const physical = name();
  const workerName = `sqlite-owner-writes-${crypto.randomUUID()}`;
  const author = new TextEncoder().encode('["https://issuer.example","alice"]');
  async function tab() {
    const worker = new SharedWorker(
      new URL("../../src/worker/jazz-broker-worker.ts", import.meta.url),
      { type: "module", name: workerName },
    );
    const ready = new Promise<void>((resolve, reject) => {
      const receive = (event: MessageEvent) => {
        if (event.data?.type === "runtime-ready") {
          worker.port.removeEventListener("message", receive);
          resolve();
        } else if (event.data?.type === "runtime-error") {
          worker.port.removeEventListener("message", receive);
          reject(new Error(event.data.error?.message ?? "worker open failed"));
        }
      };
      worker.port.addEventListener("message", receive);
    });
    worker.port.start();
    worker.port.postMessage({
      type: "connect-runtime",
      tabId: crypto.randomUUID(),
      fingerprint: "sqlite-owner-writes",
      options: {
        schema,
        dbName: physical,
        storageOwner: "alice",
        runtimeSources: sources,
        author,
        initialSyncFlushEvery: 1,
        appId: "sqlite-owner-writes",
        authSessionKey: "alice",
        authJson: "{}",
        sessionClaims: {},
      },
    });
    await ready;
    const wasm = await loadWasmModule(sources);
    const node = crypto.getRandomValues(new Uint8Array(16));
    const db = wasm.WasmDb.openMemory(encodeSchema(schema), openConfig(node, author, 1, false));
    const runtime = NativeRuntimeAdapter.fromDb(db as never, schema, node, author, 1, false);
    runtime.setNonDurableClient();
    const errors: unknown[] = [];
    const connection = new MessagePortBrowserFollowerConnection(
      runtime,
      worker.port,
      {},
      physical,
      {
        onAuthFailure() {},
        onAuthRestored() {},
        onFailure(error) {
          errors.push(error);
        },
      },
    );
    await connection.ready();
    return {
      runtime,
      errors,
      async close() {
        await connection.shutdown(true);
        await runtime.close();
      },
    };
  }
  const first = await tab();
  try {
    const write = first.runtime.insert("notes", {
      text: { type: "Text", value: "offline SDK write" },
    });
    expect(write.kind).toBe("committed");
    if (write.kind !== "committed") throw new Error("write was not committed");
    await first.runtime.waitForTransaction(await write.txId, "local");
    expect(first.errors).toEqual([]);
  } finally {
    await first.close();
  }
  const second = await tab();
  try {
    const rows = await second.runtime.query(
      JSON.stringify({ table: "notes" }),
      null,
      "local-first",
    );
    expect(rows).toHaveLength(1);
    expect(JSON.stringify(rows)).toContain("offline SDK write");
    expect(second.errors).toEqual([]);
  } finally {
    await second.close();
  }
});
