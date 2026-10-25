import { translateQuery } from "./query-adapter.js";
import * as idb from "fake-indexeddb";
import { afterAll, beforeAll, describe, expect, it, onTestFinished, vi } from "vitest";
import { schema as s } from "../index.js";
import type { TxId, WriteReceipt } from "./client.js";
import {
  encodeCellsForRow,
  encodeSchema,
  NativeRuntimeAdapter,
} from "./native-runtime/native-runtime-adapter.js";
import { openConfig, queryFromTable } from "./native-runtime/native-codec.js";
import {
  createWasmRuntime,
  hasJazzWasmBuild,
  loadWasmModuleForTest,
} from "./testing/wasm-runtime-test-utils.js";
import { testAccountId, testAuthorBytes } from "./testing/account-fixtures.js";

const app = s.defineApp({
  todos: s.table(
    {
      title: s.string(),
      done: s.boolean(),
      metadata: s.json().optional(),
    },
    {},
  ),
});
const streamingApp = s.defineApp({
  todos: s.table(
    {
      title: s.string().optional(),
      done: s.boolean(),
    },
    {},
  ),
});

beforeAll(() => {
  for (const [name, value] of Object.entries(idb)) {
    if (name.startsWith("IDB")) vi.stubGlobal(name, value);
  }
  vi.stubGlobal("indexedDB", new idb.IDBFactory());
});
afterAll(() => vi.unstubAllGlobals());

// Hold a real IndexedDB transaction open with ordinary requests. The SQLite
// VFS still queues and writes its actual blocks; only commit is delayed.
function holdNextFileCommit(path: string) {
  const started = deferredVoid();
  const originalPut = idb.IDBObjectStore.prototype.put;
  const originalCommit = idb.IDBTransaction.prototype.commit;
  let transaction: IDBTransaction | undefined;
  let commitHeld = false;
  let released = false;
  const keepAlive = () => {
    if (released) {
      if (commitHeld) {
        commitHeld = false;
        originalCommit.call(transaction!);
      }
      return;
    }
    const request = transaction!.objectStore("blocks").get([path, 0]);
    request.onsuccess = keepAlive;
  };
  const put = vi.spyOn(idb.IDBObjectStore.prototype, "put").mockImplementation(function (
    this: IDBObjectStore,
    value: unknown,
    key?: IDBValidKey,
  ) {
    const result = originalPut.call(this, value, key);
    if (!transaction && this.name === "blocks" && Reflect.get(value as object, "path") === path) {
      transaction = this.transaction;
      keepAlive();
    }
    return result;
  });
  const commit = vi
    .spyOn(idb.IDBTransaction.prototype, "commit")
    .mockImplementation(function (this: IDBTransaction) {
      if (this === transaction && !released) {
        commitHeld = true;
        started.resolve();
      } else originalCommit.call(this);
    });
  return {
    started: started.promise,
    release() {
      released = true;
    },
    dispose() {
      released = true;
      put.mockRestore();
      commit.mockRestore();
    },
  };
}

type PageStoreMetadata = {
  pageSize: number;
  generation: number;
  rootPageId: number | null;
  nextPageId: number;
};

function deferredVoid() {
  let resolve!: () => void;
  const promise = new Promise<void>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function createTestPageStore() {
  let persistedMetadata: PageStoreMetadata | null = null;
  const pages = new Map<number, Uint8Array>();
  let pendingGate:
    | {
        started: () => void;
        wait: Promise<void>;
        release: () => void;
        failAfterRelease: boolean;
      }
    | undefined;

  const pageStore = {
    storageAdmission: () => "ephemeral",
    metadata: async () => persistedMetadata,
    readPage: async (pageId: number) => pages.get(pageId) ?? null,
    commitPages: async (
      expectedGeneration: number,
      pageSize: number,
      rootPageId: number,
      nextPageId: number,
      pageIds: number[],
      pageBytes: Uint8Array[],
      deletedPageIds: number[],
    ) => {
      const gate = pendingGate;
      if (gate) {
        pendingGate = undefined;
        gate.started();
        await gate.wait;
        if (gate.failAfterRelease) throw new Error("injected page-store commit failure");
      }
      for (let index = 0; index < pageIds.length; index += 1) {
        pages.set(pageIds[index]!, new Uint8Array(pageBytes[index]!));
      }
      for (const pageId of deletedPageIds) pages.delete(pageId);
      persistedMetadata = {
        pageSize,
        generation: expectedGeneration + 1,
        rootPageId: rootPageId < 0 ? null : rootPageId,
        nextPageId,
      };
      return persistedMetadata;
    },
  };

  return {
    pageStore,
    armCommitGate(failAfterRelease = false) {
      const wait = deferredVoid();
      const started = deferredVoid();
      pendingGate = {
        started: started.resolve,
        wait: wait.promise,
        release: wait.resolve,
        failAfterRelease,
      };
      return { started: started.promise, release: wait.resolve };
    },
  };
}

let nextBrowserFixture = 0;
async function createBrowserWasmFixture() {
  const wasmModule = await loadWasmModuleForTest();
  const node = new Uint8Array(16);
  node[0] = 1;
  const author = testAuthorBytes("wasm-streaming-abort:author");
  const storage = { backend: "indexeddb", path: `/wasm-streaming-${++nextBrowserFixture}.sqlite` };
  const gates: ReturnType<typeof holdNextFileCommit>[] = [];
  const db = await wasmModule.WasmDb.openBrowser(
    storage,
    encodeSchema(streamingApp.wasmSchema),
    openConfig(node, author, 1, true),
    "wasm-streaming-abort-owner",
  );
  const runtime = NativeRuntimeAdapter.fromDb(db, streamingApp.wasmSchema, node, author, 1, true);
  onTestFinished(async () => {
    for (const gate of gates) gate.release();
    try {
      await runtime.close?.();
    } finally {
      for (const gate of gates) gate.dispose();
      await wasmModule.WasmDb.destroyBrowserStorage(storage, "wasm-streaming-abort-owner");
    }
  });
  return {
    db,
    runtime,
    author,
    armCommitGate() {
      const gate = holdNextFileCommit(storage.path);
      gates.push(gate);
      return gate;
    },
  };
}

function uuidBytes(value: string): Uint8Array {
  return Uint8Array.from(value.replaceAll("-", "").match(/../g)!, (byte) =>
    Number.parseInt(byte, 16),
  );
}

async function committedTxId(receipt: WriteReceipt): Promise<TxId> {
  if (receipt.kind !== "committed") throw new Error("expected committed write receipt");
  return await receipt.txId;
}

async function withWatchdog<T>(promise: Promise<T>, label: string, timeoutMs = 3_000): Promise<T> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timeout = setTimeout(
          () => reject(new Error(`${label} timed out after ${timeoutMs}ms`)),
          timeoutMs,
        );
      }),
    ]);
  } finally {
    if (timeout) clearTimeout(timeout);
  }
}

describe.skipIf(!hasJazzWasmBuild())("WASM streaming mutations", () => {
  it("reports write state while IndexedDB durability is pending", async () => {
    const { db, armCommitGate } = await createBrowserWasmFixture();
    const write = db.insert(
      "todos",
      encodeCellsForRow(streamingApp.wasmSchema.todos!, {
        title: { type: "Text", value: "settled" },
        done: { type: "Boolean", value: false },
      }),
    );
    await withWatchdog(write.wait("local"), "seed write settlement");
    const gate = armCommitGate();
    const upload = db.beginStreamingMutation(
      "todos",
      uuidBytes("00000000-0000-4000-8000-000000000141"),
      encodeCellsForRow(streamingApp.wasmSchema.todos!, {
        done: { type: "Boolean", value: false },
      }),
      "title",
      "insert",
    );
    let streamed: ReturnType<typeof db.insert> | undefined;
    let local: Promise<void> | undefined;
    let settled = false;
    try {
      await upload.push(new TextEncoder().encode("pending".repeat(80_000)));
      streamed = await upload.finish();
      local = streamed.wait("local").then(() => {
        settled = true;
      });
      await withWatchdog(gate.started, "streaming commit reaches IndexedDB");
      // SQLite remains readable while its asynchronous backing-store commit waits.
      expect(write.writeState()).toMatchObject({ durability: "Local" });
      expect(streamed.writeState()).toMatchObject({ durability: "None" });
      expect(settled).toBe(false);
    } finally {
      gate.release();
      try {
        if (local) await withWatchdog(local, "streamed local durability");
      } finally {
        gate.dispose();
        await upload.abort();
      }
      write.close();
    }
    expect(settled).toBe(true);
    expect(streamed.writeState()).toMatchObject({ durability: "Local" });
    streamed.close();
  });

  it("rejects an unsupported browser store before initializing its tree", async () => {
    const wasm = await loadWasmModuleForTest();
    const store = createTestPageStore();
    Reflect.deleteProperty(store.pageStore, "storageAdmission");
    const node = new Uint8Array(16);
    node[0] = 1;
    await expect(
      wasm.WasmDb.openBrowser(
        store.pageStore,
        encodeSchema(streamingApp.wasmSchema),
        openConfig(node, testAuthorBytes("unsupported-browser-store"), 1, true),
        "unsupported-browser-store",
      ),
    ).rejects.toThrow("does not provide durable admission");
    expect(await store.pageStore.metadata()).toBeNull();
    expect(await store.pageStore.readPage(0)).toBeNull();
  });

  it("keeps reads and subscriptions responsive while IndexedDB flush waits", async () => {
    const { db, runtime, armCommitGate, author } = await createBrowserWasmFixture();
    const query = queryFromTable("todos");
    const opts = { tier: "local-only" };
    const commitGate = armCommitGate();
    const upload = db.beginStreamingMutation(
      "todos",
      uuidBytes("00000000-0000-4000-8000-000000000131"),
      encodeCellsForRow(streamingApp.wasmSchema.todos!, {
        done: { type: "Boolean", value: false },
      }),
      "title",
      "insert",
      author,
    );
    const push = upload.push(new TextEncoder().encode("pending owner".repeat(50_000)));
    let flushSettled = false;
    const flush = db.flush().then(() => {
      flushSettled = true;
    });
    let cancelSubscription: number | undefined;
    try {
      await withWatchdog(commitGate.started, "streaming commit reaches IndexedDB");
      await expect(push).resolves.toBeUndefined();
      // The native ABI now returns a ReadableStream directly.
      const subscription = db.subscribe(query, opts) as ReadableStream<{
        type: string;
        delta?: Uint8Array;
      }>;
      const reader = subscription.getReader();
      try {
        const first = await withWatchdog(reader.read(), "native subscription during flush");
        expect(first.done).toBe(false);
        expect(first.value).toMatchObject({ type: "delta", delta: expect.any(Uint8Array) });
      } finally {
        await reader.cancel();
        reader.releaseLock();
      }

      let cancelledCallbacks = 0;
      const cancelled = runtime.createSubscription(JSON.stringify({ table: "todos" }));
      runtime.executeSubscription(cancelled, () => {
        cancelledCallbacks += 1;
      });
      runtime.unsubscribe(cancelled);

      const pendingQuery = runtime.query(JSON.stringify({ table: "todos" }));
      cancelSubscription = runtime.createSubscription(
        JSON.stringify({ table: "todos" }),
        undefined,
        "local-only",
      );
      const opening = deferredVoid();
      let openingError: unknown;
      runtime.executeSubscription(cancelSubscription, (result: unknown) => {
        if (result instanceof Error) openingError = result;
        opening.resolve();
      });
      await expect(withWatchdog(pendingQuery, "concurrent query during flush")).resolves.toEqual(
        [],
      );
      await withWatchdog(opening.promise, "concurrent subscription during flush");
      expect(openingError).toBeUndefined();
      expect(cancelledCallbacks).toBe(0);
      expect(flushSettled).toBe(false);
      await expect(upload.abort()).resolves.toBe(true);
    } finally {
      commitGate.release();
      try {
        await withWatchdog(flush, "streaming flush resumes");
      } finally {
        commitGate.dispose();
      }
      if (cancelSubscription !== undefined) runtime.unsubscribe(cancelSubscription);
      await upload.abort();
    }
    expect(flushSettled).toBe(true);
  });

  it("aborts a synchronously staged upload before observing its push promise", async () => {
    const { db, runtime, author } = await createBrowserWasmFixture();
    const upload = db.beginStreamingMutation(
      "todos",
      uuidBytes("00000000-0000-4000-8000-000000000128"),
      encodeCellsForRow(streamingApp.wasmSchema.todos!, {
        done: { type: "Boolean", value: false },
      }),
      "title",
      "insert",
      author,
    );
    // Calling push stages its chunks synchronously. Do not await its result
    // before aborting: observing that promise later must not resurrect the upload.
    const push = upload.push(new TextEncoder().encode("staged push".repeat(60_000)));
    const abort = upload.abort();
    const finishRejected = expect(upload.finish()).rejects.toThrow(/closed/i);
    const secondPushRejected = expect(upload.push(Uint8Array.of(65))).rejects.toThrow(/closed/i);
    const duplicate = upload.abort();
    await expect(abort).resolves.toBe(true);
    await Promise.all([finishRejected, secondPushRejected]);
    await expect(duplicate).resolves.toBe(false);
    await expect(push).resolves.toBeUndefined();
    await expect(upload.finish()).rejects.toThrow(/closed/i);
    await expect(upload.push(Uint8Array.of(65))).rejects.toThrow(/closed/i);
    await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([]);
    db.setLargeValueStagingPolicy(Number.MAX_SAFE_INTEGER, 60_000, 0);
    await new Promise((resolve) => setTimeout(resolve, 2));
    await expect(db.evictExpiredStagedLargeValues()).resolves.toBe(0);
  });

  it("closes a failed synchronous push without retaining its earlier staged chunks", async () => {
    const { db, runtime, author } = await createBrowserWasmFixture();
    const upload = db.beginStreamingMutation(
      "todos",
      uuidBytes("00000000-0000-4000-8000-000000000130"),
      encodeCellsForRow(streamingApp.wasmSchema.todos!, {
        done: { type: "Boolean", value: false },
      }),
      "title",
      "insert",
      author,
    );
    await upload.push(new TextEncoder().encode("earlier staged chunks".repeat(30_000)));
    db.setLargeValueStagingPolicy(1, 60_000, null);
    const pushRejected = expect(
      upload.push(new TextEncoder().encode("failed push".repeat(60_000))),
    ).rejects.toThrow(/rate limit/i);
    const abort = upload.abort();
    await pushRejected;
    // The failed synchronous push already consumed the upload and released it.
    await expect(abort).resolves.toBe(false);
    await expect(upload.abort()).resolves.toBe(false);
    await expect(upload.finish()).rejects.toThrow(/closed/i);
    await expect(upload.push(Uint8Array.of(65))).rejects.toThrow(/closed/i);
    await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([]);
    db.setLargeValueStagingPolicy(Number.MAX_SAFE_INTEGER, 60_000, 0);
    await new Promise((resolve) => setTimeout(resolve, 2));
    await expect(db.evictExpiredStagedLargeValues()).resolves.toBe(0);
  });

  it("keeps abort terminal across synchronous finish and repeated abort calls", async () => {
    const { db, runtime, author } = await createBrowserWasmFixture();
    const upload = db.beginStreamingMutation(
      "todos",
      uuidBytes("00000000-0000-4000-8000-000000000129"),
      encodeCellsForRow(streamingApp.wasmSchema.todos!, {
        done: { type: "Boolean", value: false },
      }),
      "title",
      "insert",
      author,
    );

    await expect(
      upload.push(new TextEncoder().encode("preserved push result")),
    ).resolves.toBeUndefined();
    const abort = upload.abort();
    const finish = upload.finish();
    const repeatedAbort = upload.abort();

    await expect(abort).resolves.toBe(true);
    await expect(repeatedAbort).resolves.toBe(false);
    await expect(finish).rejects.toThrow(/closed/i);
    await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([]);
  });

  it("selects and filters public provenance authors as structured records", async () => {
    const appId = "wasm-public-provenance";
    const authorSeed = `${appId}:test:default:author`;
    const author = {
      account: testAccountId(authorSeed),
      identity: { issuer: "urn:jazz:test", subject: authorSeed },
    };
    const runtime = await createWasmRuntime(app.wasmSchema, { appId });
    const inserted = runtime.insert("todos", {
      title: { type: "Text", value: "created by canonical author" },
      done: { type: "Boolean", value: false },
    });
    await runtime.waitForTransaction(await committedTxId(inserted), "local");

    await expect(
      runtime.query(
        translateQuery(
          app.todos
            .select("title", "$createdBy", "$updatedBy")
            .where({ $createdBy: author })
            ._build(),
          app.wasmSchema,
        ),
        null,
        "local-first",
      ),
    ).resolves.toEqual([
      {
        table: "todos",
        id: inserted.id,
        values: [
          { type: "Text", value: "created by canonical author" },
          expect.objectContaining({
            type: "Row",
            value: expect.objectContaining({
              values: [
                { type: "Uuid", value: author.account },
                expect.objectContaining({
                  type: "Row",
                  value: expect.objectContaining({
                    values: [
                      { type: "Text", value: author.identity.issuer },
                      { type: "Text", value: author.identity.subject },
                    ],
                  }),
                }),
              ],
            }),
          }),
          expect.objectContaining({
            type: "Row",
            value: expect.objectContaining({
              values: [
                { type: "Uuid", value: author.account },
                expect.objectContaining({
                  type: "Row",
                  value: expect.objectContaining({
                    values: [
                      { type: "Text", value: author.identity.issuer },
                      { type: "Text", value: author.identity.subject },
                    ],
                  }),
                }),
              ],
            }),
          }),
        ],
      },
    ]);
  });

  it("streams insert, update, and partial upsert through the browser runtime boundary", async () => {
    const runtime = await createWasmRuntime(app.wasmSchema, {
      appId: "wasm-streaming-mutations",
    });
    const id = "00000000-0000-4000-8000-000000000123";

    await runtime.streamingMutation!(
      "insert",
      "todos",
      { done: { type: "Boolean", value: false } },
      "title",
      (async function* () {
        yield "streamed ";
        yield new TextEncoder().encode("through WASM ");
        yield "\ud83d";
        yield "\ude80";
      })(),
      null,
      id,
    );
    await runtime.streamingMutation!(
      "update",
      "todos",
      { done: { type: "Boolean", value: true } },
      "title",
      (async function* () {
        yield "updated";
      })(),
      null,
      id,
    );
    await runtime.streamingMutation!(
      "upsert",
      "todos",
      {},
      "title",
      (async function* () {
        yield "upserted";
      })(),
      null,
      id,
    );

    await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([
      {
        id,
        table: "todos",
        values: [
          { type: "Text", value: "upserted" },
          { type: "Boolean", value: true },
          { type: "Null" },
        ],
      },
    ]);
  });

  it("finishes concurrent streamed writes without wedging the shared WASM runtime", async () => {
    const runtime = await createWasmRuntime(app.wasmSchema, {
      appId: "wasm-concurrent-streaming-publication",
    });
    const source = async function* (prefix: string) {
      yield `${prefix} first `;
      await Promise.resolve();
      yield `${prefix} second`;
    };
    let bodyError: unknown;

    try {
      const writes = await withWatchdog(
        Promise.all([
          runtime.streamingMutation!(
            "insert",
            "todos",
            { done: { type: "Boolean", value: false } },
            "title",
            source("one"),
            null,
            "00000000-0000-4000-8000-000000000201",
          ),
          runtime.streamingMutation!(
            "insert",
            "todos",
            { done: { type: "Boolean", value: true } },
            "title",
            source("two"),
            null,
            "00000000-0000-4000-8000-000000000202",
          ),
        ]),
        "concurrent streamed WASM publication",
      );
      const txIds = await Promise.all(writes.map(committedTxId));
      await withWatchdog(
        Promise.all(txIds.map((txId) => runtime.waitForTransaction!(txId, "local"))),
        "concurrent streamed WASM local settlement",
      );

      await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([
        {
          id: "00000000-0000-4000-8000-000000000201",
          table: "todos",
          values: [
            { type: "Text", value: "one first one second" },
            { type: "Boolean", value: false },
            { type: "Null" },
          ],
        },
        {
          id: "00000000-0000-4000-8000-000000000202",
          table: "todos",
          values: [
            { type: "Text", value: "two first two second" },
            { type: "Boolean", value: true },
            { type: "Null" },
          ],
        },
      ]);
    } catch (error) {
      bodyError = error;
    }
    let cleanupError: unknown;
    try {
      const closeRuntime = runtime.close;
      if (!closeRuntime) throw new Error("WASM runtime does not expose close()");
      await withWatchdog(
        Promise.resolve().then(() => closeRuntime.call(runtime)),
        "concurrent streamed WASM runtime cleanup",
        1_000,
      );
    } catch (error) {
      cleanupError = error;
    }
    // The publication failure is the actionable signal when both phases fail.
    if (bodyError) throw bodyError;
    if (cleanupError) throw cleanupError;
  });

  it("rejects fragmented invalid JSON without publishing a row", async () => {
    const runtime = await createWasmRuntime(app.wasmSchema, {
      appId: "wasm-streaming-invalid-json",
    });

    await expect(
      runtime.streamingMutation!(
        "insert",
        "todos",
        {
          title: { type: "Text", value: "invalid" },
          done: { type: "Boolean", value: false },
        },
        "metadata",
        (async function* () {
          yield '{"nested":[';
          yield "1,";
          yield "]}";
        })(),
        null,
        "00000000-0000-4000-8000-000000000124",
      ),
    ).rejects.toThrow();
    await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([]);
  });

  it("stops push uploads at the ingress limit before accepting the row", async () => {
    const runtime = await createWasmRuntime(app.wasmSchema, {
      appId: "wasm-streaming-rate-limit",
    });
    runtime.setLargeValueStagingPolicy!(1, 60_000, null);

    await expect(
      runtime.streamingMutation!(
        "insert",
        "todos",
        { done: { type: "Boolean", value: false } },
        "title",
        (async function* () {
          yield "x".repeat(512 * 1024);
        })(),
        null,
        "00000000-0000-4000-8000-000000000125",
      ),
    ).rejects.toThrow(/rate limit/i);
    await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([]);
  });

  it("rejects unsafe custom timestamps before consuming the stream", async () => {
    const runtime = await createWasmRuntime(app.wasmSchema, {
      appId: "wasm-streaming-unsafe-timestamp",
    });
    let consumed = false;

    await expect(
      runtime.streamingMutation!(
        "insert",
        "todos",
        { done: { type: "Boolean", value: false } },
        "title",
        (async function* () {
          consumed = true;
          yield "never consumed";
        })(),
        JSON.stringify({ updated_at: Number.MAX_SAFE_INTEGER + 1 }),
        "00000000-0000-4000-8000-000000000126",
      ),
    ).rejects.toThrow(/safe integer/i);
    expect(consumed).toBe(false);
  });

  it("releases persisted pending chunks when the producer aborts", async () => {
    const runtime = await createWasmRuntime(app.wasmSchema, {
      appId: "wasm-streaming-explicit-abort",
    });

    await expect(
      runtime.streamingMutation!(
        "insert",
        "todos",
        { done: { type: "Boolean", value: false } },
        "title",
        (async function* () {
          yield "x".repeat(512 * 1024);
          throw new Error("producer aborted");
        })(),
        null,
        "00000000-0000-4000-8000-000000000127",
      ),
    ).rejects.toThrow("producer aborted");

    runtime.setLargeValueStagingPolicy!(Number.MAX_SAFE_INTEGER, 60_000, 0);
    await new Promise((resolve) => setTimeout(resolve, 2));
    await expect(runtime.evictExpiredStagedLargeValues!()).resolves.toBe(0);
  });
});
