import { translateQuery } from "./query-adapter.js";
import { describe, expect, it, onTestFinished } from "vitest";
import { schema as s } from "../index.js";
import type { TxId, WriteReceipt } from "./client.js";
import {
  encodeCellsForRow,
  encodeSchema,
  NativeRuntimeAdapter,
} from "./native-runtime/native-runtime-adapter.js";
import { openConfig } from "./native-runtime/native-codec.js";
import {
  createWasmRuntime,
  hasJazzWasmBuild,
  loadWasmModuleForTest,
} from "./testing/wasm-runtime-test-utils.js";
import { testAccountId, testAuthorBytes } from "./testing/account-fixtures.js";

const app = s.defineApp(
  {
    todos: s.table(
      {
        title: s.string(),
        done: s.boolean(),
        metadata: s.json().optional(),
      },
      {},
    ),
  },
  { migrations: "none" },
);
const streamingApp = s.defineApp(
  {
    todos: s.table(
      {
        title: s.string().optional(),
        done: s.boolean(),
      },
      {},
    ),
  },
  { migrations: "none" },
);

function deferredVoid() {
  let resolve!: () => void;
  const promise = new Promise<void>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

// The streaming fixture's SQLite store applies a commit before the write
// call returns: no write waits on storage, so none holds the node.
async function createStreamingWasmFixture() {
  const wasmModule = await loadWasmModuleForTest();
  const node = new Uint8Array(16);
  node[0] = 1;
  const author = testAuthorBytes("wasm-streaming-abort:author");
  const db = wasmModule.WasmDb.openMemory(
    encodeSchema(streamingApp.wasmSchema),
    openConfig(node, author, 1, true),
  );
  const runtime = NativeRuntimeAdapter.fromDb(db, streamingApp.wasmSchema, node, author, 1, true);
  onTestFinished(async () => {
    await runtime.close?.();
  });
  return { db, runtime, author };
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
  it("refuses write state snapshots explicitly while a streaming upload is open", async () => {
    const { db, author } = await createStreamingWasmFixture();
    const write = db.insert(
      "todos",
      encodeCellsForRow(streamingApp.wasmSchema.todos!, {
        title: { type: "Text", value: "settled" },
        done: { type: "Boolean", value: false },
      }),
    );
    await withWatchdog(write.wait("local"), "seed write settlement");
    const upload = db.beginStreamingMutation(
      "todos",
      uuidBytes("00000000-0000-4000-8000-000000000141"),
      encodeCellsForRow(streamingApp.wasmSchema.todos!, {
        done: { type: "Boolean", value: false },
      }),
      "title",
      "insert",
      author,
    );
    const push = upload.push(new TextEncoder().encode("pending"));
    try {
      expect(() => write.writeState()).toThrow(/write state snapshots is not supported/);
    } finally {
      await withWatchdog(push, "upload push");
      await upload.abort();
    }
    write.close();
  });

  it("answers real WASM reads and subscriptions while a push is in flight", async () => {
    const { db, runtime, author } = await createStreamingWasmFixture();
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
    const push = upload.push(new TextEncoder().encode("pending owner"));
    let subscription: number | undefined;
    try {
      let cancelledCallbacks = 0;
      const cancelled = runtime.createSubscription(JSON.stringify({ table: "todos" }));
      runtime.executeSubscription(cancelled, () => {
        cancelledCallbacks += 1;
      });
      runtime.unsubscribe(cancelled);

      const query = runtime.query(JSON.stringify({ table: "todos" }));
      subscription = runtime.createSubscription(
        JSON.stringify({ table: "todos" }),
        undefined,
        "local-only",
      );
      const opening = deferredVoid();
      let openingError: unknown;
      runtime.executeSubscription(subscription, (result: unknown) => {
        if (result instanceof Error) openingError = result;
        opening.resolve();
      });
      // The staged upload is no row yet.
      await expect(withWatchdog(query, "concurrent query")).resolves.toEqual([]);
      await withWatchdog(opening.promise, "concurrent subscription");
      expect(openingError).toBeUndefined();
      await withWatchdog(push, "upload push");
      expect(cancelledCallbacks).toBe(0);
    } finally {
      if (subscription !== undefined) runtime.unsubscribe(subscription);
      await upload.abort();
    }
  });

  it("waits for an in-flight WASM push before aborting its staged upload", async () => {
    const { db, runtime, author } = await createStreamingWasmFixture();
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

    const push = upload.push(new TextEncoder().encode("in-flight push"));
    const abort = upload.abort();
    const finishRejected = expect(upload.finish()).rejects.toThrow(/closed/i);
    const secondPushRejected = expect(upload.push(Uint8Array.of(65))).rejects.toThrow(/closed/i);
    const duplicate = upload.abort();
    await Promise.all([finishRejected, secondPushRejected]);
    await expect(duplicate).resolves.toBe(false);
    await expect(push).resolves.toBeUndefined();
    await expect(abort).resolves.toBe(true);
    await expect(upload.finish()).rejects.toThrow(/closed/i);
    await expect(upload.push(Uint8Array.of(65))).rejects.toThrow(/closed/i);
    await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([]);

    db.setLargeValueStagingPolicy(Number.MAX_SAFE_INTEGER, 60_000, 0);
    // Expiry uses the platform clock; advance one event-loop turn past maxAgeMs=0.
    await new Promise((resolve) => setTimeout(resolve, 2));
    await expect(db.evictExpiredStagedLargeValues()).resolves.toBe(0);
  });

  it("hands failed in-flight push ownership to abort without retaining staged chunks", async () => {
    const { db, runtime, author } = await createStreamingWasmFixture();
    db.setLargeValueStagingPolicy(1, 60_000, null);
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
    // Past the ingress limit, the push fails.
    const pushRejected = expect(
      upload.push(new TextEncoder().encode("x".repeat(512 * 1024))),
    ).rejects.toThrow(/rate limit/i);
    const abort = upload.abort();
    await pushRejected;
    await expect(abort).resolves.toBe(true);
    await expect(upload.abort()).resolves.toBe(false);
    await expect(upload.finish()).rejects.toThrow(/closed/i);
    await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([]);
    db.setLargeValueStagingPolicy(Number.MAX_SAFE_INTEGER, 60_000, 0);
    await new Promise((resolve) => setTimeout(resolve, 2));
    await expect(db.evictExpiredStagedLargeValues()).resolves.toBe(0);
  });

  it("keeps abort terminal across synchronous finish and repeated abort calls", async () => {
    const { db, runtime, author } = await createStreamingWasmFixture();
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
