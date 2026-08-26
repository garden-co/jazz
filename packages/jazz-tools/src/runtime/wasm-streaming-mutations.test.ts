import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import type { BatchId, WriteReceipt } from "./client.js";
import { createWasmRuntime, hasJazzWasmBuild } from "./testing/wasm-runtime-test-utils.js";

const app = s.defineApp({
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
    metadata: s.json().optional(),
  }),
});

async function committedBatchId(receipt: WriteReceipt): Promise<BatchId> {
  if (receipt.kind !== "committed") throw new Error("expected committed write receipt");
  return await receipt.batchId;
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
  it("selects and filters public provenance authors as canonical text", async () => {
    const appId = "wasm-public-provenance";
    const author = JSON.stringify(["urn:jazz:test", `${appId}:test:default:author`]);
    const runtime = await createWasmRuntime(app.wasmSchema, { appId });
    const inserted = runtime.insert("todos", {
      title: { type: "Text", value: "created by canonical author" },
      done: { type: "Boolean", value: false },
    });
    await runtime.waitForTransaction(await committedBatchId(inserted), "local");

    await expect(
      runtime.query(
        JSON.stringify({
          table: "todos",
          select_columns: ["title", "$createdBy", "$updatedBy"],
          relation_ir: {
            Filter: {
              input: { TableScan: { table: "todos" } },
              predicate: {
                Cmp: {
                  left: { column: "$createdBy" },
                  op: "Eq",
                  right: { Literal: { type: "Text", value: author } },
                },
              },
            },
          },
        }),
        null,
        "local",
      ),
    ).resolves.toEqual([
      {
        table: "todos",
        id: inserted.id,
        values: [
          { type: "Text", value: "created by canonical author" },
          { type: "Text", value: author },
          { type: "Text", value: author },
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
      const batchIds = await Promise.all(writes.map(committedBatchId));
      await withWatchdog(
        Promise.all(batchIds.map((batchId) => runtime.waitForTransaction!(batchId, "local"))),
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
