import { describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { createWasmRuntime, hasJazzWasmBuild } from "./testing/wasm-runtime-test-utils.js";

const app = s.defineApp({
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
    metadata: s.json().optional(),
  }),
});

describe.skipIf(!hasJazzWasmBuild())("WASM streaming mutations", () => {
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
