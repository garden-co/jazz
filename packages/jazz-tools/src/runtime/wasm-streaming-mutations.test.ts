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
});
