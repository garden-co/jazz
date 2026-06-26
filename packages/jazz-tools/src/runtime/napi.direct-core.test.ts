import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import { DirectWasmRuntime } from "./direct-wasm/runtime.js";
import { hasJazzNapiBuild, loadNapiModule } from "./testing/napi-runtime-test-utils.js";

const TEST_SCHEMA: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
    ],
  },
};

describe.skipIf(!hasJazzNapiBuild())("jazz-napi direct core memory DB", () => {
  it("opens, mutates one row, and queries it through the direct WASM adapter shape", async () => {
    const { NapiDirectDb } = await loadNapiModule();
    const runtime = new DirectWasmRuntime(
      { openMemory: (schema, config) => NapiDirectDb.openMemory(schema, config) as never },
      TEST_SCHEMA,
      deterministicBytes("jazz-napi-direct-core:node"),
      deterministicBytes("jazz-napi-direct-core:author"),
      1,
      true,
    );

    const inserted = runtime.insert("todos", {
      title: { type: "Text", value: "direct napi memory row" },
      done: { type: "Boolean", value: false },
    });

    await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([
      {
        id: inserted.id,
        table: "todos",
        values: [
          { type: "Text", value: "direct napi memory row" },
          { type: "Boolean", value: false },
        ],
      },
    ]);

    runtime.update("todos", inserted.id, {
      title: { type: "Text", value: "direct napi updated row" },
    });

    await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([
      {
        id: inserted.id,
        table: "todos",
        values: [
          { type: "Text", value: "direct napi updated row" },
          { type: "Boolean", value: false },
        ],
      },
    ]);

    runtime.delete("todos", inserted.id);

    await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([]);
  });

  it("supports direct runtime parity writes, mergeable transactions, and upstream transport", async () => {
    const { NapiDirectDb } = await loadNapiModule();
    const runtime = new DirectWasmRuntime(
      { openMemory: (schema, config) => NapiDirectDb.openMemory(schema, config) as never },
      TEST_SCHEMA,
      deterministicBytes("jazz-napi-direct-core-parity:node"),
      deterministicBytes("jazz-napi-direct-core-parity:author"),
      2,
      true,
    );

    const inserted = runtime.insert("todos", {
      title: { type: "Text", value: "direct napi parity row" },
      done: { type: "Boolean", value: false },
    });
    runtime.delete("todos", inserted.id);
    runtime.restore("todos", inserted.id, {
      title: { type: "Text", value: "direct napi restored row" },
      done: { type: "Boolean", value: false },
    });
    runtime.upsert("todos", "11111111-1111-4111-8111-111111111111", {
      title: { type: "Text", value: "direct napi upserted row" },
      done: { type: "Boolean", value: false },
    });

    const tx = runtime.beginTransaction("mergeable");
    runtime.update(
      "todos",
      inserted.id,
      { done: { type: "Boolean", value: true } },
      JSON.stringify({ batch_id: tx }),
    );
    runtime.upsert(
      "todos",
      inserted.id,
      {
        title: { type: "Text", value: "direct napi tx upserted row" },
        done: { type: "Boolean", value: true },
      },
      JSON.stringify({ batch_id: tx }),
    );
    runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "direct napi tx row" },
        done: { type: "Boolean", value: false },
      },
      JSON.stringify({ batch_id: tx }),
      "22222222-2222-4222-8222-222222222222",
    );
    runtime.commitTransaction(tx);
    await runtime.waitForTransaction(tx, "local");

    const rows = await runtime.query(JSON.stringify({ table: "todos" }));
    expect(rows).toHaveLength(3);
    expect(rows).toEqual(
      expect.arrayContaining([
        {
          id: inserted.id,
          table: "todos",
          values: [
            { type: "Text", value: "direct napi tx upserted row" },
            { type: "Boolean", value: true },
          ],
        },
        {
          id: "11111111-1111-4111-8111-111111111111",
          table: "todos",
          values: [
            { type: "Text", value: "direct napi upserted row" },
            { type: "Boolean", value: false },
          ],
        },
        {
          id: "22222222-2222-4222-8222-222222222222",
          table: "todos",
          values: [
            { type: "Text", value: "direct napi tx row" },
            { type: "Boolean", value: false },
          ],
        },
      ]),
    );

    const transport = runtime.connectUpstreamPeer();
    expect(transport.tick()).toBeGreaterThanOrEqual(0);
    expect(transport.recvWireFrames()).toEqual(expect.any(Array));
    expect(transport.close()).toBe(true);
    expect(transport.close()).toBe(false);
  });

  it("reopens a persistent direct DB and reads previously written rows", async () => {
    const { NapiDirectDb } = await loadNapiModule();
    const tempDir = mkdtempSync(join(tmpdir(), "jazz-napi-direct-"));
    const dataPath = join(tempDir, "db");
    const node = deterministicBytes("jazz-napi-direct-persistent:node");
    const author = deterministicBytes("jazz-napi-direct-persistent:author");
    let firstRuntime: DirectWasmRuntime | null = null;
    let secondRuntime: DirectWasmRuntime | null = null;

    try {
      firstRuntime = new DirectWasmRuntime(
        {
          openMemory: (schema, config) => NapiDirectDb.openMemory(schema, config) as never,
          openPersistent: (path, schema, config) =>
            NapiDirectDb.openPersistent(path, schema, config) as never,
        },
        TEST_SCHEMA,
        node,
        author,
        7,
        true,
        { persistentPath: dataPath },
      );

      const inserted = firstRuntime.insert("todos", {
        title: { type: "Text", value: "direct napi persistent row" },
        done: { type: "Boolean", value: false },
      });
      await firstRuntime.waitForTransaction(inserted.transactionId, "local");
      firstRuntime.close();
      firstRuntime = null;

      secondRuntime = new DirectWasmRuntime(
        {
          openMemory: (schema, config) => NapiDirectDb.openMemory(schema, config) as never,
          openPersistent: (path, schema, config) =>
            NapiDirectDb.openPersistent(path, schema, config) as never,
        },
        TEST_SCHEMA,
        node,
        author,
        7,
        true,
        { persistentPath: dataPath },
      );

      await expect(secondRuntime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([
        {
          id: inserted.id,
          table: "todos",
          values: [
            { type: "Text", value: "direct napi persistent row" },
            { type: "Boolean", value: false },
          ],
        },
      ]);
      secondRuntime.close();
      secondRuntime = null;
    } finally {
      firstRuntime?.close();
      secondRuntime?.close();
      rmSync(tempDir, { recursive: true, force: true });
    }
  });
});

function deterministicBytes(seed: string): Uint8Array {
  let hash = 0x811c9dc5;
  const bytes = new Uint8Array(16);
  const view = new DataView(bytes.buffer);
  for (let round = 0; round < 4; round += 1) {
    for (let i = 0; i < seed.length; i += 1) {
      hash ^= seed.charCodeAt(i) + round;
      hash = Math.imul(hash, 0x01000193);
    }
    view.setUint32(round * 4, hash >>> 0, true);
  }
  return bytes;
}
