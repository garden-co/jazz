import { randomUUID } from "node:crypto";
import { expect, it } from "vitest";
import {
  NativeRuntimeAdapter,
  encodeCellsForRow,
  encodeSchema,
} from "./native-runtime/native-runtime-adapter.js";
import { encodedCells, openConfig } from "./native-runtime/native-codec.js";
import { loadNapiModule } from "./testing/napi-runtime-test-utils.js";
import { testAuthorBytes } from "./testing/account-fixtures.js";
import type { WriteReceipt } from "./client.js";
import type { WasmSchema } from "../drivers/types.js";

const schema = {
  notes: { columns: [{ name: "text", column_type: { type: "Text" }, nullable: false }] },
} satisfies WasmSchema;

async function committed(write: WriteReceipt) {
  if (write.kind !== "committed") throw new Error("Expected a direct write receipt");
  return await write.txId;
}

async function fixture() {
  const { NapiDb } = await loadNapiModule();
  const node = new Uint8Array(16).fill(0x61);
  const author = testAuthorBytes("native-write-receipts");
  const native = NapiDb.openMemory(encodeSchema(schema), openConfig(node, author, 1, false));
  const runtime = NativeRuntimeAdapter.fromDb(native as never, schema, node, author, 1, false);
  return { native, runtime };
}

it("keeps consecutive SQLite write receipts distinct and applies writes in call order", async () => {
  const { runtime } = await fixture();
  try {
    const inserted = runtime.insert("notes", { text: { type: "Text", value: "before" } });
    const first = runtime.update("notes", inserted.id, { text: { type: "Text", value: "first" } });
    const second = runtime.update("notes", inserted.id, {
      text: { type: "Text", value: "second" },
    });
    const ids = await Promise.all([inserted, first, second].map(committed));
    expect(new Set(ids).size).toBe(3);
    await Promise.all(ids.map((id) => runtime.waitForTransaction(id, "local")));
    expect(await runtime.query(JSON.stringify({ table: "notes" }))).toMatchObject([
      { id: inserted.id, values: [{ type: "Text", value: "second" }] },
    ]);
  } finally {
    await runtime.close();
  }
});

it("an empty SQLite update reuses the existing transaction and settles through the same receipt", async () => {
  const { native, runtime } = await fixture();
  try {
    const inserted = runtime.insert("notes", { text: { type: "Text", value: "unchanged" } });
    const original = await committed(inserted);
    await runtime.waitForTransaction(original, "local");
    const highWater = native.foregroundTxTimeHighWater();
    const noOp = runtime.update("notes", inserted.id, {});
    const unchanged = await committed(noOp);
    expect(unchanged).toBe(original);
    expect(native.foregroundTxTimeHighWater()).toBe(highWater);
    await runtime.waitForTransaction(unchanged, "local");
    expect(await runtime.query(JSON.stringify({ table: "notes" }))).toMatchObject([
      { id: inserted.id, values: [{ type: "Text", value: "unchanged" }] },
    ]);
  } finally {
    await runtime.close();
  }
});

it("a caught streamed attachment refusal rejects companion writes and rollback clears it", async () => {
  const { native, runtime } = await fixture();
  const original = new Uint8Array(16).fill(0x11);
  const companion = new Uint8Array(16).fill(0x12);
  const textCells = (text: string) =>
    encodeCellsForRow(schema.notes, { text: { type: "Text", value: text } });
  try {
    await native.insert("notes", textCells("original"), { rowId: original }).wait("local");
    for (const rollback of [false, true]) {
      const upload = native.beginStreamingMutation("notes", original, encodedCells([], []), "text");
      upload.push(new TextEncoder().encode("refused upload".repeat(8_000)));
      const staged = upload.stage();
      const transaction = randomUUID().replaceAll("-", "");
      native.beginTransaction(transaction, "serializable");
      native.insertInTransaction(transaction, "notes", textCells("must not escape"), {
        rowId: companion,
      });
      let caught: unknown;
      try {
        staged.attach(transaction);
      } catch (error) {
        caught = error;
      }
      if (rollback) {
        native.rollbackTransaction(transaction);
      } else {
        const write = native.commitTransaction(transaction, "serializable");
        await expect(write.wait("local")).rejects.toThrow(/already exists/);
      }
      expect(caught).toBeUndefined();
      expect(staged.abort()).toBe(false);
      expect(await runtime.query(JSON.stringify({ table: "notes" }))).toMatchObject([
        { values: [{ type: "Text", value: "original" }] },
      ]);
    }
    const transaction = randomUUID().replaceAll("-", "");
    native.beginTransaction(transaction, "serializable");
    native.insertInTransaction(transaction, "notes", textCells("replacement"), {
      rowId: companion,
    });
    await native.commitTransaction(transaction, "serializable").wait("local");
    expect(await runtime.query(JSON.stringify({ table: "notes" }))).toHaveLength(2);
  } finally {
    await runtime.close();
  }
});
