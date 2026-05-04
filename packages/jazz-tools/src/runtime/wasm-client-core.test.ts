import { describe, expect, it } from "vitest";
import { serializeRuntimeSchema } from "../drivers/schema-wire.js";
import type { WasmSchema } from "../drivers/types.js";
import { loadWasmModule } from "./testing/wasm-runtime-test-utils.js";

const schema: WasmSchema = {
  users: {
    columns: [
      { name: "id", column_type: { type: "Uuid" }, nullable: false },
      { name: "name", column_type: { type: "Text" }, nullable: false },
    ],
  },
};

describe("WasmJazzClient binding shape", () => {
  it("is declared by jazz-wasm types", async () => {
    const wasm = await import("jazz-wasm");

    expect(typeof wasm.WasmJazzClient).toBe("function");
  });

  it("uses Rust-owned direct batches", async () => {
    const { WasmJazzClient } = await loadWasmModule();
    const client = new WasmJazzClient(
      serializeRuntimeSchema(schema),
      "wasm-client-core-batch-test",
      "dev",
      "main",
    );

    const batch = client.beginDirectBatch();
    const aliceId = "00000000-0000-7000-8000-000000000001";
    const bobId = "00000000-0000-7000-8000-000000000002";
    const alice = batch.insert(
      "users",
      {
        id: { type: "Uuid", value: aliceId },
        name: { type: "Text", value: "Alice" },
      },
      aliceId,
    );
    const bob = batch.insert(
      "users",
      {
        id: { type: "Uuid", value: bobId },
        name: { type: "Text", value: "Bob" },
      },
      bobId,
    );
    const updated = batch.update(aliceId, {
      name: { type: "Text", value: "Alicia" },
    });
    const deleted = batch.delete(bobId);
    const handle = batch.commit();

    expect(alice.batchId).toEqual(bob.batchId);
    expect(alice.batchId).toEqual(updated.batchId);
    expect(alice.batchId).toEqual(deleted.batchId);
    expect(alice.batchId).toEqual(handle.batchId);
  });
});
