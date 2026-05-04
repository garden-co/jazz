import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { serializeRuntimeSchema } from "../drivers/schema-wire.js";
import type { WasmSchema } from "../drivers/types.js";
import { loadNapiModule } from "./testing/napi-runtime-test-utils.js";

const schema: WasmSchema = {
  users: {
    columns: [
      { name: "id", column_type: { type: "Uuid" }, nullable: false },
      { name: "name", column_type: { type: "Text" }, nullable: false },
    ],
  },
};

let dataDir: string | undefined;

afterEach(async () => {
  if (dataDir) {
    await rm(dataDir, { recursive: true, force: true });
    dataDir = undefined;
  }
});

describe("NapiJazzClient", () => {
  it("delegates insert behavior to Rust client core", async () => {
    const { NapiJazzClient } = await loadNapiModule();
    dataDir = await mkdtemp(join(tmpdir(), "jazz-napi-client-core-"));
    const dataPath = join(dataDir, "runtime.db");
    const client = new NapiJazzClient(
      serializeRuntimeSchema(schema),
      "napi-client-core-test",
      "dev",
      "main",
      dataPath,
    );

    const result = client.insert("users", {
      id: { type: "Uuid", value: "00000000-0000-7000-8000-000000000001" },
      name: { type: "Text", value: "Alice" },
    });

    expect(result.batchId).toEqual(expect.any(String));
    expect(result.values[1]).toEqual({ type: "Text", value: "Alice" });
    client.close();
  });

  it("uses Rust-owned direct batches", async () => {
    const { NapiJazzClient } = await loadNapiModule();
    dataDir = await mkdtemp(join(tmpdir(), "jazz-napi-client-core-batch-"));
    const dataPath = join(dataDir, "runtime.db");
    const client = new NapiJazzClient(
      serializeRuntimeSchema(schema),
      "napi-client-core-batch-test",
      "dev",
      "main",
      dataPath,
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
    client.close();
  });
});
