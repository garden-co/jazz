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
});

describe("NapiRuntime sealed write helpers", () => {
  it("exposes Rust-sealed writes and Rust-generated batch contexts", async () => {
    const { NapiRuntime } = await loadNapiModule();
    dataDir = await mkdtemp(join(tmpdir(), "jazz-napi-runtime-sealed-"));
    const dataPath = join(dataDir, "runtime.db");
    const runtime = new NapiRuntime(
      serializeRuntimeSchema(schema),
      "napi-runtime-sealed-write-test",
      "dev",
      "main",
      dataPath,
      undefined,
    );
    const aliceId = "00000000-0000-7000-8000-000000000011";

    const context = runtime.createWriteBatchContext("transactional");
    expect(context.batchMode).toBe("transactional");
    expect(context.batchId).toEqual(expect.any(String));
    expect(context.targetBranchName).toContain("dev-");

    const inserted = runtime.insertSealed(
      "users",
      {
        id: { type: "Uuid", value: aliceId },
        name: { type: "Text", value: "Alice" },
      },
      undefined,
      aliceId,
    );
    const updated = runtime.updateSealed(
      aliceId,
      {
        name: { type: "Text", value: "Alicia" },
      },
      undefined,
    );
    const deleted = runtime.deleteSealed(aliceId, undefined);

    expect(inserted.batchId).toEqual(expect.any(String));
    expect(updated.batchId).toEqual(expect.any(String));
    expect(deleted.batchId).toEqual(expect.any(String));
    runtime.close();
  });
});
