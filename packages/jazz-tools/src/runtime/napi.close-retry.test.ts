import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import { expect, it } from "vitest";
import {
  encodeCellsForRow,
  encodeSchema,
  NativeRuntimeAdapter,
  rowsFromBatches,
} from "./native-runtime/native-runtime-adapter.js";
import {
  encodedCells,
  openConfig,
  PostcardReader,
  readNativeRowBatch,
} from "./native-runtime/native-codec.js";
import { testAuthorBytes } from "./testing/account-fixtures.js";
import { loadNapiModule } from "./testing/napi-runtime-test-utils.js";
import type { WasmSchema } from "../drivers/types.js";

// Alice's admitted native root contains a committed file and an abandoned
// upload. A separate SQLite process holds a write lock to fail real cleanup;
// it changes no stored bytes. This exercises the public close/tick/reopen path
// without adding a synthetic native failure hook or reviving Groove's queue.
async function lockSQLite(path: string): Promise<() => Promise<void>> {
  const child = spawn(
    process.execPath,
    [
      "--input-type=module",
      "-e",
      `import { DatabaseSync } from "node:sqlite";
       const db = new DatabaseSync(process.argv[1]);
       db.exec("BEGIN IMMEDIATE");
       process.once("message", () => {
         db.exec("ROLLBACK");
         db.close();
         process.disconnect();
       });
       process.send("locked");`,
      path,
    ],
    { stdio: ["ignore", "ignore", "ignore", "ipc"] },
  );
  const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
  try {
    await new Promise<void>((resolve, reject) => {
      child.once("error", reject);
      child.once("exit", (code) => reject(new Error(`SQLite lock process exited: ${code}`)));
      child.once("message", (message) => {
        if (message === "locked") resolve();
        else reject(new Error("SQLite lock process did not acquire its lock"));
      });
    });
  } catch (error) {
    child.kill();
    await exited;
    throw error;
  }
  return async () => {
    if (child.connected) child.send("release");
    await exited;
  };
}

it.each(["binding", "adapter"] as const)(
  "retries a failed SQLite close through the %s while keeping writes fenced",
  async (layer) => {
    const { NapiDb } = await loadNapiModule();
    const root = await mkdtemp(join(tmpdir(), "jazz-native-close-retry-"));
    const schema = {
      files: { columns: [{ name: "body", column_type: { type: "Text" }, nullable: false }] },
    } satisfies WasmSchema;
    const encodedSchema = encodeSchema(schema);
    const node = new Uint8Array(16).fill(0x71);
    const author = testAuthorBytes("close-retry");
    const config = openConfig(node, author, 1, false);
    const owner = NapiDb.openPersistent(root, encodedSchema, config);
    const runtime =
      layer === "adapter"
        ? NativeRuntimeAdapter.fromDb(owner as never, schema, node, author, 1, false)
        : undefined;
    const close = () => (runtime ? runtime.close() : owner.close());
    let unlock: (() => Promise<void>) | undefined;
    let reopened: InstanceType<typeof NapiDb> | undefined;
    try {
      const publishedId = new Uint8Array(16).fill(0x31);
      const uploadId = new Uint8Array(16).fill(0x32);
      const emptyCells = encodedCells([], []);
      const published = owner.beginStreamingMutation("files", publishedId, emptyCells, "body");
      published.push(new TextEncoder().encode("committed file"));
      const receipt = published.finish();
      await receipt.wait("local");
      const original = owner.localCurrentRow("files", publishedId);
      expect(
        rowsFromBatches(new PostcardReader(original).readVec(readNativeRowBatch), schema),
      ).toMatchObject([{ values: [{ type: "Text", value: "committed file" }] }]);
      const abandoned = owner.beginStreamingMutation("files", uploadId, emptyCells, "body");
      abandoned.push(new TextEncoder().encode("unpublished".repeat(8_000)));
      unlock = await lockSQLite(join(root, "jazz.sqlite"));
      await expect(close()).rejects.toThrow(/database is locked/);
      expect(() => NapiDb.openPersistent(root, encodedSchema, config)).toThrow(/already open/);
      expect(() => abandoned.push(new Uint8Array([1]))).toThrow(/closed/);
      expect(() => owner.beginStreamingMutation("files", uploadId, emptyCells, "body")).toThrow(
        /closed/,
      );
      expect(() =>
        owner.insert(
          "files",
          encodeCellsForRow(schema.files, {
            body: { type: "Text", value: "late" },
          }),
        ),
      ).toThrow(/closed|closing/);
      expect(() =>
        owner.beginTransaction("019a0f00000070008000000000000001", "serializable"),
      ).toThrow(/closed|closing/);
      if (runtime) {
        expect(() => runtime.insert("files", { body: { type: "Text", value: "late" } })).toThrow(
          /closed/,
        );
      }
      await unlock();
      unlock = undefined;
      expect(() => owner.tick()).not.toThrow();
      await receipt.wait("local");
      await close();
      reopened = NapiDb.openPersistent(root, encodedSchema, config);
      expect(reopened.localCurrentRow("files", publishedId)).toEqual(original);
      expect(
        rowsFromBatches(
          new PostcardReader(reopened.localCurrentRow("files", uploadId)).readVec(
            readNativeRowBatch,
          ),
          schema,
        ),
      ).toEqual([]);
    } finally {
      await unlock?.();
      await reopened?.close();
      await runtime?.close();
      await owner.close();
      await rm(root, { recursive: true, force: true });
    }
  },
  30_000,
);
