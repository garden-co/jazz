import { expect, it, onTestFinished } from "vitest";
import { schema as s } from "../schema-namespace.js";
import {
  encodeCellsForPatch,
  encodeCellsForRow,
  encodeSchema,
  rowsFromBatches,
} from "./native-runtime/native-runtime-adapter.js";
import { openConfig, PostcardReader, readNativeRowBatch } from "./native-runtime/native-codec.js";
import { testAuthorBytes } from "./testing/account-fixtures.js";
import { formatUuid } from "./uuid.js";
import { hasJazzWasmBuild, loadWasmModuleForTest } from "./testing/wasm-runtime-test-utils.js";

const app = s.defineApp({ values: s.table({ text: s.string() }, {}) });

it.skipIf(!hasJazzWasmBuild()).each(["absence", "seal"] as const)(
  "publishes raw Memory initialization after an unrelated failed commit before %s",
  async (operation) => {
    const wasm = await loadWasmModuleForTest();
    const node = new Uint8Array(16);
    node[0] = 81;
    const author = testAuthorBytes(`initialization-ready-predecessor:${operation}`);
    const db = wasm.WasmDb.openMemory(encodeSchema(app.wasmSchema), openConfig(node, author, 51));
    onTestFinished(async () => {
      await db.close();
    });
    const open = crypto.randomUUID().replaceAll("-", "");
    const absent = new Uint8Array(16);
    absent[0] = 52;
    const failed = crypto.randomUUID().replaceAll("-", "");
    db.beginTransaction(failed, "exclusive");
    db.updateInTransaction(
      failed,
      "missing_table",
      absent,
      encodeCellsForPatch(app.wasmSchema.values!, { text: { type: "Text", value: "invalid" } }),
    );
    const failedWrite = db.commitTransaction(failed, "exclusive");
    db.beginTransaction(open, "exclusive");
    // No scheduler or explicit tick may rescue the awaited initialization.
    if (operation === "absence") await db.recordInitializationInsertAbsence(open, "values", absent);
    const row = db.insertInTransaction(
      open,
      "values",
      encodeCellsForRow(app.wasmSchema.values!, {
        text: { type: "Text", value: "ready predecessor preserves this payload" },
      }),
    );
    const seal = await db.sealInitializationTransaction(open);
    await expect(failedWrite.wait("local")).rejects.toThrow(/missing_table|unknown table/i);
    await db.publishInitializationTransaction(seal.token);
    await db.tick();

    const batches = new PostcardReader(db.localCurrentRow("values", row)).readVec(
      readNativeRowBatch,
    );
    expect(rowsFromBatches(batches, app.wasmSchema)).toEqual([
      {
        table: "values",
        id: formatUuid(row),
        values: [{ type: "Text", value: "ready predecessor preserves this payload" }],
      },
    ]);
  },
  5_000,
);
