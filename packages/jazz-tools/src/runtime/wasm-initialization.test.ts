import { expect, it, onTestFinished } from "vitest";
import { schema as s } from "../schema-namespace.js";
import {
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
    // Admit the offline replica before the seed so its writes have upload
    // identities, as required by the initialization reservation path.
    db.setRemoteLinkHint("none");
    onTestFinished(async () => {
      await db.close();
    });
    const open = crypto.randomUUID().replaceAll("-", "");
    const absent = new Uint8Array(16);
    absent[0] = 52;
    // SQLite rejects unknown tables at staging. A duplicate live ID is a
    // real apply-time refusal, retained until commit and reported by its
    // write receipt. Finish the seed before the no-scheduler critical path.
    const seed = db.insert(
      "values",
      encodeCellsForRow(app.wasmSchema.values!, {
        text: { type: "Text", value: "existing row" },
      }),
    );
    const duplicate = seed.rowId;
    await seed.wait("local");
    seed.close();
    const failed = crypto.randomUUID().replaceAll("-", "");
    db.beginTransaction(failed, "serializable");
    db.insertInTransaction(
      failed,
      "values",
      encodeCellsForRow(app.wasmSchema.values!, {
        text: { type: "Text", value: "must not replace existing row" },
      }),
      { rowId: duplicate },
    );
    const failedWrite = db.commitTransaction(failed, "serializable");
    db.beginTransaction(open, "serializable");
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
    const failure = expect(failedWrite.wait("local")).rejects.toThrow(/object already exists/i);
    await db.publishInitializationTransaction(seal.token);
    await db.tick();
    await failure;
    failedWrite.close();
    const original = new PostcardReader(db.localCurrentRow("values", duplicate)).readVec(
      readNativeRowBatch,
    );
    expect(rowsFromBatches(original, app.wasmSchema)).toEqual([
      {
        table: "values",
        id: formatUuid(duplicate),
        values: [{ type: "Text", value: "existing row" }],
      },
    ]);

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
