import { describe, expect, it } from "vitest";
import { schema as s } from "../../index.js";
import { createOpenTransactionId } from "../client.js";
import { loadWasmModuleForTest } from "../testing/wasm-runtime-test-utils.js";
import { openConfig, queryFromTable } from "./native-codec.js";
import { encodeSchema } from "./schema-codec.js";
import { translateQuery } from "../query-adapter.js";

const app = s.defineApp({
  folders: s.table({ title: s.string() }),
  notes: s.table({ text: s.string(), folderId: s.ref("folders") }),
});

describe("WASM backend read capability parity", () => {
  for (const backend of [false, true]) {
    it(`uses the consolidated read surface after ${backend ? "backend" : "ordinary"} open`, async () => {
      const { WasmDb } = await loadWasmModuleForTest();
      const open = backend ? WasmDb.openMemoryAsBackend : WasmDb.openMemory;
      const db = open(
        encodeSchema(app.wasmSchema),
        openConfig(
          new Uint8Array(16).fill(backend ? 1 : 2),
          new TextEncoder().encode('["https://issuer.example","reader"]'),
          1,
          true,
        ),
      );
      const query = db.prepareQuery(queryFromTable("notes"));
      const relation = translateQuery(app.notes.where({}).hopTo("folder")._build(), app.wasmSchema);
      const opts = { tier: "local" };
      const txId = createOpenTransactionId();
      db.beginTransaction(txId, "mergeable");
      const reads = [
        () => db.allAsync(query, opts),
        () => db.allAsync(query, opts, txId),
        () => db.allRelationSnapshot(query, opts),
        () => db.allRelationSnapshot(query, opts, txId),
        () => db.allRelationQuery(relation, opts),
      ];
      try {
        const attachment = db.attachQuery(query, opts);
        db.detachQuery(attachment);
        for (const read of reads) expect(await read()).toBeInstanceOf(Uint8Array);
        if (backend) {
          await db.subscribeForBackend(query, opts).cancel();
          await db.subscribeRelationQueryForBackend(relation, opts).cancel();
        } else {
          for (const read of [
            () => db.subscribeForBackend(query, opts),
            () => db.subscribeRelationQueryForBackend(relation, opts),
          ])
            expect(read).toThrow(/explicit backend runtime/);
        }
      } finally {
        db.rollbackTransaction(txId);
        db.close();
      }
    });
  }
});
