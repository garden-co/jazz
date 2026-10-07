import { expect, it } from "vitest";
import { schema as s } from "../../index.js";
import { createOpenTransactionId } from "../client.js";
import { testAuthorBytes } from "../testing/account-fixtures.js";
import { loadWasmModuleForTest } from "../testing/wasm-runtime-test-utils.js";
import { openConfig, queryFromTable } from "./native-codec.js";
import { encodeSchema } from "./schema-codec.js";

// Internal binding contract: the public query API does not expose settlements.
it("returns rows and settlement evidence together from one native read", async () => {
  const app = s.defineApp({ notes: s.table({ title: s.string() }, {}) });
  const { WasmDb } = await loadWasmModuleForTest();
  const db = WasmDb.openMemory(
    encodeSchema(app.wasmSchema),
    openConfig(new Uint8Array(16).fill(4), testAuthorBytes("settled-rows"), 1, true),
  );
  const transaction = createOpenTransactionId();
  try {
    db.beginTransaction(transaction, "exclusive");
    const reading = db.allSettlementMetadata(
      queryFromTable("notes"),
      { tier: "global", propagation: "local_only" },
      transaction,
      undefined,
      undefined,
      true,
    );
    let bytes = reading instanceof Uint8Array ? reading : null;
    for (let attempt = 0; bytes === null && attempt < 10; attempt++) {
      await db.tick();
      if (!(reading instanceof Uint8Array)) bytes = reading.poll();
    }
    expect(bytes).toBeInstanceOf(Uint8Array);
    // Settled-row binding frame v1, pinned independently of JSON serialization
    // defaults: u32 LE length 2, UTF-8 "[]", then the empty existing row batch.
    expect([...bytes!]).toEqual([0x02, 0x00, 0x00, 0x00, 0x5b, 0x5d, 0x00]);
  } finally {
    db.rollbackTransaction(transaction);
    await db.tick();
    db.close();
  }
});
