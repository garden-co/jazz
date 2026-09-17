import { describe, expect, it } from "vitest";
import { schema as s } from "../../index.js";
import { createOpenTransactionId } from "../client.js";
import { testAuthorBytes } from "../testing/account-fixtures.js";
import { loadWasmModuleForTest } from "../testing/wasm-runtime-test-utils.js";
import { openConfig, queryFromTable, queryWithPredicates } from "./native-codec.js";
import { encodeSchema } from "./schema-codec.js";

const app = s.defineApp({
  folders: s.table({ title: s.string() }, { notesViaFolder: s.reverse("notes", "folder") }),
  notes: s.table(
    { text: s.string(), folderId: s.uuid() },
    { folder: s.rel("folders", "folderId") },
  ),
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
          testAuthorBytes(`wasm-backend-read-contract:${backend ? "backend" : "ordinary"}`),
          1,
          true,
        ),
      );
      const query = queryFromTable("notes");
      const relation = queryWithPredicates("notes", [], {
        relation: {
          Project: {
            input: { TableScan: { table: "notes" } },
            columns: [{ alias: "text", expr: { Column: { scope: "notes", column: "text" } } }],
          },
        },
      });
      const opts = { tier: "local" };
      const txId = createOpenTransactionId();
      db.beginTransaction(txId, "mergeable");
      const reads = [
        () => db.all(query, opts),
        () => db.all(query, opts, txId),
        () => db.all(relation, opts),
      ];
      try {
        for (const read of reads)
          expect(await resolveRead(read(), () => db.tick())).toBeInstanceOf(Uint8Array);
        await db.subscribe(query, opts).cancel();
        await db.subscribe(relation, opts).cancel();
      } finally {
        db.rollbackTransaction(txId);
        db.close();
      }
    });
  }
});

it("lets host timers complete a pending read even when every tick completes immediately", async () => {
  let ready = false;
  let polls = 0;
  const bytes = Uint8Array.of(1);
  const timer = setTimeout(() => {
    ready = true;
  }, 0);
  try {
    const read = {
      poll() {
        if (++polls > 20) throw new Error("Read polling starved the host timer");
        return ready ? bytes : null;
      },
    };
    expect(await resolveRead(read, () => undefined)).toBe(bytes);
  } finally {
    clearTimeout(timer);
  }
});

it("reports host tick failures while a read remains pending", async () => {
  const error = new Error("Host tick failed");
  await expect(resolveRead({ poll: () => null }, () => Promise.reject(error))).rejects.toBe(error);
});

async function resolveRead(
  read: Uint8Array | { poll(): Uint8Array | null },
  serviceTick: () => unknown,
): Promise<Uint8Array> {
  if (read instanceof Uint8Array) return read;
  let tick: Promise<unknown> | undefined;
  let failure: { error: unknown } | undefined;
  for (;;) {
    const result = read.poll();
    if (result !== null) await tick;
    if (failure) throw failure.error;
    if (result !== null) return result;
    // Raw bindings need a host tick, but the read must keep polling while it runs.
    tick ??= Promise.resolve(serviceTick())
      .catch((error: unknown) => {
        failure = { error };
      })
      .finally(() => {
        tick = undefined;
      });
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
}
