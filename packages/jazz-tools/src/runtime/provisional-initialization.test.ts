import { expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { resolveSchemaSource } from "../schema-source.js";
import { JazzClient } from "./client.js";
import { translateQuery } from "./query-adapter.js";
import { createNapiNativeRuntimeAdapter } from "./testing/napi-runtime-test-utils.js";
import { allowAll } from "./testing/allow-all.js";
import { decodeInitializationStatuses, type ReservedTxId } from "./provisional-initialization.js";

const app = s.defineApp({ notes: s.table({ text: s.string() }, {}) });
const schema = resolveSchemaSource(app);
async function client() {
  const runtime = await createNapiNativeRuntimeAdapter(schema, allowAll(app), {
    peerId: crypto.randomUUID(),
  });
  return JazzClient.connectWithRuntime(runtime, {
    appId: "initialization-test",
    schema,
    driver: { type: "memory" },
    tier: "local",
  });
}
const query = translateQuery(app.notes._build(), schema);

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

it("drains preparation, seals without publication, then publishes the exact reserved unit once", async () => {
  const owner = await client();
  const foreign = await client();
  const id = owner.beginTransaction("exclusive");
  const rowId = crypto.randomUUID();
  const entered = deferred();
  const release = deferred();
  void owner.prepareTransaction(id, async (io) => {
    entered.resolve();
    await release.promise;
    await io.recordInitializationInsertAbsence("notes", rowId);
    io.insertInternal("notes", { text: { type: "Text", value: "prepared once" } }, { id: rowId });
  });
  const sealing = owner.sealInitializationTransaction(id);
  await entered.promise;
  expect(await owner.queryInternal(query, { tier: "local" })).toEqual([]);
  release.resolve();
  const seal = await sealing;
  expect(await owner.queryInternal(query, { tier: "local" })).toEqual([]);
  expect(await owner.initializationTransactionStatus([seal.reservedTxId])).toEqual([
    { kind: "not-observed", reservedTxId: seal.reservedTxId },
  ]);
  await expect(foreign.publishInitializationTransaction(seal)).rejects.toThrow(/foreign/);
  const published = await owner.publishInitializationTransaction(seal);
  await owner.waitForTransaction(published, "local");
  expect(await owner.initializationTransactionStatus([seal.reservedTxId])).toEqual([
    {
      kind: "complete",
      reservedTxId: seal.reservedTxId,
      fate: { kind: "pending" },
      durability: "local",
    },
  ]);
  expect(await owner.queryInternal(query, { tier: "local" })).toEqual([
    expect.objectContaining({ id: rowId, values: [{ type: "Text", value: "prepared once" }] }),
  ]);
  await expect(owner.publishInitializationTransaction(seal)).rejects.toThrow(/consumed/);
  await expect(
    owner.initializationTransactionStatus(Array(65).fill(seal.reservedTxId)),
  ).rejects.toThrow(/64/);
});

it.each(["cancel", "rollback"])(
  "abandons a sealed unit via %s without exposing rows or reusing its reservation",
  async (action) => {
    const owner = await client();
    const id = owner.beginTransaction("exclusive");
    await owner.prepareTransaction(id, async (io) => {
      const row = crypto.randomUUID();
      await io.recordInitializationInsertAbsence("notes", row);
      io.insertInternal(
        "notes",
        { text: { type: "Text", value: "must not publish" } },
        { id: row },
      );
    });
    const seal = await owner.sealInitializationTransaction(id);
    if (action === "cancel") await owner.cancelInitializationTransaction(seal);
    else await owner.rollbackTransaction(id);
    await expect(owner.publishInitializationTransaction(seal)).rejects.toThrow(/consumed/);
    expect(await owner.queryInternal(query, { tier: "local" })).toEqual([]);
    const second = await owner.sealInitializationTransaction(owner.beginTransaction("exclusive"));
    expect(second.reservedTxId).not.toBe(seal.reservedTxId);
    await owner.cancelInitializationTransaction(second);
  },
);

it("failed preparation cannot publish a partial initialization unit", async () => {
  const owner = await client();
  const id = owner.beginTransaction("exclusive");
  void owner.prepareTransaction(id, async (io) => {
    io.insertInternal("notes", { text: { type: "Text", value: "rolled back" } });
    throw new Error("durable preparation failed");
  });
  await expect(owner.sealInitializationTransaction(id)).rejects.toThrow(
    "durable preparation failed",
  );
  expect(await owner.queryInternal(query, { tier: "local" })).toEqual([]);
});

it("refuses corrupt recovery status instead of treating it as missing or durable", () => {
  const ids = ["original-reservation" as ReservedTxId];
  expect(() => decodeInitializationStatuses('{"version":2,"statuses":[]}', ids)).toThrow();
  expect(() =>
    decodeInitializationStatuses(
      '{"version":1,"statuses":[{"kind":"not-observed","reservedTxId":"foreign"}]}',
      ids,
    ),
  ).toThrow(/identity/);
  expect(() =>
    decodeInitializationStatuses(
      '{"version":1,"statuses":[{"kind":"complete","reservedTxId":"original-reservation","fate":{"kind":"pending"},"durability":"invented"}]}',
      ids,
    ),
  ).toThrow(/status/);
  expect(
    decodeInitializationStatuses(
      '{"version":1,"statuses":[{"kind":"incomplete","reservedTxId":"original-reservation"}]}',
      ids,
    ),
  ).toEqual([{ kind: "incomplete", reservedTxId: ids[0] }]);
});
