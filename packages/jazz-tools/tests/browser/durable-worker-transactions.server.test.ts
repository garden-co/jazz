/// <reference types="vite/client" />
import { afterEach, expect, it } from "vitest";
import { ReadTier, schema, type Db } from "../../src/index.js";
import { beginDbTransactionAfter } from "../../src/runtime/db.js";
import { deploy } from "../../src/dev/catalogue.js";
import { NativeRuntimeAdapter } from "../../src/runtime/native-runtime/native-runtime-adapter.js";
import { createOpenTransactionId, type OpenTransactionId } from "../../src/runtime/client.js";
import {
  TestCleanup,
  acquireBrowserTestAccount,
  createBrowserTestDb,
  uniqueDbName,
  withTimeout,
} from "./support.js";
import {
  createJazzServerTransportControl,
  getJazzServerInfo,
  startJazzApp,
} from "./testing-server.js";

const app = schema.defineApp(
  { notes: schema.table({ body: schema.string() }, {}) },
  { migrations: "none" },
);
const permissions = schema.definePermissions(app, ({ policy }) => [
  policy.notes.allowRead.always(),
  policy.notes.allowInsert.always(),
  policy.notes.allowUpdate.always(),
]);
const ctx = new TestCleanup();
afterEach(async () => {
  await withTimeout(ctx.cleanup(), 10_000, "durable owner cleanup did not finish");
});
// Produced by the normal declared bundler from this candidate and its admitted
// NAPI/WASM pair. This path is inside the runner's existing repository root.
const brokerWorkerUrl = new URL(
  "../../target/durable-worker-sdk-qualification/jazz-broker-worker.js",
  import.meta.url,
).href;

async function workerRuntimeSources() {
  const wasmUrl = new URL("jazz_wasm_bg.wasm", brokerWorkerUrl).href;
  const hashes = await Promise.all(
    [brokerWorkerUrl, wasmUrl].map(async (url) => {
      const response = await fetch(url);
      if (!response.ok) throw new Error(`Missing normal worker qualification asset: ${url}`);
      const digest = await crypto.subtle.digest("SHA-256", await response.arrayBuffer());
      return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join(
        "",
      );
    }),
  );
  return { brokerWorkerUrl, wasmUrl, wasmVersion: hashes.join(":") };
}

async function setup(label: string) {
  // Bind the actual normal producer outputs, including after a source correction.
  const runtimeSources = await workerRuntimeSources();
  const core = await getJazzServerInfo(uniqueDbName(label));
  await deploy({ ...core, schema: app.wasmSchema, permissions });
  const server = await startJazzApp(core, { schema: app.wasmSchema, permissions });
  const control = ctx.trackTransport(await createJazzServerTransportControl(server.serverUrl));
  const account = await acquireBrowserTestAccount({
    appId: server.appId,
    serverUrl: control.url,
    key: label,
  });
  const dbName = uniqueDbName(`${label}-owner`);
  const reopen = async () =>
    ctx.track(
      await createBrowserTestDb({
        appId: server.appId,
        serverUrl: control.url,
        account,
        driver: { type: "persistent", dbName },
        runtimeSources,
      }),
    );
  const tab = await reopen();
  // Normal application/schema setup precedes BEGIN. No read is replayed or
  // warmed outside an already-open transaction to obtain an authority answer.
  await tab.all(app.notes, { tier: ReadTier.Remote });
  const direct = ctx.track(
    await createBrowserTestDb({
      appId: server.appId,
      serverUrl: server.serverUrl,
      account: await acquireBrowserTestAccount({
        appId: server.appId,
        serverUrl: server.serverUrl,
        key: `${label}-other`,
      }),
      driver: { type: "memory" },
    }),
  );
  await direct.all(app.notes, { tier: ReadTier.Remote });
  return { tab, direct, control, reopen };
}
function native(db: Db): NativeRuntimeAdapter {
  const runtime = db.currentRuntime();
  if (!(runtime instanceof NativeRuntimeAdapter))
    throw new Error("expected actual browser native adapter");
  return runtime;
}
async function begin(runtime: NativeRuntimeAdapter): Promise<OpenTransactionId> {
  const id = createOpenTransactionId();
  const opening = runtime.beginTransactionOnDurableOwner(
    "serializable",
    id,
    undefined,
    "client-accepted-evidence",
  );
  if (!opening) throw new Error("the real durable worker was not selected before BEGIN");
  await opening;
  return id;
}
async function insert(runtime: NativeRuntimeAdapter, tx: OpenTransactionId, body: string) {
  const id = crypto.randomUUID();
  await runtime.recordInitializationInsertAbsence(tx, app.notes._table, id);
  runtime.insert(
    app.notes._table,
    { body: { type: "Text", value: body } },
    JSON.stringify({ transaction_id: tx }),
    id,
  );
  return id;
}
function rows(runtime: NativeRuntimeAdapter, tx: OpenTransactionId) {
  return runtime.query(
    JSON.stringify({ table: app.notes._table }),
    undefined,
    "local-first",
    JSON.stringify({ transaction_id: tx }),
  );
}

it("seals and publishes the actual durable worker batch with its original ID and genuine Global outcome", async () => {
  const { tab, direct } = await setup("durable-worker-publish");
  const runtime = native(tab);
  const tx = await begin(runtime);
  const id = await insert(runtime, tx, "worker founder");
  expect(await rows(runtime, tx)).toEqual(
    expect.arrayContaining([expect.objectContaining({ id })]),
  );
  const seal = await runtime.sealInitializationTransaction(tx);
  const published = await runtime.publishInitializationTransaction(seal);
  expect(published).toBe(seal.reservedTxId);
  await runtime.waitForTransaction(published, "local");
  await withTimeout(
    runtime.waitForTransaction(published, "global"),
    15_000,
    "actual worker publication did not accept",
  );
  expect(await direct.all(app.notes.where({ id }), { tier: ReadTier.Remote })).toEqual([
    { id, body: "worker founder" },
  ]);
});

it("refuses a foreign live client seal and cancels the original batch without publishing rows", async () => {
  const { tab, direct } = await setup("durable-worker-foreign");
  const runtime = native(tab);
  const tx = await begin(runtime);
  const id = await insert(runtime, tx, "must stay private");
  const seal = await runtime.sealInitializationTransaction(tx);
  await expect(native(direct).publishInitializationTransaction(seal)).rejects.toThrow(
    /foreign|consumed/,
  );
  await runtime.cancelInitializationTransaction(seal);
  expect(await direct.all(app.notes.where({ id }), { tier: ReadTier.Remote })).toEqual([]);
});

it("refuses an explicit wrong actor and claims on the admitted owner's original batch", async () => {
  const { tab, direct } = await setup("durable-worker-actor");
  const runtime = native(tab);
  const tx = await begin(runtime);
  const id = crypto.randomUUID();
  runtime.insert(
    app.notes._table,
    { body: { type: "Text", value: "forbidden actor" } },
    JSON.stringify({
      transaction_id: tx,
      session: {
        issuer: "https://wrong-issuer.example",
        user_id: "mallory",
        claims: { role: "owner" },
      },
    }),
    id,
  );
  await expect(runtime.sealInitializationTransaction(tx)).rejects.toThrow(
    /differs from the admitted durable owner/,
  );
  await runtime.rollbackTransaction(tx);
  expect(await direct.all(app.notes.where({ id }), { tier: ReadTier.Remote })).toEqual([]);
});

it("retains the actual accepted-read refusal after staged own content was exposed", async () => {
  const { tab, direct } = await setup("durable-worker-accepted-refusal");
  const runtime = native(tab);
  const tx = await begin(runtime);
  const id = await insert(runtime, tx, "unaccepted own content");
  expect(await rows(runtime, tx)).toEqual(
    expect.arrayContaining([expect.objectContaining({ id })]),
  );
  await expect(
    runtime.queryAccepted(JSON.stringify({ table: app.notes._table }), tx, undefined, 1000),
  ).rejects.toThrow(/accepted reads cannot substitute already staged content/);
  await runtime.rollbackTransaction(tx);
  expect(await direct.all(app.notes.where({ id }), { tier: ReadTier.Remote })).toEqual([]);
});

it("persists a Local pending publication across an offline owner reopen and later obtains real Global acceptance", async () => {
  const { tab, direct, control, reopen } = await setup("durable-worker-offline-reopen");
  await control.block();
  try {
    const runtime = native(tab);
    const tx = await begin(runtime);
    const id = await insert(runtime, tx, "offline original");
    const seal = await runtime.sealInitializationTransaction(tx);
    const published = await runtime.publishInitializationTransaction(seal);
    expect(published).toBe(seal.reservedTxId);
    await withTimeout(
      runtime.waitForTransaction(published, "local"),
      10_000,
      "Local worker persistence did not complete",
    );
    await tab.shutdown({ waitForSync: false });
    ctx.untrack(tab);
    const restored = await reopen();
    expect(await restored.all(app.notes.where({ id }), { tier: ReadTier.LocalFirst })).toEqual([
      { id, body: "offline original" },
    ]);
    const before = await native(restored).initializationTransactionStatus([seal.reservedTxId]);
    expect(before).toEqual([
      {
        kind: "complete",
        reservedTxId: seal.reservedTxId,
        fate: { kind: "pending" },
        durability: "local",
      },
    ]);
    await control.unblock();
    await withTimeout(
      restored.waitForSync(),
      15_000,
      "reopened original publication did not accept",
    );
    expect(await direct.all(app.notes.where({ id }), { tier: ReadTier.Remote })).toEqual([
      { id, body: "offline original" },
    ]);
    // waitForSync explicitly excludes writes recovered by the shared worker.
    // Wait boundedly on this exact journal identity for actual accepted fate.
    await expect
      .poll(
        async () =>
          (await native(restored).initializationTransactionStatus([seal.reservedTxId]))[0],
        { timeout: 15_000 },
      )
      .toMatchObject({ kind: "complete", fate: { kind: "accepted" }, durability: "global" });
    const after = await native(restored).initializationTransactionStatus([seal.reservedTxId]);
    expect(after).toEqual([
      {
        kind: "complete",
        reservedTxId: seal.reservedTxId,
        fate: { kind: "accepted" },
        durability: "global",
      },
    ]);
  } finally {
    await control.unblock();
  }
});

it("preserves the typed transaction's real snapshot/read-set conflict across the durable owner", async () => {
  const { tab, direct } = await setup("durable-worker-readset");
  const row = await direct.insert(app.notes, { body: "before begin" }).wait({ tier: "global" });
  await tab.all(app.notes.where({ id: row.id }), { tier: ReadTier.Remote });
  const tx = beginDbTransactionAfter(tab, async () => {}, "client-accepted-evidence");
  expect(await tx.all(app.notes.where({ id: row.id }))).toEqual([
    { id: row.id, body: "before begin" },
  ]);
  await direct
    .update(app.notes, row.id, { body: "real concurrent writer" })
    .wait({ tier: "global" });
  tx.update(app.notes, row.id, { body: "must be rejected" });
  await expect(tx.commit().wait({ tier: "global", timeoutMs: 15_000 })).rejects.toThrow(
    /(?:transaction|serialization|exclusive)[_: ]?conflict/i,
  );
  expect(await direct.all(app.notes.where({ id: row.id }), { tier: ReadTier.Remote })).toEqual([
    { id: row.id, body: "real concurrent writer" },
  ]);
});
