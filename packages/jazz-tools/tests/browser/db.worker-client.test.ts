import { afterEach, expect, it } from "vitest";
import { schema as s, type Db } from "../../src/index.js";
import { createAccountDbWithRuntimeSource } from "../../src/accounts/context.js";
import { localAccountConfig } from "../../src/runtime/testing/account-fixtures.js";
import { DefaultRuntimeSource } from "../../src/runtime/default-runtime-source.js";
import { WorkerClientRuntimeSource } from "../../src/runtime/worker-client-runtime-source.js";
import { deploy } from "../../src/dev/catalogue.js";
import { getJazzServerInfo } from "./testing-server.js";
import { withTimeout } from "./support.js";

const app = s.defineApp({
  documents: s.table({ label: s.string(), rank: s.int(), payload: s.bytes() }, {}),
});
const permissions = s.definePermissions(app, ({ policy }) => [
  policy.documents.allowRead.always(),
  policy.documents.allowInsert.always(),
  policy.documents.allowUpdate.always(),
  policy.documents.allowDelete.always(),
]);
const open = new Set<Db>();
async function close(db: Db): Promise<void> {
  open.delete(db);
  await db.shutdown();
}
afterEach(async () => {
  const results = await Promise.allSettled([...open].map((db) => db.shutdown()));
  open.clear();
  for (const result of results) if (result.status === "rejected") throw result.reason;
});

it("opens a legacy root, shares one client between ports and retains exact blob bytes after reopen", async () => {
  const account = await localAccountConfig(crypto.randomUUID());
  const config = {
    ...account,
    driver: { type: "persistent" as const, dbName: `worker-client-${crypto.randomUUID()}` },
  };
  const create = async (source: DefaultRuntimeSource = new WorkerClientRuntimeSource()) => {
    const db = await createAccountDbWithRuntimeSource(config, source);
    open.add(db);
    return db;
  };
  const legacy = await create(new DefaultRuntimeSource());
  const payload = Uint8Array.from({ length: 96 * 1024 }, (_, index) => index % 251);
  const first = legacy.insert(app.documents, { label: "before", rank: 2, payload });
  await first.wait({ tier: "local" });
  await close(legacy);

  const alice = await create();
  expect(await alice.all(app.documents, { tier: "local" })).toEqual([first.value]);
  const sibling = await create();
  const snapshots: string[][] = [];
  const errors: Error[] = [];
  const stop = alice.subscribe(
    app.documents.orderBy("rank", "asc"),
    {
      onUpdate: (rows) => snapshots.push(rows.map((row) => row.label)),
      onError: (error) => errors.push(error),
    },
    { tier: "local" },
  );
  try {
    await expect.poll(() => snapshots.at(-1)).toEqual(["before"]);
    const tx = sibling.beginTransaction();
    tx.insert(app.documents, { label: "after", rank: 1, payload: new Uint8Array([1, 2, 3]) });
    expect((await tx.all(app.documents, { tier: "local" })).length).toBe(2);
    expect(await alice.all(app.documents, { tier: "local" })).toEqual([first.value]);
    await tx.commit().wait({ tier: "local" });
    await expect.poll(() => snapshots.at(-1)).toEqual(["after", "before"]);
    expect(errors).toEqual([]);
  } finally {
    stop();
  }
  await close(sibling);
  await close(alice);
  const reopened = await create();
  expect(
    (await reopened.all(app.documents.orderBy("rank", "asc"), { tier: "local" })).map(
      (row) => row.label,
    ),
  ).toEqual(["after", "before"]);
  expect(
    (await reopened.one(app.documents.where({ id: first.value.id }), { tier: "local" }))?.payload,
  ).toEqual(payload);
}, 60_000);

it("rejects mixed worker roles without retiring the existing root owner", async () => {
  const account = await localAccountConfig(crypto.randomUUID());
  const config = {
    ...account,
    driver: { type: "persistent" as const, dbName: `worker-mode-${crypto.randomUUID()}` },
  };
  const legacy = await createAccountDbWithRuntimeSource(config, new DefaultRuntimeSource());
  open.add(legacy);
  const row = legacy.insert(app.documents, {
    label: "owner",
    rank: 0,
    payload: new Uint8Array([7]),
  });
  await row.wait({ tier: "local" });
  const candidate = await createAccountDbWithRuntimeSource(config, new WorkerClientRuntimeSource());
  open.add(candidate);
  await expect(candidate.all(app.documents, { tier: "local" })).rejects.toThrow(
    "incompatible persistent browser configuration",
  );
  await close(candidate);
  expect(await legacy.all(app.documents, { tier: "local" })).toEqual([row.value]);
}, 60_000);

it("uploads pending writes from former tab nodes and keeps resident reads live while disconnected", async () => {
  const server = await getJazzServerInfo(crypto.randomUUID());
  await deploy({ ...server, schema: app.wasmSchema, permissions });
  const account = await localAccountConfig(server.appId, server.serverUrl);
  const config = {
    ...account,
    driver: { type: "persistent" as const, dbName: `worker-recovery-${crypto.randomUUID()}` },
  };
  const legacy = await createAccountDbWithRuntimeSource(config, new DefaultRuntimeSource());
  open.add(legacy);
  expect(await legacy.all(app.documents, { tier: "global" })).toEqual([]);
  await legacy.disconnect();
  const payload = Uint8Array.from({ length: 32 * 1024 }, (_, index) => index % 239);
  const pending = legacy.insert(app.documents, { label: "pending upload", rank: 1, payload });
  await pending.wait({ tier: "local" });
  await close(legacy);

  const alice = await createAccountDbWithRuntimeSource(config, new WorkerClientRuntimeSource());
  open.add(alice);
  expect(await alice.all(app.documents, { tier: "global" })).toEqual([pending.value]);
  // Independent in-memory reader proves that recovery reached the server.
  const observer = await createAccountDbWithRuntimeSource(account, new DefaultRuntimeSource());
  open.add(observer);
  expect(await observer.all(app.documents, { tier: "global" })).toEqual([pending.value]);

  await alice.disconnect();
  let remoteSettled = false;
  const remote = alice.all(app.documents, { tier: "global" }).finally(() => {
    remoteSettled = true;
  });
  try {
    expect(
      await withTimeout(
        alice.all(app.documents, { tier: "local" }),
        5000,
        "resident read behind disconnected global read",
      ),
    ).toEqual([pending.value]);
    expect(remoteSettled).toBe(false);
  } finally {
    await alice.reconnect();
  }
  expect(await remote).toEqual([pending.value]);
}, 90_000);
