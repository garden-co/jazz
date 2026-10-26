/**
 * Serializable transactions from a tab whose durable replica lives in its
 * SharedWorker: the worker relays the commit to the server, which validates
 * it against its own state. Every tab here is a persistent one
 * (tab -> SharedWorker -> server); a memory client writes to the server
 * directly as the concurrent writer.
 */

import { afterEach, expect, it } from "vitest";
import { ReadTier, generateAuthSecret, schema, type Db } from "../../src/index.js";
import { deploy } from "../../src/dev/catalogue.js";
import {
  TestCleanup,
  createBrowserTestDb,
  uniqueDbName,
  waitForCondition,
  withTimeout,
} from "./support.js";
import { getJazzServerInfo } from "./testing-server.js";

const app = schema.defineApp({
  seats: schema.table({ show: schema.string(), holder: schema.string() }, {}),
  audit: schema.table({ note: schema.string() }, {}),
});

const permissions = schema.definePermissions(app, ({ policy }) => [
  policy.seats.allowRead.always(),
  policy.seats.allowInsert.always(),
  policy.seats.allowUpdate.always(),
  policy.audit.allowRead.always(),
  policy.audit.allowInsert.always(),
]);

const ctx = new TestCleanup();
afterEach(async () => {
  await withTimeout(ctx.cleanup(), 10_000, "cleanup did not finish");
});

async function setup(label: string) {
  const server = await getJazzServerInfo(uniqueDbName(label));
  await deploy({
    appId: server.appId,
    serverUrl: server.serverUrl,
    adminSecret: server.adminSecret,
    schema: app.wasmSchema,
    permissions,
  });
  const open = async (persistent: boolean): Promise<Db> =>
    ctx.track(
      await createBrowserTestDb({
        appId: server.appId,
        serverUrl: server.serverUrl,
        secret: generateAuthSecret(),
        driver: persistent
          ? { type: "persistent", dbName: uniqueDbName(`${label}-tab`) }
          : { type: "memory" },
      }),
    );
  const tab = await open(true);
  const direct = await open(false);
  const { value: seat } = direct.insert(app.seats, { show: "s", holder: "" });
  await direct.insert(app.seats, { show: "s", holder: "" }).wait({ tier: "global" });
  // The tab holds the seats while it watches them.
  const seen: string[][] = [];
  ctx.trackSubscription(
    tab.subscribe(app.seats, (rows) => seen.push(rows.map((row) => row.holder))),
  );
  await waitForCondition(async () => seen.at(-1)?.length === 2, 10_000, "tab sees both seats");
  return { tab, direct, seat, seen };
}

// The worker refuses the commit itself once it has heard of the server's
// change (`transaction_conflict`); before that the server does
// (`serialization_conflict`). Either way the booking is refused.
const CONFLICT = /\((?:transaction|serialization)_conflict\)/;

async function holders(db: Db): Promise<string[]> {
  const rows = await db.all(app.seats, { tier: ReadTier.Remote });
  return rows.map((row) => row.holder).filter((holder) => holder !== "");
}

it("commits a tab's serializable booking through its worker", async () => {
  const { tab, direct } = await setup("serializable-worker-tab-book");
  const tx = tab.beginSerializableTransaction();
  const free = (await tx.all(app.seats)).filter((row) => row.holder === "");
  expect(free).toHaveLength(2);
  tx.update(app.seats, free[0]!.id, { holder: "tab" });
  await withTimeout(tx.commit().wait({ tier: "global" }), 15_000, "the booking did not settle");
  expect(await holders(direct)).toEqual(["tab"]);
});

it("refuses a tab's blind write over a seat the server changed after its snapshot", async () => {
  const { tab, direct, seat } = await setup("serializable-worker-tab-blind");
  const tx = tab.beginSerializableTransaction();
  // An unrelated read pins the snapshot; the seat itself is never read.
  await tx.all(app.audit);
  await direct.update(app.seats, seat.id, { holder: "direct" }).wait({ tier: "global" });
  tx.update(app.seats, seat.id, { holder: "tab" });
  await expect(
    withTimeout(tx.commit().wait({ tier: "global" }), 15_000, "the conflict did not arrive"),
  ).rejects.toThrow(CONFLICT);
  expect(await holders(direct)).toEqual(["direct"]);
});

it("refuses a tab's booking of a seat taken after it read the seat as free", async () => {
  const { tab, direct, seat } = await setup("serializable-worker-tab-read");
  const tx = tab.beginSerializableTransaction();
  const free = (await tx.all(app.seats)).filter((row) => row.holder === "");
  expect(free.map((row) => row.id)).toContain(seat.id);
  await direct.update(app.seats, seat.id, { holder: "direct" }).wait({ tier: "global" });
  tx.update(app.seats, seat.id, { holder: "tab" });
  await expect(
    withTimeout(tx.commit().wait({ tier: "global" }), 15_000, "the conflict did not arrive"),
  ).rejects.toThrow(CONFLICT);
  expect(await holders(direct)).toEqual(["direct"]);
});
