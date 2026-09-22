import { afterEach, beforeEach, expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import { createDb } from "./default-create-db.js";
import { prepareDbTransaction, type Db } from "./db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import type { LocalJazzServerHandle } from "../testing/index.js";

const app = s.defineApp({
  notes: s.table({ title: s.string() }, {}),
  markers: s.table({ title: s.string() }, {}),
});
const renamed = s.defineApp({ notes: s.table({ body: s.string() }, {}) });
const foreign = s.defineApp({ notes: s.table({ title: s.string() }, {}) });
let server: LocalJazzServerHandle;
let db: Db;
let note: { id: string; title: string };

beforeEach(async () => {
  server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  await deploy({
    serverUrl: server.url,
    appId: server.appId,
    adminSecret: server.adminSecret,
    schema: app,
    permissions: definePermissions(app, ({ policy }) => {
      policy.notes.allowRead.always();
      policy.notes.allowInsert.always();
      policy.notes.allowUpdate.always();
      policy.markers.allowRead.always();
      policy.markers.allowInsert.always();
      policy.markers.allowUpdate.always();
    }),
  });
  db = await createDb(await localAccountConfig(server.appId, server.url));
  note = await db.insert(app.notes, { title: "Accepted note" }).wait({ tier: "global" });
});

afterEach(async () => {
  await db?.shutdown();
  await server?.stop();
});

it.each(["all", "one", "settled", "insert", "upsert"] as const)(
  "rejects a foreign-schema prepared %s and prevents committing prior writes",
  async (operation) => {
    const tx = db.beginExclusiveTransaction();
    tx.insert(app.markers, { title: "Must roll back" });
    await expect(
      prepareDbTransaction(tx, async (scope) => {
        switch (operation) {
          case "all":
            await scope.all(renamed.notes, { tier: "local" });
            break;
          case "one":
            await scope.one(renamed.notes, { tier: "local" });
            break;
          case "settled":
            await scope.allSettledForE2ee(renamed.notes);
            break;
          case "insert":
            scope.insert(foreign.notes, { title: "Wrong schema" });
            break;
          case "upsert":
            scope.upsert(foreign.notes, note.id, { title: "Wrong schema" });
            break;
        }
      }),
    ).rejects.toThrow();
    await expect(tx.commit().wait({ tier: "global" })).rejects.toThrow();
    tx.rollback();
    expect(await db.all(app.markers, { tier: "global" })).toEqual([]);
    expect(await db.all(app.notes, { tier: "global" })).toEqual([note]);
  },
);

it("drains same-schema preparation after commit is requested without losing typed rows", async () => {
  const tx = db.beginExclusiveTransaction();
  let release!: () => void;
  const ready = new Promise<void>((resolve) => {
    release = resolve;
  });
  let markerId = "";
  const preparation = prepareDbTransaction(tx, async (scope) => {
    await ready;
    const settled = await scope.allSettledForE2ee(app.notes);
    expect(settled.rows).toEqual([note]);
    expect(settled.settlements.map((entry) => entry.rowId)).toEqual([note.id]);
    const marker = scope.insert(app.markers, { title: "Prepared" });
    markerId = marker.id;
    scope.upsert(app.markers, marker.id, { title: "Updated during preparation" });
    expect(await scope.one(app.markers.where({ id: marker.id }), { tier: "local" })).toEqual({
      id: marker.id,
      title: "Updated during preparation",
    });
  });
  const committed = tx.commit();
  release();
  await Promise.all([preparation, committed.wait({ tier: "global" })]);
  expect(await db.all(app.markers, { tier: "global" })).toEqual([
    { id: markerId, title: "Updated during preparation" },
  ]);
});
