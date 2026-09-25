import { afterEach, expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createDb } from "./default-create-db.js";
import type { Db, QueryOptions } from "./db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

// Plain JavaScript (or a cast) can still pass tiers removed in alpha.57.
const removedReadTiers = [
  ["remote-if-possible", 'The "remote-if-possible" tier was removed'],
  ["edge", 'The "edge" tier was removed'],
] as const;

const app = s.defineApp({ notes: s.table({ title: s.string() }, {}) });

let db: Db | undefined;

afterEach(async () => {
  await db?.shutdown();
  db = undefined;
});

it.each(removedReadTiers)("rejects Db reads at the removed %s tier", async (tier, message) => {
  db = await createDb({
    ...(await localAccountConfig(`removed-read-tier-${tier}`)),
    driver: { type: "memory" },
  });
  const options = { tier } as unknown as QueryOptions;

  await expect(db.all(app.notes, options)).rejects.toThrow(message);
  await expect(db.one(app.notes, options)).rejects.toThrow(message);
  expect(() => db!.subscribe(app.notes, () => {}, options)).toThrow(message);
  const tx = db.beginTransaction();
  await expect(tx.all(app.notes, options)).rejects.toThrow(message);
});

it("rejects a wait at the removed edge tier and says the write was already applied", async () => {
  const permissions = definePermissions(app, ({ policy }) => {
    policy.notes.allowRead.always();
    policy.notes.allowInsert.always();
    policy.notes.allowUpdate.always();
  });
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let reader: Db | undefined;
  try {
    await deploy({
      serverUrl: server.url,
      appId: server.appId,
      adminSecret: server.adminSecret,
      schema: app,
      permissions,
    });
    db = await createDb(await localAccountConfig(server.appId, server.url));
    const edge = { tier: "edge" } as unknown as { tier: "global" };
    const removed = /The "edge" tier was removed\. Use "global".*already applied; do not retry/;

    const insert = db.insert(app.notes, { title: "Draft" });
    await expect(insert.wait(edge)).rejects.toThrow(removed);
    const update = db.update(app.notes, insert.value.id, { title: "Final" });
    await expect(update.wait(edge)).rejects.toThrow(removed);

    // The rejection is not a write failure: both writes still reach the server.
    await update.wait({ tier: "global" });
    reader = await createDb(await localAccountConfig(server.appId, server.url));
    expect(await reader.all(app.notes, { tier: "global" })).toEqual([
      { id: insert.value.id, title: "Final" },
    ]);
  } finally {
    await reader?.shutdown();
    await db?.shutdown();
    db = undefined;
    await server.stop();
  }
}, 60_000);
