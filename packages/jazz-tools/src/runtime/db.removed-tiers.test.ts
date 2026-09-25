import { afterEach, expect, it, vi } from "vitest";
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
  vi.restoreAllMocks();
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

it("waits for global instead of rejecting an already committed write at the removed edge tier", async () => {
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
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    const edge = { tier: "edge" } as unknown as { tier: "global" };

    const inserted = await db.insert(app.notes, { title: "Draft" }).wait(edge);
    expect(inserted.title).toBe("Draft");
    await db.update(app.notes, inserted.id, { title: "Final" }).wait(edge);
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('The "edge" tier was removed'));

    // Resolving at edge means the write reached the server: a fresh client sees
    // exactly one row, so a caller never has a reason to retry it.
    reader = await createDb(await localAccountConfig(server.appId, server.url));
    expect(await reader.all(app.notes, { tier: "global" })).toEqual([
      { id: inserted.id, title: "Final" },
    ]);
  } finally {
    await reader?.shutdown();
    await db?.shutdown();
    db = undefined;
    await server.stop();
  }
}, 60_000);
