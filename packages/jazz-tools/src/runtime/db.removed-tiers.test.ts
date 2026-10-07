import { afterEach, expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { createDb } from "./default-create-db.js";
import type { Db, QueryOptions } from "./db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";

// Plain JavaScript (or a cast) can still pass removed read tiers. Reads accept
// only "local-first" and "remote"; "local" and "global" remain write tiers.
const removedReadTiers = [
  ["remote-if-possible", 'The "remote-if-possible" tier was removed'],
  ["edge", 'The "edge" read tier was removed'],
  ["local-first-unless-empty", 'The "local-first-unless-empty" tier was removed'],
  ["local", 'The "local" read tier was removed'],
  ["global", 'The "global" read tier was removed'],
  ["core", 'The "core" read tier was removed'],
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

it.each([
  ["edge", 'Unknown wait tier "edge"; expected "local" or "global".'],
  ["globl", 'Unknown wait tier "globl"; expected "local" or "global".'],
])("rejects wait tier %s without duplicating the applied write", async (tier, message) => {
  db = await createDb({
    ...(await localAccountConfig("unknown-write-wait-tier")),
    driver: { type: "memory" },
  });
  // A typo from plain JavaScript or a cast.
  const invalidOptions = { tier } as unknown as { tier: "global" };

  const inserted = db.insert(app.notes, { title: "Draft" });
  const waiting = inserted.wait(invalidOptions);
  await expect(waiting).rejects.toThrow(TypeError);
  await expect(waiting).rejects.toThrow(`${message} The write was already applied`);
  await expect(
    db.update(app.notes, inserted.value.id, { title: "Final" }).wait(invalidOptions),
  ).rejects.toThrow("The write was already applied");

  // The write itself stands and still settles at a valid tier.
  await inserted.wait({ tier: "local" });
  expect(await db.all(app.notes)).toEqual([{ id: inserted.value.id, title: "Final" }]);
});
