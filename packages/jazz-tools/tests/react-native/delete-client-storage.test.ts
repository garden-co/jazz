import { expect, it } from "vitest";
import { schema as s } from "../../src/schema-namespace.js";
import { ReadTier } from "../../src/runtime/client.js";
import { withNativeRelayFixture } from "./fixture.js";

const app = s.defineApp({ notes: s.table({ title: s.string() }, {}) }, { migrations: "none" });
const permissions = s.definePermissions(app, ({ policy }) => [
  policy.notes.allowRead.always(),
  policy.notes.allowInsert.always(),
]);

// #5145: on React Native `deleteClientStorage()` deleted nothing (it logged
// that it is "only available in browser runtimes"), although it is the
// documented way out of a changed authority or a store opened without the
// app's migrations. Now the native relay deletes the account's store, and
// the next client of the same account opens a fresh, empty one.
it("deletes the account's native store and opens a fresh one for the same account", async () => {
  await withNativeRelayFixture(app, permissions, async (fixture) => {
    const db = await fixture.createDb();
    await db.insert(app.notes, { title: "local only" }).wait({ tier: "local" });
    expect(await db.all(app.notes, { tier: ReadTier.LocalFirst })).toHaveLength(1);

    await db.deleteClientStorage();

    const fresh = await fixture.createDb();
    expect(await fresh.all(app.notes, { tier: ReadTier.LocalFirst })).toEqual([]);
  });
}, 60_000);
