import { expect, it } from "vitest";
import { testApp, testPermissions } from "../../src/testing/index.js";
import { ReadTier } from "../../src/runtime/client.js";
import { withNativeRelayFixture } from "./fixture.js";

const app = testApp({ tables: { notes: { columns: { title: "string" } } } });
const permissions = testPermissions(app, {
  tables: { notes: { read: "true", insert: "true" } },
});

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
