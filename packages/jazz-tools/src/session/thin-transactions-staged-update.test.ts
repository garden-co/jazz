// garden-co/jazz#6053: a transaction that inserts a row and then updates it
// is accepted. The row has no committed version, so its insert decides it
// (SPEC 7), on the state the transaction leaves it in; the update's own
// WITH CHECK, which compares the row with its committed version, does not.
// A committed row keeps its WITH CHECK.
import { afterEach, expect, it } from "vitest";
import type { AccountStore } from "../accounts/persistence.js";
import { startTestJazz, testApp, type TestJazz } from "../testing/index.js";
import { isThinDb } from "../runtime/local-first-upgrade.js";
import {
  openBrowserSessionCore as createJazzClient,
  type BrowserJazzSession,
} from "./create-jazz-session.js";

const app = testApp({
  tables: {
    items: { columns: { kind: "string", title: "string" } },
  },
});
// An item's kind is fixed once written: an update must keep the kind its
// committed version has.
const permissions = {
  format: 1,
  tables: {
    items: {
      read: "true",
      insert: "true",
      update: { old: "true", new: "exists items(id = this.id, kind = this.kind)" },
    },
  },
};

let server: TestJazz | undefined;
const sessions: BrowserJazzSession[] = [];
afterEach(async () => {
  for (const session of sessions.splice(0)) await session.close();
  await server?.stop();
  server = undefined;
});

function memoryStore(): AccountStore {
  let stored: string | null = null;
  return {
    async read() {
      return stored;
    },
    async update(transform) {
      stored = transform(stored);
    },
  };
}

async function thinDb() {
  server = await startTestJazz({ app, permissions, coreOptions: { inMemory: true } });
  const session = await createJazzClient({
    scope: server.appId,
    serverUrl: server.url,
    store: memoryStore(),
    initial: "device",
    driver: { type: "memory" },
  });
  sessions.push(session);
  const db = session.getSnapshot().client!.db;
  expect(isThinDb(db)).toBe(true);
  return db;
}

const kindsAndTitles = (rows: { kind: string; title: string }[]) =>
  rows.map(({ kind, title }) => ({ kind, title }));

it("accepts an update to a row inserted earlier in the same transaction", async () => {
  const db = await thinDb();

  const committed = await db.transaction((tx) => {
    const item = tx.insert(app.items, { kind: "a", title: "x" });
    tx.update(app.items, item.id, { title: "y" });
  });
  await committed.wait({ tier: "global" });

  expect(kindsAndTitles(await db.all(app.items))).toEqual([{ kind: "a", title: "y" }]);
}, 60_000);

it("still checks an update to a committed row against its committed version", async () => {
  const db = await thinDb();
  const item = await db.insert(app.items, { kind: "a", title: "x" }).wait({ tier: "global" });

  const refused = await db.transaction((tx) => {
    tx.update(app.items, item.id, { kind: "b" });
  });
  await expect(refused.wait({ tier: "global" })).rejects.toThrow(/WITH CHECK/);

  expect(kindsAndTitles(await db.all(app.items))).toEqual([{ kind: "a", title: "x" }]);
}, 60_000);
