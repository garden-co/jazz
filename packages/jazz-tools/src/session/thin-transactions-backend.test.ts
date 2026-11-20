// A thin client's transactions, served by the app's backend (SPEC 8.14):
// as whoever the client is, on the schema version it was built against.
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
    notes: { columns: { title: "string" } },
    locked: { columns: { title: "string" } },
  },
});
// Notes are their author's; no one may write `locked`.
const permissions = {
  format: 1,
  tables: {
    notes: { read: "$createdBy = session.user", insert: "true" },
    locked: { read: "true", insert: "false" },
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

async function thinSession(
  options: Omit<Parameters<typeof createJazzClient>[0], "store" | "driver">,
): Promise<BrowserJazzSession> {
  const session = await createJazzClient({
    ...options,
    store: memoryStore(),
    driver: { type: "memory" },
  } as Parameters<typeof createJazzClient>[0]);
  sessions.push(session);
  return session;
}

it("runs a signed-in user's transaction as them, and their policies refuse what isn't theirs", async () => {
  server = await startTestJazz({ app, permissions, coreOptions: { inMemory: true } });
  const token = server.bearer("alice");
  const session = await thinSession({ serverUrl: server.url, bearer: async () => token });
  await session.loginWithBackend();
  const db = session.getSnapshot().client!.db;
  expect(isThinDb(db)).toBe(true);

  const committed = await db.transaction((tx) => {
    tx.insert(app.notes, { title: "alice's" });
  });
  await committed.wait();
  // Readable only by its author: written as alice.
  expect((await db.all(app.notes)).map((note) => note.title)).toEqual(["alice's"]);

  const refused = await db.transaction((tx) => {
    tx.insert(app.notes, { title: "with a refused write" });
    tx.insert(app.locked, { title: "not anyone's to write" });
  });
  await expect(refused.wait({ tier: "global" })).rejects.toThrow();
  expect((await db.all(app.notes)).map((note) => note.title)).toEqual(["alice's"]);
  expect(await db.all(app.locked)).toEqual([]);
}, 60_000);

it("runs the transaction of a guest whose token is for the endpoint", async () => {
  server = await startTestJazz({ app, permissions, coreOptions: { inMemory: true } });
  // No app id: the guest is scoped by the endpoint, and its token is for it.
  const session = await thinSession({ serverUrl: server.url, initial: "device" });
  const db = session.getSnapshot().client!.db;
  expect(isThinDb(db)).toBe(true);

  const committed = await db.transaction((tx) => {
    tx.insert(app.notes, { title: "the guest's" });
  });
  await committed.wait();
  expect((await db.all(app.notes)).map((note) => note.title)).toEqual(["the guest's"]);
}, 60_000);

it("runs the transaction of a client built against the app's previous schema", async () => {
  const before = {
    notes: { columns: { title: "string" } },
    locked: { columns: { title: "string" } },
  };
  const after = {
    notes: { columns: { body: "string" } },
    locked: { columns: { title: "string" } },
  };
  const oldApp = testApp({ tables: before });
  const newApp = testApp({
    tables: after,
    history: { v1: { tables: before } },
    migrations: [{ from: "v1", migrate: { notes: { body: { renameFrom: "title" } } } }],
  });
  server = await startTestJazz({ app: oldApp, permissions, coreOptions: { inMemory: true } });
  const session = await thinSession({
    scope: server.appId,
    serverUrl: server.url,
    initial: "device",
  });
  const db = session.getSnapshot().client!.db;
  expect(isThinDb(db)).toBe(true);
  await db.insert(oldApp.notes, { title: "before" }).wait({ tier: "global" });

  // The app moves on; this client stays on the version it was built for.
  await server.redeploy({ app: newApp, permissions: { ...permissions } });
  const committed = await db.transaction((tx) => {
    tx.insert(oldApp.notes, { title: "after the redeploy" });
  });
  await committed.wait();
  expect((await db.all(oldApp.notes)).map((note) => note.title).sort()).toEqual([
    "after the redeploy",
    "before",
  ]);
}, 60_000);

it("runs the transaction of a client built against an app that ships migrations", async () => {
  // The app's current version renames a column from its first one, so the
  // core stores it under names its migrations derive. The session's replica
  // in the backend must be opened with those migrations, or it names the
  // version itself and the core refuses every transaction it serves.
  const before = {
    notes: { columns: { title: "string" } },
    locked: { columns: { title: "string" } },
  };
  const migratedApp = testApp({
    tables: {
      notes: { columns: { body: "string" } },
      locked: { columns: { title: "string" } },
    },
    history: { v1: { tables: before } },
    migrations: [{ from: "v1", migrate: { notes: { body: { renameFrom: "title" } } } }],
  });
  server = await startTestJazz({
    app: migratedApp,
    permissions,
    coreOptions: { inMemory: true },
  });
  const session = await thinSession({
    scope: server.appId,
    serverUrl: server.url,
    initial: "device",
  });
  const db = session.getSnapshot().client!.db;
  expect(isThinDb(db)).toBe(true);

  const committed = await db.transaction((tx) => {
    tx.insert(migratedApp.notes, { body: "in a transaction" });
  });
  await committed.wait();
  const serializable = await db.serializableTransaction(async (tx) => {
    const existing = await tx.all(migratedApp.notes);
    tx.insert(migratedApp.notes, { body: `after ${existing.length}` });
  });
  await serializable.wait();
  expect((await db.all(migratedApp.notes)).map((note) => note.body).sort()).toEqual([
    "after 1",
    "in a transaction",
  ]);
}, 60_000);
