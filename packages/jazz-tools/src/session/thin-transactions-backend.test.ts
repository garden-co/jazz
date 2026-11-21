// A thin client's transactions, served by the app's backend (SPEC 8.14):
// as whoever the client is, on the schema version it was built against.
import { afterEach, expect, it } from "vitest";
import type { AccountStore } from "../accounts/persistence.js";
import { startTestJazz, testApp, type TestJazz } from "../testing/index.js";
import { isThinDb } from "../runtime/local-first-upgrade.js";
import { createOpenTransactionId } from "../runtime/client.js";
import type { ThinRuntime } from "../thin/thin-runtime.js";
import type { ThinOp } from "../thin/wire.js";
import { encodeCellsForRow, parseUuid } from "../runtime/native-runtime/native-runtime-adapter.js";
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
  await committed.wait({ tier: "global" });
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
  await committed.wait({ tier: "global" });
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
  await committed.wait({ tier: "global" });
  expect((await db.all(oldApp.notes)).map((note) => note.title).sort()).toEqual([
    "after the redeploy",
    "before",
  ]);
}, 60_000);

// An app that declares encryption: a task's title is sealed in its
// project's space; notes are plain, and readable only by their author.
const encryptedApp = testApp({
  format: 1,
  tables: {
    projects: { columns: { name: "string", ownerId: "uuid" } },
    tasks: {
      columns: { projectId: "uuid", title: "bytes" },
      relations: { project: { rel: "projects", column: "projectId" } },
      encryption: { space: "projectId", scope: "projects", columns: { title: "string" } },
    },
    notes: { columns: { text: "string" } },
  },
});
const encryptedPermissions = {
  $schema: "https://jazz.tools/schemas/jazz.permissions.v1.json",
  tables: {
    projects: {
      read: "exists __e2ee_members(scopeId = table_id('projects'), identifier = this.id, accountId = session.user)",
      insert: "$createdBy.account = session.user",
    },
    tasks: { read: "true", insert: "true" },
    notes: { read: "$createdBy = session.user", insert: "true" },
  },
} as never;

it("runs a user's transaction on a plain table of an encrypted app, as them", async () => {
  server = await startTestJazz({
    app: encryptedApp,
    permissions: encryptedPermissions,
    coreOptions: { inMemory: true },
  });
  const session = await thinSession({
    serverUrl: server.url,
    bearer: async () => server!.bearer("alice"),
  });
  await session.loginWithBackend();
  const db = session.getSnapshot().client!.db;
  expect(isThinDb(db)).toBe(true);

  const alice = session.getSnapshot().client!.session!.user!;

  // The backend's client stages the write as alice, never as the backend
  // itself: read back in the transaction, the row is hers.
  const committed = await db.transaction(async (tx) => {
    tx.insert(encryptedApp.notes, { text: "alice's plain note" });
    return (await tx.all(encryptedApp.notes.select("text", "$createdBy"))).map((note) => [
      note.text,
      note.$createdBy.account,
    ]);
  });
  expect(await committed.wait({ tier: "global" })).toEqual([["alice's plain note", alice]]);
  // Readable only by its author: committed as alice.
  expect((await db.all(encryptedApp.notes)).map((note) => note.text)).toEqual([
    "alice's plain note",
  ]);
}, 60_000);

it("runs a guest's transaction on a plain table of an encrypted app, as the guest", async () => {
  server = await startTestJazz({
    app: encryptedApp,
    permissions: encryptedPermissions,
    coreOptions: { inMemory: true },
  });
  // An app that encrypts columns serves device accounts founded for its own
  // app id only.
  const session = await thinSession({
    scope: server.appId,
    serverUrl: server.url,
    initial: "device",
  });
  const client = session.getSnapshot().client!;
  expect(isThinDb(client.db)).toBe(true);
  const guest = client.session!.user!;

  const committed = await client.db.transaction(async (tx) => {
    tx.insert(encryptedApp.notes, { text: "the guest's plain note" });
    return (await tx.all(encryptedApp.notes.select("text", "$createdBy"))).map((note) => [
      note.text,
      note.$createdBy.account,
    ]);
  });
  expect(await committed.wait({ tier: "global" })).toEqual([["the guest's plain note", guest]]);
}, 60_000);

it("refuses a thin transaction's write of an encrypted column in the backend, which holds no keys", async () => {
  server = await startTestJazz({
    app: encryptedApp,
    permissions: encryptedPermissions,
    coreOptions: { inMemory: true },
  });
  const session = await thinSession({
    serverUrl: server.url,
    bearer: async () => server!.bearer("alice"),
  });
  await session.loginWithBackend();
  const db = session.getSnapshot().client!.db;
  expect(isThinDb(db)).toBe(true);

  // Plaintext bytes in the sealed column, as a thin client without the
  // client's guard would send them: the request is encoded raw
  // (`encodeThinRequest`) and handed to the runtime's sender, past the guard
  // that now refuses it first. The server must refuse it itself.
  // (A first read binds the connection to the app's schema.)
  expect(await db.all(encryptedApp.notes)).toEqual([]);
  const runtime = db.currentRuntime() as ThinRuntime;
  const raw = runtime as unknown as {
    definition(table: string): never;
    requestWith(op: ThinOp): { answer: Promise<unknown> };
  };
  const tx = runtime.beginTransaction("mergeable", createOpenTransactionId());
  const sent = raw.requestWith({
    type: "insert",
    table: "tasks",
    cells: encodeCellsForRow(
      raw.definition("tasks"),
      {
        projectId: { type: "Uuid", value: crypto.randomUUID() },
        title: { type: "Bytea", value: new TextEncoder().encode("PLAINTEXT") },
      },
      "tasks",
    ),
    options: { rowId: parseUuid(crypto.randomUUID()) },
    transaction: tx,
  } as ThinOp).answer;
  await expect(sent.then(() => runtime.commitTransaction(tx))).rejects.toThrow(
    /holds no encryption keys.*tasks\.title/,
  );
  // The refusal is the server's: it holds no row of the write.
  expect(await server.jazz.db.all(encryptedApp.tasks)).toEqual([]);
}, 60_000);
