import { randomUUID } from "node:crypto";
import { afterEach, expect, it } from "vitest";
import type { AccountStore } from "../accounts/persistence.js";
import { startTestJazz, testApp, type TestJazz } from "../testing/index.js";
import { isThinDb } from "../runtime/local-first-upgrade.js";
import {
  openBrowserSessionCore as createJazzClient,
  type BrowserJazzSession,
} from "./create-jazz-session.js";

// A board and its owner's membership, written together: the membership's
// insert policy needs the board, which only the same transaction holds.
const app = testApp({
  tables: {
    boards: { columns: { name: "string" } },
    members: {
      columns: { boardId: "uuid", userId: "uuid", role: "string" },
      relations: { board: { rel: "boards", column: "boardId" } },
    },
  },
});
const permissions = {
  format: 1,
  tables: {
    // Boards are readable only by their members, so the board alone does not
    // show up for its creator until the membership lands with it.
    boards: {
      read: "exists members(boardId = this.id, userId = session.user)",
      insert: "true",
    },
    members: {
      read: "true",
      insert: {
        allOf: [
          "userId = session.user and role = 'owner'",
          "exists boards(id = this.boardId, $createdBy = session.user)",
        ],
      },
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

it("lets a thin client's transaction satisfy exists with a row it inserted itself", async () => {
  server = await startTestJazz({
    app,
    permissions,
    coreOptions: { inMemory: true },
  });
  const session = await createJazzClient({
    scope: server.appId,
    serverUrl: server.url,
    store: memoryStore(),
    initial: "device",
    driver: { type: "memory" },
  });
  sessions.push(session);
  const client = session.getSnapshot().client!;
  const db = client.db;
  expect(isThinDb(db)).toBe(true);
  const me = client.session!.user!;

  const result = await db.transaction(async (tx) => {
    const board = tx.insert(app.boards, { name: "Retro" });
    tx.insert(app.members, { boardId: board.id, userId: me, role: "owner" });
    return board.id;
  });
  const boardId = await result.wait({ tier: "global" });
  expect((await db.all(app.members.where({ boardId }))).map((m) => m.role)).toEqual(["owner"]);
  expect((await db.all(app.boards)).map((b) => b.name)).toEqual(["Retro"]);
}, 60_000);

it("checks a thin client's transaction as a whole, so the order of its writes doesn't matter", async () => {
  server = await startTestJazz({
    app,
    permissions,
    coreOptions: { inMemory: true },
  });
  const session = await createJazzClient({
    scope: server.appId,
    serverUrl: server.url,
    store: memoryStore(),
    initial: "device",
    driver: { type: "memory" },
  });
  sessions.push(session);
  const client = session.getSnapshot().client!;
  const db = client.db;
  expect(isThinDb(db)).toBe(true);
  const me = client.session!.user!;

  // The membership first: alone, its policy would refuse it, since the
  // board it needs is staged after it. The backend serves the transaction
  // and the core checks it once it is finished, in the state it leaves.
  const boardId = randomUUID();
  const result = await db.transaction(async (tx) => {
    tx.insert(app.members, { boardId, userId: me, role: "owner" });
    tx.insert(app.boards, { name: "Planning" }, { id: boardId });
  });
  await result.wait({ tier: "global" });
  expect((await db.all(app.members.where({ boardId }))).map((m) => m.role)).toEqual(["owner"]);
  expect((await db.all(app.boards)).map((b) => b.name)).toEqual(["Planning"]);
}, 60_000);

// A note may be added where its author may update the branch: the branch
// the same transaction creates counts.
const branches = testApp({
  tables: {
    branches: { columns: { name: "string" } },
    notes: {
      columns: { branchId: "uuid", title: "string" },
      relations: { branch: { rel: "branches", column: "branchId" } },
    },
  },
});
const branchPermissions = {
  format: 1,
  tables: {
    branches: { read: "true", insert: "true", update: "true" },
    notes: { read: "allowedTo.read(branch)", insert: "allowedTo.update(branch)" },
  },
};

it("lets a thin client's transaction satisfy allowedTo.update with a parent it inserted itself", async () => {
  server = await startTestJazz({
    app: branches,
    permissions: branchPermissions,
    coreOptions: { inMemory: true },
  });
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

  const result = await db.transaction(async (tx) => {
    const branch = tx.insert(branches.branches, { name: "draft" });
    tx.insert(branches.notes, { branchId: branch.id, title: "first" });
    return branch.id;
  });
  const branchId = await result.wait({ tier: "global" });
  expect((await db.all(branches.notes.where({ branchId }))).map((n) => n.title)).toEqual(["first"]);
}, 60_000);
