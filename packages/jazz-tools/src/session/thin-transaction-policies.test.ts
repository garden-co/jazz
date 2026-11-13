import { afterEach, expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import type { AccountStore } from "../accounts/persistence.js";
import { startTestJazz, type TestJazz } from "../testing/index.js";
import { isThinDb } from "../runtime/local-first-upgrade.js";
import {
  openBrowserSessionCore as createJazzClient,
  type BrowserJazzSession,
} from "./create-jazz-session.js";

// A board and its owner's membership, written together: the membership's
// insert policy needs the board, which only the same transaction holds.
const app = s.defineApp(
  {
    boards: s.table({ name: s.string() }, {}),
    members: s.table(
      { boardId: s.uuid(), userId: s.uuid(), role: s.string() },
      { board: s.rel("boards", "boardId") },
    ),
  },
  { migrations: "none" },
);
const permissions = definePermissions(app, ({ policy, session, allOf }) => {
  // Boards are readable only by their members, so the board alone does not
  // show up for its creator until the membership lands with it.
  policy.boards.allowRead.where((b) =>
    policy.members.exists.where({ boardId: b.id, userId: session.user }),
  );
  policy.boards.allowInsert.always();
  policy.members.allowRead.always();
  policy.members.allowInsert.where((m) =>
    allOf([
      { userId: session.user, role: "owner" },
      policy.boards.exists.where({ id: m.boardId, $createdBy: session.user }),
    ]),
  );
});

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

// A note may be added where its author may update the branch: the branch
// the same transaction creates counts.
const branches = s.defineApp(
  {
    branches: s.table({ name: s.string() }, {}),
    notes: s.table(
      { branchId: s.uuid(), title: s.string() },
      { branch: s.rel("branches", "branchId") },
    ),
  },
  { migrations: "none" },
);
const branchPermissions = definePermissions(branches, ({ policy, allowedTo }) => {
  policy.branches.allowRead.always();
  policy.branches.allowInsert.always();
  policy.branches.allowUpdate.always();
  policy.notes.allowRead.where(allowedTo.read("branch"));
  policy.notes.allowInsert.where(allowedTo.update("branch"));
});

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
