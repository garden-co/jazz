import { afterAll, beforeAll, expect, it } from "vitest";
import { schema as s } from "../schema-namespace.js";
import { definePermissions } from "../permissions/index.js";
import { createJazzSession } from "../backend/index.js";
import { createDb } from "./default-create-db.js";
import type { Db } from "./db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";

// garden-co/jazz#3778: `gather` honours `start` and `maxDepth` for a backend
// client and for a user client whose rows are behind a read policy.
const app = s.defineApp(
  {
    todos: s.table(
      {
        title: s.string(),
        done: s.boolean(),
        owner: s.uuid(),
        parent_id: s.uuid().optional(),
      },
      { parent: s.rel("todos", "parent_id") },
    ),
  },
  { migrations: "none" },
);
const permissions = definePermissions(app, ({ policy, session }) => {
  policy.todos.allowRead.where({ owner: session.user.account });
  policy.todos.allowInsert.where({ owner: session.user.account });
});

let server: Awaited<ReturnType<typeof startLocalJazzServer>>;
let backendSession: Awaited<ReturnType<typeof createJazzSession>>;
let backend: Db;
let user: Db;
const ids: Record<string, string> = {};

beforeAll(async () => {
  server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  await deploy({
    serverUrl: server.url,
    appId: server.appId,
    adminSecret: server.adminSecret,
    schema: app,
    permissions,
  });
  const account = await localAccountConfig(server.appId, server.url);
  user = await createDb(account);
  const owner = account.account.id;
  // Two chains: root <- mid <- leaf, and other-root <- other-leaf.
  for (const [name, parent, done] of [
    ["root", undefined, true],
    ["mid", "root", false],
    ["leaf", "mid", true],
    ["other-root", undefined, true],
    ["other-leaf", "other-root", true],
  ] as const) {
    const row = await user
      .insert(app.todos, {
        title: name,
        done,
        owner,
        parent_id: parent === undefined ? undefined : ids[parent],
      })
      .wait({ tier: "global" });
    ids[name] = row.id;
  }
  backendSession = await createJazzSession({
    appId: server.appId,
    app,
    permissions,
    serverUrl: server.url,
    driver: { type: "memory" },
    initial: { backendSecret: server.backendSecret },
  });
  const snapshot = backendSession.getSnapshot();
  if (!snapshot.client) throw snapshot.error ?? new Error(snapshot.status);
  backend = snapshot.client.db;
}, 60_000);

afterAll(async () => {
  await user?.shutdown();
  await backendSession?.getSnapshot().client?.shutdown({ waitForSync: false });
  await backendSession?.close();
  await server?.stop();
});

const ancestors = (start: Parameters<typeof app.todos.gather>[0]["start"], maxDepth: number) =>
  app.todos.gather({
    start,
    step: ({ current }) => app.todos.where({ id: current }).hopTo("parent"),
    maxDepth,
  });

async function titles(db: Db, query: ReturnType<typeof ancestors>): Promise<string[]> {
  const rows = await db.all(query, { tier: "remote" });
  return rows.map((row) => row.title).sort();
}

for (const [name, db] of [
  ["a backend client", () => backend],
  ["a user client behind a read policy", () => user],
] as const) {
  it(`${name} gathers from its start, up to maxDepth hops`, async () => {
    expect(await titles(db(), ancestors({ id: ids.leaf }, 0))).toEqual(["leaf"]);
    expect(await titles(db(), ancestors({ id: ids.leaf }, 1))).toEqual(["leaf", "mid"]);
    expect(await titles(db(), ancestors({ id: ids.leaf }, 10))).toEqual(["leaf", "mid", "root"]);
    expect(await titles(db(), ancestors({ done: false }, 10))).toEqual(["mid", "root"]);
  }, 60_000);
}
