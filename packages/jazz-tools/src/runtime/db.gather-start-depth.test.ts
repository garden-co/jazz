import { afterAll, beforeAll, expect, it } from "vitest";
import { testApp } from "../testing/test-app.js";
import { createDb } from "./default-create-db.js";
import type { Db } from "./db.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { startTestJazz } from "../testing/index.js";

// garden-co/jazz#3778: `gather` honours `start` and `maxDepth` for a backend
// client and for a user client whose rows are behind a read policy.
const app = testApp({
  tables: {
    todos: {
      columns: { title: "string", done: "boolean", owner: "uuid", parent_id: "uuid?" },
      relations: { parent: { rel: "todos", column: "parent_id" } },
    },
  },
});
const permissions = {
  format: 1,
  tables: { todos: { read: "owner = session.user", insert: "owner = session.user" } },
};

let server: Awaited<ReturnType<typeof startTestJazz>>;
let backend: Db;
let user: Db;
const ids: Record<string, string> = {};

beforeAll(async () => {
  server = await startTestJazz({
    app,
    permissions,
    coreOptions: { inMemory: true },
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
  // The app's own backend database.
  backend = server.jazz.db;
}, 60_000);

afterAll(async () => {
  await user?.shutdown();
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
