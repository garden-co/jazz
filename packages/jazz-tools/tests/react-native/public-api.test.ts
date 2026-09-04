import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, it } from "vitest";
import { createDb } from "../../src/react-native/create-db.js";
import { schema } from "../../src/schema-namespace.js";
import { host } from "./native-platform.js";

const app = schema.defineApp({
  todos: schema.table({ title: schema.string(), done: schema.boolean() }),
});

it("runs public CRUD, filtered query and subscription through the real RN owner, then closes", async () => {
  const directory = await mkdtemp(join(tmpdir(), "jazz-rn-api-"));
  const issuer = "https://auth.example";
  const user = "rn-api-test";
  const capability = host.admit(
    JSON.stringify({
      scope: {
        app_namespace: "rn-api-test",
        storage_namespace: "default",
        auth_scope: JSON.stringify([issuer, user]),
      },
      sqlite_path: join(directory, "relay.sqlite"),
      schema_json: JSON.stringify(app.wasmSchema),
      identity: {
        node: "01010101-0101-0101-0101-010101010101",
        author: JSON.stringify([issuer, user]),
      },
      claims: {},
    }),
  );
  const db = await createDb({
    appId: "rn-api-test",
    nativeRelay: { capability },
    cookieSession: {
      issuer,
      user_id: user,
      claims: {},
      authMode: "external",
    },
  });
  const snapshots: { id: string; title: string; done: boolean }[][] = [];
  const open = app.todos.where({ done: false }).orderBy("title");
  const unsubscribe = db.subscribe(open, (rows) => snapshots.push(rows));
  try {
    const created = await db
      .insert(app.todos, { title: "first", done: false })
      .wait({ tier: "local" });
    await expect.poll(async () => db.all(open, { tier: "local" })).toEqual([created]);
    await expect.poll(() => snapshots.at(-1)).toEqual([created]);
    await db.update(app.todos, created.id, { title: "updated" }).wait({ tier: "local" });
    await expect
      .poll(async () => db.one(app.todos.where({ id: created.id }), { tier: "local" }))
      .toMatchObject({ title: "updated" });
    await expect.poll(() => snapshots.at(-1)).toEqual([{ ...created, title: "updated" }]);
    await db.delete(app.todos, created.id).wait({ tier: "local" });
    await expect.poll(async () => db.all(open, { tier: "local" })).toEqual([]);
    await expect.poll(() => snapshots.at(-1)).toEqual([]);
    unsubscribe();
    await db.shutdown();
    await expect(db.all(open, { tier: "local" })).rejects.toThrow();
  } finally {
    unsubscribe();
    await db.shutdown();
    await rm(directory, { recursive: true, force: true });
  }
});
