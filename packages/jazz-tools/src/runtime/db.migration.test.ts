import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { deploy, startLocalJazzServer, type LocalJazzServerHandle } from "../testing/index.js";
import { generateAuthSecret } from "./auth-secret-store.js";
import { createDb, type Db } from "./db.js";
import { waitForRows } from "./testing/support.js";

const oldSchema = {
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
  }),
};

const newSchema = {
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
    tags: s.array(s.string()).default([]),
  }),
};

type OldAppSchema = s.Schema<typeof oldSchema>;
type NewAppSchema = s.Schema<typeof newSchema>;

const oldApp: s.App<OldAppSchema> = s.defineApp(oldSchema);
const newApp: s.App<NewAppSchema> = s.defineApp(newSchema);

const oldPermissions = s.definePermissions(oldApp, ({ policy }) => [
  policy.todos.allowRead.always(),
  policy.todos.allowInsert.always(),
  policy.todos.allowUpdate.always(),
  policy.todos.allowDelete.always(),
]);

const newPermissions = s.definePermissions(newApp, ({ policy }) => [
  policy.todos.allowRead.always(),
  policy.todos.allowInsert.always(),
  policy.todos.allowUpdate.always(),
  policy.todos.allowDelete.always(),
]);

const migration = s.defineMigration({
  fromHash: await oldApp.schemaHash,
  toHash: await newApp.schemaHash,
  from: oldSchema,
  to: newSchema,
  migrate: {
    todos: {
      tags: s.add.array({ of: s.string(), default: [] }),
    },
  },
});

describe("schema migrations", () => {
  let server: LocalJazzServerHandle;
  let oldDb: Db;
  let newDb: Db;

  beforeEach(async () => {
    server = await startLocalJazzServer({
      allowLocalFirstAuth: true,
      inMemory: true,
    });
    const { appId, adminSecret, url: serverUrl } = server;

    await deploy({
      serverUrl,
      appId,
      adminSecret,
      schema: oldApp,
      permissions: oldPermissions,
    });

    await deploy({
      serverUrl,
      appId,
      adminSecret,
      schema: newApp,
      permissions: newPermissions,
      migration,
    });

    oldDb = await createDb({
      appId,
      driver: { type: "memory" },
      serverUrl,
      secret: generateAuthSecret(),
    });
    newDb = await createDb({
      appId,
      driver: { type: "memory" },
      serverUrl,
      secret: generateAuthSecret(),
    });
  });

  afterEach(async () => {
    await oldDb.shutdown();
    await newDb.shutdown();
    await server.stop();
  });

  it("a new-schema client can read rows written by an old-schema client", async () => {
    const created = await oldDb
      .insert(oldApp.todos, { title: "written through old schema", done: false })
      .wait({ tier: "edge" });

    const newRows = await waitForRows(
      newDb,
      newApp.todos.where({ id: { eq: created.id } }),
      (rows) => rows.length === 1,
    );

    expect(newRows).toEqual([
      {
        id: created.id,
        title: "written through old schema",
        done: false,
        tags: [],
      },
    ]);
  }, 60_000);

  it("an old-schema client can read rows written by a new-schema client", async () => {
    const created = await newDb
      .insert(newApp.todos, {
        title: "written through new schema",
        done: true,
        tags: ["migration"],
      })
      .wait({ tier: "edge" });

    const oldRows = await waitForRows(
      oldDb,
      oldApp.todos.where({ id: { eq: created.id } }),
      (rows) => rows.length === 1,
    );

    expect(oldRows).toEqual([
      {
        id: created.id,
        title: "written through new schema",
        done: true,
      },
    ]);
  }, 60_000);
});
