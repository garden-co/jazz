import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { deploy, startLocalJazzServer, type LocalJazzServerHandle } from "../testing/index.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { type Db } from "./db.js";
import { createDb } from "./default-create-db.js";
import { waitForRows } from "./testing/support.js";

const oldSchema = {
  todos: s.table(
    {
      title: s.string(),
      done: s.boolean(),
    },
    {},
  ),
};

const newSchema = {
  todos: s.table(
    {
      title: s.string(),
      done: s.boolean(),
      tags: s.array(s.string()).default([]),
    },
    {},
  ),
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
      ...(await localAccountConfig(appId, serverUrl)),
    });
    newDb = await createDb({
      ...(await localAccountConfig(appId, serverUrl)),
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

it("publishes UUID reference identity lenses and relates rows written before publication", async () => {
  const columns = {
    ownerId: s.uuid(),
    memberIds: s.array(s.uuid()),
    reviewerId: s.uuid().optional(),
  };
  const users = s.table({ name: s.string() }, {});
  const before = { users, records: s.table(columns, {}) };
  const after = {
    users,
    records: s.table(columns, {
      owner: s.rel("users", "ownerId"),
      members: s.rel("users", "memberIds"),
      reviewer: s.rel("users", "reviewerId"),
    }),
  };
  const beforeApp = s.defineApp(before),
    afterApp = s.defineApp(after);
  const permissionsBefore = s.definePermissions(beforeApp, ({ policy }) => [
    policy.users.allowRead.always(),
    policy.users.allowInsert.always(),
    policy.records.allowRead.always(),
    policy.records.allowInsert.always(),
  ]);
  const permissionsAfter = s.definePermissions(afterApp, ({ policy }) => [
    policy.users.allowRead.always(),
    policy.users.allowInsert.always(),
    policy.records.allowRead.always(),
    policy.records.allowInsert.always(),
  ]);
  const server = await startLocalJazzServer({ allowLocalFirstAuth: true, inMemory: true });
  let oldDb: Db | undefined, newDb: Db | undefined;
  try {
    const { appId, adminSecret, url: serverUrl } = server;
    await deploy({
      appId,
      adminSecret,
      serverUrl,
      schema: beforeApp,
      permissions: permissionsBefore,
    });
    oldDb = await createDb({ ...(await localAccountConfig(appId, serverUrl)) });
    const owner = await oldDb
      .insert(beforeApp.users, { name: "Existing owner" })
      .wait({ tier: "edge" });
    const record = await oldDb
      .insert(beforeApp.records, {
        ownerId: owner.id,
        memberIds: [owner.id, owner.id],
        reviewerId: null,
      })
      .wait({ tier: "edge" });
    const migration = s.defineMigration({ from: before, to: after });
    expect(migration.forward).toEqual([{ table: "records", operations: [] }]);
    await deploy({
      appId,
      adminSecret,
      serverUrl,
      schema: afterApp,
      permissions: permissionsAfter,
      migration,
    });
    newDb = await createDb({ ...(await localAccountConfig(appId, serverUrl)) });
    const rows = await waitForRows(
      newDb,
      afterApp.records
        .where({ id: record.id })
        .include({ owner: true, members: true, reviewer: true }),
      (rows) => rows.length === 1 && rows[0]?.owner?.name === "Existing owner",
    );
    expect(rows).toEqual([
      {
        id: record.id,
        ownerId: owner.id,
        memberIds: [owner.id, owner.id],
        reviewerId: null,
        owner: { id: owner.id, name: "Existing owner" },
        members: [
          { id: owner.id, name: "Existing owner" },
          { id: owner.id, name: "Existing owner" },
        ],
        reviewer: null,
      },
    ]);
  } finally {
    await newDb?.shutdown();
    await oldDb?.shutdown();
    await server.stop();
  }
}, 60_000);
