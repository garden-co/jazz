import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { deploy, startLocalJazzServer, type LocalJazzServerHandle } from "../testing/index.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { type Db } from "./db.js";
import { createDb } from "./default-create-db.js";
import { waitForRows } from "./testing/support.js";

import { computeSchemaHash, pushPermissions, pushSchema } from "../dev/catalogue.js";
import { pushMigration } from "../dev/catalogue-project.js";
import { renderMigrationStub } from "../dev/migrations.js";
import { wasmSchemasEqual } from "../dev/schema-utils.js";
import { fetchSchemaConnectivity, fetchStoredWasmSchema } from "./schema-fetch.js";

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

it("publishes generated default-bearing relation migrations and preserves them across restart", async () => {
  const complexDefaults = {
    sequence: 9_223_372_036_854_775_806n,
    checkpoints: [[9_223_372_036_854_775_806n, -9_223_372_036_854_775_807n], []],
    payload: new Uint8Array([0, 1, 127, 255]),
    createdAt: new Date("2026-01-02T03:04:05.678Z"),
    metadata: { archived: false, labels: ["initial"], nested: { count: 3, value: null } },
  };
  const users = s.table({ name: s.string().default("Unnamed") }, {});
  const columns = {
    ownerId: s.uuid(),
    status: s.string().default("draft"),
    enabled: s.boolean().default(false),
    tags: s.array(s.string()).default(["initial"]),
    sequence: s.bigint().default(complexDefaults.sequence),
    checkpoints: s.array(s.array(s.bigint())).default(complexDefaults.checkpoints),
    payload: s.bytes().default(complexDefaults.payload),
    createdAt: s.timestamp().default(complexDefaults.createdAt),
    metadata: s.json().default(complexDefaults.metadata),
  };
  const before = { users, records: s.table(columns, {}) };
  const after = {
    users,
    records: s.table(columns, { owner: s.rel("users", "ownerId") }),
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
  const root = await mkdtemp(join(tmpdir(), "jazz-default-relation-e2e-"));
  let server: LocalJazzServerHandle | undefined;
  let oldDb: Db | undefined, newDb: Db | undefined;
  try {
    server = await startLocalJazzServer({
      allowLocalFirstAuth: true,
      dataDir: join(root, "data"),
    });
    const { appId, adminSecret, backendSecret } = server;
    const catalogue = { appId, adminSecret, serverUrl: server.url };
    const deployed = await deploy({
      ...catalogue,
      schema: beforeApp,
      permissions: permissionsBefore,
    });
    oldDb = await createDb(await localAccountConfig(appId, server.url));
    const owner = await oldDb
      .insert(beforeApp.users, { name: "Existing owner" })
      .wait({ tier: "edge" });
    const existing = await oldDb
      .insert(beforeApp.records, {
        ownerId: owner.id,
        status: "published",
        enabled: true,
        tags: ["retained"],
      })
      .wait({ tier: "edge" });
    const fromHash = deployed.schema.hash;
    const { hash: toHash } = await pushSchema({ ...catalogue, schema: afterApp });
    expect(toHash).not.toBe(fromHash);
    expect(await computeSchemaHash(beforeApp.wasmSchema)).toBe(fromHash);
    expect(await computeSchemaHash(afterApp.wasmSchema)).toBe(toHash);
    const source = renderMigrationStub({
      fromHash,
      toHash,
      fromSchema: beforeApp.wasmSchema,
      toSchema: afterApp.wasmSchema,
    }).replace('"jazz-tools"', JSON.stringify(new URL("../index.ts", import.meta.url).pathname));
    const migrationFile = join(
      root,
      `references-${fromHash.slice(0, 12)}-${toHash.slice(0, 12)}.ts`,
    );
    await writeFile(join(root, "package.json"), '{"type":"module"}');
    const options = { ...catalogue, fromHash, toHash, migrationsDir: root };
    expect(source).toContain('.default("draft")');
    for (const invalid of [
      source.replaceAll('.default("draft")', ""),
      source.replaceAll('.default("draft")', '.default("tampered")'),
    ]) {
      await writeFile(migrationFile, invalid);
      await expect(pushMigration(options)).rejects.toThrow(
        /schema witness for table records does not match canonical schema/,
      );
      expect(await fetchSchemaConnectivity(server.url, options)).toEqual({ connected: false });
    }
    await writeFile(migrationFile, source);
    expect(await pushMigration(options)).toMatchObject({ status: "published", fromHash, toHash });
    expect(await fetchSchemaConnectivity(server.url, options)).toEqual({ connected: true });
    await pushPermissions({ ...catalogue, schemaHash: toHash, permissions: permissionsAfter });

    await oldDb.shutdown();
    oldDb = undefined;
    await server.stop();
    server = await startLocalJazzServer({
      appId,
      adminSecret,
      backendSecret,
      allowLocalFirstAuth: true,
      dataDir: join(root, "data"),
    });
    for (const [schemaHash, expected] of [
      [fromHash, beforeApp.wasmSchema],
      [toHash, afterApp.wasmSchema],
    ] as const) {
      const stored = await fetchStoredWasmSchema(server.url, { appId, adminSecret, schemaHash });
      expect(wasmSchemasEqual(stored.schema, expected)).toBe(true);
      expect(await computeSchemaHash(stored.schema)).toBe(schemaHash);
    }
    newDb = await createDb(await localAccountConfig(appId, server.url));
    const rows = await waitForRows(
      newDb,
      afterApp.records.where({ id: existing.id }).include({ owner: true }),
      (rows) => rows.length === 1 && rows[0]?.owner?.name === "Existing owner",
    );
    expect(rows).toEqual([
      {
        ...complexDefaults,
        id: existing.id,
        ownerId: owner.id,
        status: "published",
        enabled: true,
        tags: ["retained"],
        owner: { id: owner.id, name: "Existing owner" },
      },
    ]);
    const defaultOwner = await newDb.insert(afterApp.users, {}).wait({ tier: "edge" });
    const created = await newDb
      .insert(afterApp.records, { ownerId: defaultOwner.id })
      .wait({ tier: "edge" });
    const defaultRows = await waitForRows(
      newDb,
      afterApp.records.where({ id: created.id }).include({ owner: true }),
      (rows) => rows.length === 1 && rows[0]?.owner?.name === "Unnamed",
    );
    expect(defaultRows).toEqual([
      {
        ...complexDefaults,
        id: created.id,
        ownerId: defaultOwner.id,
        status: "draft",
        enabled: false,
        tags: ["initial"],
        owner: { id: defaultOwner.id, name: "Unnamed" },
      },
    ]);
  } finally {
    await newDb?.shutdown();
    await oldDb?.shutdown();
    await server?.stop();
    await rm(root, { recursive: true, force: true });
  }
}, 60_000);
