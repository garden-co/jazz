import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { bindApp, boundApp } from "../typed-app.js";
import { compileTestApp, testApp, testPermissions, type TestAppFile } from "../testing/test-app.js";
import {
  deploy,
  startLocalJazzServer,
  startTestJazz,
  type LocalJazzServerHandle,
  type TestJazz,
} from "../testing/index.js";
import { localAccountConfig } from "./testing/account-fixtures.js";
import { type Db } from "./db.js";
import { createDb } from "./default-create-db.js";
import { waitForRows } from "./testing/support.js";
import { shippedMigrations } from "./client-migrations.js";
import type { WasmSchema } from "../drivers/types.js";

import { generateTs, loadBackendAppFor, migrationsCreate } from "../dev/codegen-napi.js";
import { fetchMigrationGraph } from "../dev/migration-graph.js";
import { wasmSchemasEqual } from "../dev/schema-utils.js";
import {
  DeploymentError,
  fetchStoredWasmSchema,
  publishDeployment,
  type DeploymentBody,
} from "../dev/catalogue-api.js";

/** Every operation allowed on each of `tables`, as a `jazz.permissions.json`. */
function allowAll(tables: string[], ops = ["read", "insert", "update", "delete"]) {
  return {
    format: 1,
    tables: Object.fromEntries(
      tables.map((table) => [table, Object.fromEntries(ops.map((op) => [op, "true"]))]),
    ),
  };
}

const oldTables = { todos: { columns: { title: "string", done: "boolean" } } };
const newTables = {
  todos: {
    columns: { title: "string", done: "boolean", tags: { array: "string", default: [] } },
  },
};

// Each app ships its migrations, as its generated module would hold them.
const oldApp = testApp({ tables: oldTables });
const newApp = testApp({
  tables: newTables,
  history: { v1: { tables: oldTables } },
  migrations: [
    { from: "v1", migrate: { todos: { tags: { add: { array: "string" }, default: [] } } } },
  ],
});

const oldPermissions = allowAll(["todos"]);
const newPermissions = allowAll(["todos"]);

describe("schema migrations", () => {
  let server: LocalJazzServerHandle;
  // The app's backend, running the latest deploy: clients of both schema
  // versions sync through it.
  let front: TestJazz;
  let oldDb: Db;
  let newDb: Db;

  beforeEach(async () => {
    server = await startLocalJazzServer({
      inMemory: true,
    });
    const { appId, adminSecret, url: serverUrl } = server;

    await deploy({
      serverUrl,
      appId,
      adminSecret,
      app: oldApp,
      permissions: oldPermissions,
    });

    await deploy({
      serverUrl,
      appId,
      adminSecret,
      app: newApp,
      permissions: newPermissions,
    });
    front = await startTestJazz({ app: newApp, permissions: newPermissions, core: server });

    oldDb = await createDb({ ...(await localAccountConfig(appId, front.url)) });
    newDb = await createDb({ ...(await localAccountConfig(appId, front.url)) });
  });

  afterEach(async () => {
    await oldDb.shutdown();
    await newDb.shutdown();
    await front.stop();
    await server.stop();
  });

  it("a new-schema client can read rows written by an old-schema client", async () => {
    const created = await oldDb
      .insert(oldApp.todos, { title: "written through old schema", done: false })
      .wait({ tier: "global" });

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
      .wait({ tier: "global" });

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
    ownerId: "uuid",
    memberIds: { array: "uuid" },
    reviewerId: "uuid?",
  };
  const users = { columns: { name: "string" } };
  const before = { users, records: { columns } };
  const after = {
    users,
    records: {
      columns,
      relations: {
        owner: { rel: "users", column: "ownerId" },
        members: { rel: "users", column: "memberIds" },
        reviewer: { rel: "users", column: "reviewerId" },
      },
    },
  };
  const beforeApp = testApp({ tables: before }),
    afterApp = testApp({
      tables: after,
      history: { v1: { tables: before } },
      migrations: [{ from: "v1", migrate: { records: {} } }],
    });
  const permissionsBefore = allowAll(["users", "records"], ["read", "insert"]);
  const permissionsAfter = allowAll(["users", "records"], ["read", "insert"]);
  const server = await startTestJazz({
    app: beforeApp,
    permissions: permissionsBefore,
    coreOptions: { inMemory: true },
  });
  let oldDb: Db | undefined, newDb: Db | undefined;
  try {
    const { appId, url: serverUrl } = server;
    oldDb = await createDb({ ...(await localAccountConfig(appId, serverUrl)) });
    const owner = await oldDb
      .insert(beforeApp.users, { name: "Existing owner" })
      .wait({ tier: "global" });
    const record = await oldDb
      .insert(beforeApp.records, {
        ownerId: owner.id,
        memberIds: [owner.id, owner.id],
        reviewerId: null,
      })
      .wait({ tier: "global" });
    expect(
      shippedMigrations(afterApp.wasmSchema)?.migrations.map((edge) => edge.forward),
    ).toMatchObject([[{ table: "records", operations: [] }]]);
    await server.redeploy({ app: afterApp, permissions: permissionsAfter });
    newDb = await createDb({ ...(await localAccountConfig(appId, serverUrl)) });
    const rows = await waitForRows(
      newDb,
      afterApp.records
        .where({ id: record.id })
        .include({ owner: true, members: true, reviewer: true }),
      (rows) =>
        rows.length === 1 &&
        (rows[0]?.owner as { name?: string } | undefined)?.name === "Existing owner",
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
  const users = { columns: { name: { type: "string", default: "Unnamed" } } };
  const columns = {
    ownerId: "uuid",
    status: { type: "string", default: "draft" },
    enabled: { type: "boolean", default: false },
    tags: { array: "string", default: ["initial"] },
    sequence: { type: "bigint", default: "9223372036854775806" },
    checkpoints: {
      array: { array: "bigint" },
      default: [["9223372036854775806", "-9223372036854775807"], []],
    },
    payload: { type: "bytes", default: "AAF//w==" },
    createdAt: { type: "timestamp", default: 1767323045678 },
    metadata: {
      json: true,
      default: { archived: false, labels: ["initial"], nested: { count: 3, value: null } },
    },
  };
  // Not `records`: its TS type `Record` would clash with the built-in.
  const before = { users, items: { columns } };
  const after = {
    users,
    items: { columns, relations: { owner: { rel: "users", column: "ownerId" } } },
  };
  const beforeFile: TestAppFile = { format: 1, tables: before };
  const beforeApp = testApp(beforeFile);
  // `migrations create` after adding the relation: the previous version moves
  // into history, with a migration to the current one.
  const stub = await migrationsCreate(
    JSON.stringify({ format: 1, tables: after }),
    await generateTs(JSON.stringify(beforeFile)),
    "references",
  );
  expect(stub).toMatchObject({ changed: true, todos: [] });
  const afterApp = testApp(stub.app);
  const permissionsBefore = allowAll(["users", "items"], ["read", "insert"]);
  const permissionsAfter = allowAll(["users", "items"], ["read", "insert"]);
  const root = await mkdtemp(join(tmpdir(), "jazz-default-relation-e2e-"));
  let server: LocalJazzServerHandle | undefined;
  let front: TestJazz | undefined;
  let oldDb: Db | undefined, newDb: Db | undefined;
  try {
    server = await startLocalJazzServer({
      dataDir: join(root, "data"),
    });
    const { appId, adminSecret, backendSecret } = server;
    const catalogue = { appId, adminSecret, serverUrl: server.url };
    const deployed = await deploy({
      ...catalogue,
      app: beforeApp,
      permissions: permissionsBefore,
    });
    front = await startTestJazz({ app: beforeApp, permissions: permissionsBefore, core: server });
    oldDb = await createDb(await localAccountConfig(appId, front.url));
    const owner = await oldDb
      .insert(beforeApp.users, { name: "Existing owner" })
      .wait({ tier: "global" });
    const existing = await oldDb
      .insert(beforeApp.items, {
        ownerId: owner.id,
        status: "published",
        enabled: true,
        tags: ["retained"],
      })
      .wait({ tier: "global" });
    const fromHash = deployed.schema.hash;
    const toHash = boundApp(afterApp.wasmSchema)!.schema.current;
    expect(toHash).not.toBe(fromHash);
    expect(boundApp(beforeApp.wasmSchema)!.schema.current).toBe(fromHash);
    // The new version ships the deployed one, under the same hash.
    expect(boundApp(afterApp.wasmSchema)!.schema.schemas.map((version) => version.hash)).toEqual(
      expect.arrayContaining([fromHash, toHash]),
    );
    // The migration ships the previous version's schema, defaults included:
    // a shipped version whose defaults differ from what its hash names is
    // refused, and nothing is published.
    // The loader refuses it locally, and the server refuses a deployment body
    // that ships it anyway.
    const compiled = compileTestApp(stub.app);
    const shippedBefore = compiled.schema.schemas.find((version) => version.hash === fromHash);
    expect(shippedBefore).toBeDefined();
    const statusOf = (schema: unknown) =>
      (schema as { tables: WasmSchema }).tables.items!.columns.find(
        (column) => column.name === "status",
      )!;
    expect(statusOf(shippedBefore!.schema).default).toEqual({ type: "Text", value: "draft" });
    for (const tamper of [
      (status: { default?: unknown }) => delete status.default,
      (status: { default?: unknown }) => (status.default = { type: "Text", value: "tampered" }),
    ]) {
      const invalid = structuredClone(compiled);
      tamper(statusOf(invalid.schema.schemas.find((version) => version.hash === fromHash)!.schema));
      const invalidApp = bindApp(invalid as never);
      expect(() => testPermissions(invalidApp, permissionsAfter)).toThrow(
        new RegExp(`schema ${fromHash} doesn't match its content`),
      );
      expect((await fetchMigrationGraph(catalogue)).migrations).toEqual([]);
    }
    // The deployment body `deploy` would publish, sent whole (deploy leaves
    // out schemas the server already stores), with the previous version or
    // the lens tampered.
    const { deployment } = await loadBackendAppFor(afterApp, permissionsAfter);
    const body = deployment as DeploymentBody;
    expect(body.schemas.map(({ hash }) => hash)).toContain(fromHash);
    const tamperedBodies: [DeploymentBody, RegExp, string][] = [];
    for (const tamper of [
      (status: { default?: unknown }) => delete status.default,
      (status: { default?: unknown }) => (status.default = { type: "Text", value: "tampered" }),
    ]) {
      const invalid = structuredClone(body);
      tamper(statusOf(invalid.schemas.find(({ hash }) => hash === fromHash)!.schema));
      tamperedBodies.push([
        invalid,
        new RegExp(`schema ${fromHash} does not match its content hash`),
        "schema_hash_mismatch",
      ]);
    }
    const tamperedLens = structuredClone(body);
    tamperedLens.migrations[0]!.forward = [
      { table: "items", operations: [{ type: "rename", column: "status", value: "state" }] },
    ];
    tamperedBodies.push([
      tamperedLens,
      /the migration does not produce the columns of table "items"/,
      "invalid_migration",
    ]);
    for (const [invalid, message, code] of tamperedBodies) {
      const refused = publishDeployment(server.url, catalogue, invalid);
      await expect(refused).rejects.toThrow(message);
      await expect(refused).rejects.toBeInstanceOf(DeploymentError);
      await expect(refused).rejects.toMatchObject({ code });
      expect((await fetchMigrationGraph(catalogue)).migrations).toEqual([]);
    }
    expect(
      await deploy({
        ...catalogue,
        app: afterApp,
        permissions: permissionsAfter,
      }),
    ).toMatchObject({ changed: true, published: { migrations: [{ fromHash, toHash }] } });
    expect((await fetchMigrationGraph(catalogue)).migrations).toContainEqual(
      expect.objectContaining({ fromHash, toHash }),
    );

    await oldDb.shutdown();
    oldDb = undefined;
    await front.stop();
    front = undefined;
    await server.stop();
    server = await startLocalJazzServer({
      appId,
      adminSecret,
      backendSecret,
      dataDir: join(root, "data"),
    });
    for (const [schemaHash, expected] of [
      [fromHash, beforeApp.wasmSchema],
      [toHash, afterApp.wasmSchema],
    ] as const) {
      const stored = await fetchStoredWasmSchema(server.url, { appId, adminSecret, schemaHash });
      expect(wasmSchemasEqual(stored.schema, expected)).toBe(true);
    }
    // The app as it ships after `migrations create`: its jazz.app.json now
    // holds the previous version and the generated migration.
    const shippedApp = afterApp;
    front = await startTestJazz({ app: shippedApp, permissions: permissionsAfter, core: server });
    newDb = await createDb(await localAccountConfig(appId, front.url));
    const rows = await waitForRows(
      newDb,
      shippedApp.items.where({ id: existing.id }).include({ owner: true }),
      (rows) =>
        rows.length === 1 &&
        (rows[0]?.owner as { name?: string } | undefined)?.name === "Existing owner",
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
    const defaultOwner = await newDb.insert(shippedApp.users, {}).wait({ tier: "global" });
    const created = await newDb
      .insert(shippedApp.items, { ownerId: defaultOwner.id })
      .wait({ tier: "global" });
    const defaultRows = await waitForRows(
      newDb,
      shippedApp.items.where({ id: created.id }).include({ owner: true }),
      (rows) =>
        rows.length === 1 && (rows[0]?.owner as { name?: string } | undefined)?.name === "Unnamed",
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
    await front?.stop();
    await server?.stop();
    await rm(root, { recursive: true, force: true });
  }
}, 60_000);

it("a client shipping the app's migrations writes a renamed column where the server keeps it", async () => {
  const before = { notes: { columns: { title: "string" } } };
  const after = { notes: { columns: { body: "string" } } };
  const beforeApp = testApp({ tables: before }),
    afterApp = testApp({
      tables: after,
      history: { v1: { tables: before } },
      migrations: [{ from: "v1", migrate: { notes: { body: { renameFrom: "title" } } } }],
    });
  const allowBefore = allowAll(["notes"], ["read", "insert"]);
  const allowAfter = allowAll(["notes"], ["read", "insert"]);
  const server = await startTestJazz({
    app: beforeApp,
    permissions: allowBefore,
    coreOptions: { inMemory: true },
  });
  let oldDb: Db | undefined, newDb: Db | undefined;
  try {
    const { appId, url: serverUrl } = server;
    await server.redeploy({ app: afterApp, permissions: allowAfter });
    newDb = await createDb({ ...(await localAccountConfig(appId, serverUrl)) });
    oldDb = await createDb({ ...(await localAccountConfig(appId, serverUrl)) });
    const created = await newDb
      .insert(afterApp.notes, { body: "written by v2" })
      .wait({ tier: "global" });
    const rows = await waitForRows(
      oldDb,
      beforeApp.notes.where({ id: { eq: created.id } }),
      (rows) => rows.length === 1,
    );
    expect(rows).toEqual([{ id: created.id, title: "written by v2" }]);
  } finally {
    await oldDb?.shutdown();
    await newDb?.shutdown();
    await server.stop();
  }
}, 60_000);
