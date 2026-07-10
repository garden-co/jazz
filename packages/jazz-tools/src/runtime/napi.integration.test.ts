import { randomUUID } from "node:crypto";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { beforeAll, describe, expect, it, vi } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import { type Row } from "./client.js";
import type { Db, QueryBuilder, TableProxy } from "./db.js";
import { translateQuery } from "./query-adapter.js";
import { loadCompiledSchema, type LoadedSchemaProject } from "../schema-loader.js";
import { deploy, startLocalJazzServer } from "../testing/index.js";
import {
  createPersistentNapiNativeRuntimeAdapter,
  loadNapiModule,
} from "./testing/napi-runtime-test-utils.js";

type RuntimeRowWithTransactionId = Row & {
  transactionId: string;
};

type SimpleTodo = {
  id: string;
  title: string;
  done: boolean;
};

type SimpleTodoInit = {
  title: string;
  done: boolean;
};

type TimestampProject = {
  id: string;
  name: string;
  created_at: Date;
  updated_at: Date;
};

type TimestampProjectInit = {
  name: string;
  created_at: Date;
  updated_at: Date;
};

type ByteChunk = {
  id: string;
  label: string;
  data: Uint8Array;
};

type ByteChunkInit = {
  label: string;
  data: Uint8Array;
};

type StoredFile = {
  id: string;
  name: string;
  mime_type: string;
  data: Uint8Array;
};

type StoredFileInit = {
  name: string;
  mime_type: string;
  data: Uint8Array;
};

type PolicyTodo = {
  id: string;
  title: string;
  done: boolean;
  description?: string;
  parentId?: string;
  projectId?: string;
  owner_id: string;
};

type PolicyTodoInit = {
  title: string;
  done: boolean;
  description?: string;
  parentId?: string;
  projectId?: string;
  owner_id: string;
};

const TEST_SCHEMA: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
    ],
  },
};

const TIMESTAMP_SCHEMA: WasmSchema = {
  projects: {
    columns: [
      { name: "name", column_type: { type: "Text" }, nullable: false },
      { name: "created_at", column_type: { type: "Timestamp" }, nullable: false },
      { name: "updated_at", column_type: { type: "Timestamp" }, nullable: false },
    ],
  },
};

const BYTEA_SCHEMA: WasmSchema = {
  byte_chunks: {
    columns: [
      { name: "label", column_type: { type: "Text" }, nullable: false },
      { name: "data", column_type: { type: "Bytea" }, nullable: false },
    ],
  },
};

const FILE_STORAGE_SCHEMA: WasmSchema = {
  files: {
    columns: [
      { name: "name", column_type: { type: "Text" }, nullable: false },
      { name: "mime_type", column_type: { type: "Text" }, nullable: false },
      { name: "data", column_type: { type: "Bytea" }, nullable: false },
    ],
  },
};

let todoServerProjectPromise: Promise<LoadedSchemaProject> | null = null;

async function loadTodoServerProject(): Promise<LoadedSchemaProject> {
  if (!todoServerProjectPromise) {
    todoServerProjectPromise = loadCompiledSchema(TODO_SERVER_SCHEMA_DIR);
  }
  return await todoServerProjectPromise;
}

const simpleTodosTable: TableProxy<SimpleTodo, SimpleTodoInit> = {
  _table: "todos",
  _schema: TEST_SCHEMA,
  _rowType: undefined as unknown as SimpleTodo,
  _initType: undefined as unknown as SimpleTodoInit,
};

const allTodosQuery: QueryBuilder<SimpleTodo> = {
  _table: "todos",
  _schema: TEST_SCHEMA,
  _rowType: undefined as unknown as SimpleTodo,
  _build() {
    return JSON.stringify({
      table: "todos",
      conditions: [],
      includes: {},
      orderBy: [],
      offset: 0,
    });
  },
};

const timestampProjectsTable: TableProxy<TimestampProject, TimestampProjectInit> = {
  _table: "projects",
  _schema: TIMESTAMP_SCHEMA,
  _rowType: undefined as unknown as TimestampProject,
  _initType: undefined as unknown as TimestampProjectInit,
};

type WhereTable<Row, Init> = TableProxy<Row, Init> & {
  where(conditions: Record<string, unknown>): QueryBuilder<Row>;
};

function makeWhereQuery<T>(
  table: string,
  schema: WasmSchema,
  conditions: Record<string, unknown>,
): QueryBuilder<T> {
  return {
    _table: table,
    _schema: schema,
    _rowType: undefined as unknown as T,
    _build() {
      return JSON.stringify({
        table,
        conditions: Object.entries(conditions).map(([column, value]) => ({
          column,
          op: "eq",
          value,
        })),
        includes: {},
        orderBy: [],
        offset: 0,
      });
    },
  };
}

function makeWhereTable<Row, Init>(table: string, schema: WasmSchema): WhereTable<Row, Init> {
  return {
    _table: table,
    _schema: schema,
    _rowType: undefined as unknown as Row,
    _initType: undefined as unknown as Init,
    where(conditions: Record<string, unknown>) {
      return makeWhereQuery<Row>(table, schema, conditions);
    },
  };
}

const byteChunksTable = makeWhereTable<ByteChunk, ByteChunkInit>("byte_chunks", BYTEA_SCHEMA);
const filesTable = makeWhereTable<StoredFile, StoredFileInit>("files", FILE_STORAGE_SCHEMA);

function makePolicyTodosTable(schema: WasmSchema): TableProxy<PolicyTodo, PolicyTodoInit> {
  return {
    _table: "todos",
    _schema: schema,
    _rowType: undefined as unknown as PolicyTodo,
    _initType: undefined as unknown as PolicyTodoInit,
  };
}

function makePolicyTodoByIdQuery(schema: WasmSchema, id: string): QueryBuilder<PolicyTodo> {
  return {
    _table: "todos",
    _schema: schema,
    _rowType: undefined as unknown as PolicyTodo,
    _build() {
      return JSON.stringify({
        table: "todos",
        conditions: [{ column: "id", op: "eq", value: id }],
        includes: {},
        orderBy: [],
        offset: 0,
      });
    },
  };
}

const BASIC_SCHEMA_DIR = fileURLToPath(new URL("../testing/fixtures/basic", import.meta.url));
const TODO_SERVER_SCHEMA_DIR = fileURLToPath(
  new URL("../../../../examples/todo-server-ts", import.meta.url),
);

beforeAll(async () => {
  await loadNapiModule();
});

async function waitForQueryRows<T>(
  db: Db,
  query: QueryBuilder<T>,
  predicate: (rows: T[]) => boolean,
  timeoutMs = 20_000,
  queryOptions: { tier?: "local" | "edge" | "global" } = { tier: "edge" },
): Promise<T[]> {
  const deadline = Date.now() + timeoutMs;
  let lastRows: T[] = [];
  let lastError: unknown = undefined;

  while (Date.now() < deadline) {
    try {
      const rows = await db.all(query, queryOptions);
      if (predicate(rows)) return rows;
      lastRows = rows;
    } catch (error) {
      lastError = error;
    }

    await new Promise((resolve) => setTimeout(resolve, 150));
  }

  const lastErrorMessage =
    lastError instanceof Error ? lastError.message : lastError ? String(lastError) : "none";
  throw new Error(
    `timed out waiting for rows; lastRows=${JSON.stringify(lastRows)}, lastError=${lastErrorMessage}`,
  );
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  let timeoutId: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_, reject) => {
        timeoutId = setTimeout(() => {
          reject(new Error(`${label} after ${timeoutMs}ms`));
        }, timeoutMs);
      }),
    ]);
  } finally {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }
  }
}

async function settleAsyncSyncWork(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 50));
}

async function createTempDir(prefix: string): Promise<string> {
  return await mkdtemp(join(tmpdir(), prefix));
}

type TempRuntimeData = {
  dataRoot: string;
  dataPath: string;
};

async function createTempRuntimeData(prefix: string): Promise<TempRuntimeData> {
  const dataRoot = await createTempDir(prefix);
  return {
    dataRoot,
    dataPath: join(dataRoot, "runtime.db"),
  };
}

async function cleanupTempRuntimeData(data: TempRuntimeData | null): Promise<void> {
  if (!data) {
    return;
  }
  await rm(data.dataRoot, { recursive: true, force: true });
}

describe("NAPI integration", () => {
  it("supports oversized indexed persistent mutations from JS callers", async () => {
    const dataDir = await createTempDir("jazz-napi-large-index-");
    const dataPath = join(dataDir, "jazz.db");
    const runtime = (await createPersistentNapiNativeRuntimeAdapter(TEST_SCHEMA, dataPath, {
      appId: `napi-large-index-${randomUUID()}`,
      env: "test",
      userBranch: "main",
    })) as unknown as {
      insert(table: string, values: unknown): RuntimeRowWithTransactionId;
      update(
        table: string,
        objectId: string,
        updates: Record<string, unknown>,
      ): { transactionId: string };
      query(queryJson: string): Promise<Row[]>;
      close(): void;
    };

    const oversizedTitle = "x".repeat(40_000);
    const updatedOversizedTitle = "y".repeat(45_000);
    const queryJson = translateQuery(allTodosQuery._build(), TEST_SCHEMA);

    try {
      const insertedRow = runtime.insert("todos", {
        title: { type: "Text", value: oversizedTitle },
        done: { type: "Boolean", value: false },
      });
      expect(insertedRow.transactionId).toEqual(expect.any(String));

      let rows = await runtime.query(queryJson);
      expect(rows).toHaveLength(1);
      expect(rows[0]).toMatchObject({ id: insertedRow.id });
      expect(rows[0]?.values[0]).toEqual({ type: "Text", value: oversizedTitle });
      expect(rows[0]?.values[1]).toEqual({ type: "Boolean", value: false });

      const secondRow = runtime.insert("todos", {
        title: { type: "Text", value: "kept title" },
        done: { type: "Boolean", value: false },
      });
      expect(secondRow.transactionId).toEqual(expect.any(String));

      const updateResult = runtime.update("todos", secondRow.id, {
        title: { type: "Text", value: updatedOversizedTitle },
      });
      expect(updateResult.transactionId).toEqual(expect.any(String));

      rows = await runtime.query(queryJson);
      expect(rows).toHaveLength(2);

      const insertedOversized = rows.find((row) => row.id === insertedRow.id);
      expect(insertedOversized).toBeDefined();
      expect(insertedOversized?.values[0]).toEqual({ type: "Text", value: oversizedTitle });
      expect(insertedOversized?.values[1]).toEqual({ type: "Boolean", value: false });

      const updatedOversized = rows.find((row) => row.id === secondRow.id);
      expect(updatedOversized).toBeDefined();
      expect(updatedOversized?.values[0]).toEqual({
        type: "Text",
        value: updatedOversizedTitle,
      });
      expect(updatedOversized?.values[1]).toEqual({ type: "Boolean", value: false });
    } finally {
      runtime.close();
      await rm(dataDir, { recursive: true, force: true });
    }
  }, 20_000);

  it("applies createJazzContext(...).forSession() mutations through high-level Db APIs", async () => {
    const appId = randomUUID();
    const backendSecret = "napi-session-secret";
    const adminSecret = "napi-session-admin-secret";
    let runtimeData: TempRuntimeData | null = null;
    const server = await startLocalJazzServer({
      appId,
      backendSecret,
      adminSecret,
    });
    let context: {
      asBackend(): Db;
      forSession(session: { user_id: string; claims: Record<string, unknown> }): Db;
      shutdown(): Promise<void>;
    } | null = null;

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");

      await deploy({
        serverUrl: server.url,
        appId,
        adminSecret,
        schemaDir: TODO_SERVER_SCHEMA_DIR,
      });
      const todoServerProject = await loadTodoServerProject();
      const todoServerSchema = todoServerProject.wasmSchema;
      const policyTodosTable = makePolicyTodosTable(todoServerSchema);

      runtimeData = await createTempRuntimeData("jazz-napi-session-runtime-");
      context = createJazzContext({
        appId,
        app: { wasmSchema: todoServerSchema },
        permissions: todoServerProject.permissions ?? {},
        driver: { type: "persistent", dataPath: runtimeData.dataPath },
        serverUrl: server.url,
        backendSecret,
        env: "test",
        userBranch: "main",
      });
      await settleAsyncSyncWork();

      const backendDb = context.asBackend();
      const aliceDb = context.forSession({
        user_id: "alice",
        claims: { role: "editor", team: "alpha" },
      });

      const createdTodo = await withTimeout(
        aliceDb
          .insert(policyTodosTable, {
            title: "session-created-item",
            done: false,
            description: "created via forSession",
            owner_id: "alice",
          })
          .wait({ tier: "edge" }),
        10_000,
        "session insert timed out",
      );

      await vi.waitFor(
        async () => {
          expect(
            await withTimeout(
              backendDb.one(makePolicyTodoByIdQuery(todoServerSchema, createdTodo.id), {
                tier: "edge",
              }),
              10_000,
              "backend session read timed out",
            ),
          ).toMatchObject({
            id: createdTodo.id,
            title: "session-created-item",
            done: false,
            owner_id: "alice",
          });
        },
        { timeout: 20_000 },
      );

      expect(() =>
        aliceDb.insert(policyTodosTable, {
          title: "session-policy-denied",
          done: false,
          description: "",
          owner_id: "bob",
        }),
      ).toThrow('Insert failed: WriteError("policy denied INSERT on table todos")');

      await withTimeout(
        aliceDb.update(policyTodosTable, createdTodo.id, { done: true }).wait({ tier: "edge" }),
        10_000,
        "session update timed out",
      );

      await vi.waitFor(
        async () => {
          expect(
            await withTimeout(
              backendDb.one(makePolicyTodoByIdQuery(todoServerSchema, createdTodo.id), {
                tier: "edge",
              }),
              10_000,
              "backend session update read timed out",
            ),
          ).toMatchObject({
            id: createdTodo.id,
            done: true,
          });
        },
        { timeout: 20_000 },
      );

      await withTimeout(
        aliceDb.delete(policyTodosTable, createdTodo.id).wait({ tier: "edge" }),
        10_000,
        "session delete timed out",
      );

      await vi.waitFor(
        async () => {
          expect(
            await withTimeout(
              backendDb.one(makePolicyTodoByIdQuery(todoServerSchema, createdTodo.id), {
                tier: "edge",
              }),
              10_000,
              "backend session delete read timed out",
            ),
          ).toBeNull();
        },
        { timeout: 20_000 },
      );
    } finally {
      if (context) {
        await context.shutdown();
      }
      await settleAsyncSyncWork();
      await cleanupTempRuntimeData(runtimeData);
      await server.stop();
    }
  }, 60_000);

  it("resolves global waits for backend context writes through the local server route", async () => {
    const appId = randomUUID();
    const backendSecret = "napi-global-wait-secret";
    const adminSecret = "napi-global-wait-admin-secret";
    let runtimeData: TempRuntimeData | null = null;
    const server = await startLocalJazzServer({
      appId,
      backendSecret,
      adminSecret,
    });
    let context: {
      asBackend(): Db;
      shutdown(): Promise<void>;
    } | null = null;

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");

      await deploy({
        serverUrl: server.url,
        appId,
        adminSecret,
        schemaDir: TODO_SERVER_SCHEMA_DIR,
      });
      const todoServerProject = await loadTodoServerProject();
      const todoServerSchema = todoServerProject.wasmSchema;
      const policyTodosTable = makePolicyTodosTable(todoServerSchema);

      runtimeData = await createTempRuntimeData("jazz-napi-global-wait-runtime-");
      context = createJazzContext({
        appId,
        app: { wasmSchema: todoServerSchema },
        permissions: todoServerProject.permissions ?? {},
        driver: { type: "persistent", dataPath: runtimeData.dataPath },
        serverUrl: server.url,
        backendSecret,
        adminSecret,
        env: "test",
        userBranch: "main",
        tier: "global",
      });
      await settleAsyncSyncWork();

      const backendDb = context.asBackend();
      const createdTodo = await withTimeout(
        backendDb
          .insert(policyTodosTable, {
            title: "global-wait-item",
            done: false,
            description: "global wait repro",
            owner_id: "backend",
          })
          .wait({ tier: "global" }),
        15_000,
        "backend global insert wait timed out",
      );

      expect(createdTodo).toMatchObject({
        title: "global-wait-item",
        done: false,
        owner_id: "backend",
      });
    } finally {
      if (context) {
        await context.shutdown();
      }
      await settleAsyncSyncWork();
      await cleanupTempRuntimeData(runtimeData);
      await server.stop();
    }
  }, 60_000);

  it("publishes inherited seeded-reachability permissions through the local server route", async () => {
    const appId = randomUUID();
    const backendSecret = "napi-pilot-app-permissions-secret";
    const adminSecret = "napi-pilot-app-permissions-admin-secret";
    const schemaDir = await createTempDir("jazz-napi-pilot-app-permissions-schema-");
    const server = await startLocalJazzServer({
      appId,
      backendSecret,
      adminSecret,
    });

    try {
      const publicApiImport = pathToFileURL(join(process.cwd(), "src/index.ts")).href;
      await writeFile(
        join(schemaDir, "schema.ts"),
        `
          import { schema as s } from ${JSON.stringify(publicApiImport)};

          const schema = {
            team: s.table({
              identity_key: s.string(),
            }),
            team_entry: s.table({
              team_id: s.ref("team"),
              target_id: s.ref("team"),
              administrator: s.boolean(),
            }),
            dropdowns: s.table({
              name: s.string(),
            }),
            dropdowns_access_edges: s.table({
              resource_id: s.ref("dropdowns"),
              team_id: s.ref("team"),
              grant_role: s.string(),
              administrator: s.boolean(),
            }),
            dropdown_entry: s.table({
              dropdowns_id: s.ref("dropdowns"),
              label: s.string(),
            }),
          };

          type AppSchema = s.Schema<typeof schema>;
          export const app: s.App<AppSchema> = s.defineApp(schema);
        `,
      );
      await writeFile(
        join(schemaDir, "permissions.ts"),
        `
          import { schema as s } from ${JSON.stringify(publicApiImport)};
          import { app } from "./schema.js";

          export default s.definePermissions(app, ({ policy, allowedTo, session }) => {
            const directlyReachableTeams = policy.team_entry
              .where({ team_id: session.user_id })
              .hopTo("target");
            const memberReachableTeams = directlyReachableTeams.gather({
              step: ({ current }) =>
                policy.team_entry
                  .where({ team_id: current, administrator: false })
                  .hopTo("target"),
              maxDepth: 32,
            });

            policy.dropdowns.allowRead.where((dropdown) =>
              policy.exists(
                memberReachableTeams.hopTo("dropdowns_access_edgesViaTeam").where({
                  "dropdowns_access_edges.resource_id": dropdown.id,
                  grant_role: { in: ["EDITOR"] },
                  administrator: false,
                }),
              ),
            );
            policy.dropdowns.allowInsert.where({});
            policy.dropdowns.allowUpdate
              .whereOld((dropdown) =>
                policy.exists(
                  memberReachableTeams.hopTo("dropdowns_access_edgesViaTeam").where({
                    "dropdowns_access_edges.resource_id": dropdown.id,
                    grant_role: { in: ["EDITOR"] },
                    administrator: false,
                  }),
                ),
              )
              .whereNew({});
            policy.dropdowns.allowDelete.where({});

            policy.dropdowns_access_edges.allowRead.where({});
            policy.dropdowns_access_edges.allowInsert.where({});
            policy.dropdowns_access_edges.allowUpdate.where({});
            policy.dropdowns_access_edges.allowDelete.where({});

            policy.dropdown_entry.allowRead.where(allowedTo.read("dropdowns_id"));
            policy.dropdown_entry.allowInsert.where(allowedTo.update("dropdowns_id"));
            policy.dropdown_entry.allowUpdate
              .whereOld(allowedTo.update("dropdowns_id"))
              .whereNew(allowedTo.update("dropdowns_id"));
            policy.dropdown_entry.allowDelete.where(allowedTo.update("dropdowns_id"));
          });
        `,
      );

      await deploy({
        serverUrl: server.url,
        appId,
        adminSecret,
        schemaDir,
      });
    } finally {
      await rm(schemaDir, { recursive: true, force: true });
      await server.stop();
    }
  }, 60_000);

  it("can update an optional row field to null", async () => {
    const appId = randomUUID();
    const backendSecret = "napi-null-update-secret";
    const adminSecret = "napi-null-update-admin-secret";
    let runtimeData: TempRuntimeData | null = null;
    const server = await startLocalJazzServer({
      appId,
      backendSecret,
      adminSecret,
    });
    let context: {
      asBackend(): Db;
      forSession(session: { user_id: string; claims: Record<string, unknown> }): Db;
      shutdown(): Promise<void>;
    } | null = null;

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");

      await deploy({
        serverUrl: server.url,
        appId,
        adminSecret,
        schemaDir: TODO_SERVER_SCHEMA_DIR,
      });
      const todoServerProject = await loadTodoServerProject();
      const todoServerSchema = todoServerProject.wasmSchema;
      const policyTodosTable = makePolicyTodosTable(todoServerSchema);

      runtimeData = await createTempRuntimeData("jazz-napi-null-update-runtime-");
      context = createJazzContext({
        appId,
        app: { wasmSchema: todoServerSchema },
        permissions: todoServerProject.permissions ?? {},
        driver: { type: "persistent", dataPath: runtimeData.dataPath },
        serverUrl: server.url,
        backendSecret,
        env: "test",
        userBranch: "main",
      });
      await settleAsyncSyncWork();

      const aliceDb = context.forSession({
        user_id: "alice",
        claims: { role: "editor", team: "alpha" },
      });

      const createdTodo = await aliceDb
        .insert(policyTodosTable, {
          title: "nullable-description-repro",
          done: false,
          description: "server-original",
          owner_id: "alice",
        })
        .wait({ tier: "edge" });

      const nullUpdate = aliceDb.update(policyTodosTable, createdTodo.id, {
        description: null,
      } as unknown as Partial<PolicyTodoInit>);
      await nullUpdate.wait({ tier: "local" });

      const rowAfterUpdate = await aliceDb.one(
        makePolicyTodoByIdQuery(todoServerSchema, createdTodo.id),
        {
          tier: "local",
          localUpdates: "immediate",
        },
      );
      expect(rowAfterUpdate).not.toBeNull();
      expect(rowAfterUpdate?.description ?? null).toBeNull();
    } finally {
      if (context) {
        await context.shutdown();
      }
      await settleAsyncSyncWork();
      await cleanupTempRuntimeData(runtimeData);
      await server.stop();
    }
  }, 60_000);

  it("syncs edge create/update/delete flows between real backend NAPI contexts", async () => {
    const appId = randomUUID();
    const backendSecret = "napi-e2e-backend-secret";
    const adminSecret = "napi-e2e-admin-secret";
    let writerRuntimeData: TempRuntimeData | null = null;
    let readerRuntimeData: TempRuntimeData | null = null;
    const server = await startLocalJazzServer({
      appId,
      backendSecret,
      adminSecret,
    });
    let writerContext: {
      asBackend(): Db;
      shutdown(): Promise<void>;
    } | null = null;
    let readerContext: {
      asBackend(): Db;
      shutdown(): Promise<void>;
    } | null = null;

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");

      await deploy({
        serverUrl: server.url,
        appId,
        adminSecret,
        schemaDir: BASIC_SCHEMA_DIR,
      });

      writerRuntimeData = await createTempRuntimeData("jazz-napi-sync-writer-");
      writerContext = createJazzContext({
        appId,
        app: { wasmSchema: TEST_SCHEMA },
        permissions: {},
        driver: { type: "persistent", dataPath: writerRuntimeData.dataPath },
        serverUrl: server.url,
        backendSecret,
        adminSecret,
      });
      readerRuntimeData = await createTempRuntimeData("jazz-napi-sync-reader-");
      readerContext = createJazzContext({
        appId,
        app: { wasmSchema: TEST_SCHEMA },
        permissions: {},
        driver: { type: "persistent", dataPath: readerRuntimeData.dataPath },
        serverUrl: server.url,
        backendSecret,
        adminSecret,
      });
      await settleAsyncSyncWork();

      const writer = writerContext.asBackend();
      const reader = readerContext.asBackend();

      await waitForQueryRows(reader, allTodosQuery, (rows) => rows.length === 0);

      const createdRow = await writer
        .insert(simpleTodosTable, { title: "napi-shared-item", done: false })
        .wait({ tier: "edge" });
      const rowId = createdRow.id;

      const rowsAfterCreate = await waitForQueryRows(reader, allTodosQuery, (rows) =>
        rows.some((row) => row.id === rowId),
      );
      const replicatedRow = rowsAfterCreate.find((row) => row.id === rowId);
      expect(replicatedRow).toMatchObject({
        id: rowId,
        title: "napi-shared-item",
        done: false,
      });

      await writer.update(simpleTodosTable, rowId, { done: true }).wait({ tier: "edge" });

      const rowsAfterUpdate = await waitForQueryRows(reader, allTodosQuery, (rows) => {
        const row = rows.find((entry) => entry.id === rowId);
        return row?.done === true;
      });
      const updatedRow = rowsAfterUpdate.find((row) => row.id === rowId);
      expect(updatedRow?.done).toBe(true);

      await writer.delete(simpleTodosTable, rowId).wait({ tier: "edge" });
      await settleAsyncSyncWork();
      await waitForQueryRows(
        writer,
        allTodosQuery,
        (rows) => !rows.some((row) => row.id === rowId),
      );
      await readerContext.shutdown();
      await cleanupTempRuntimeData(readerRuntimeData);
      readerRuntimeData = await createTempRuntimeData("jazz-napi-sync-reader-reopen-");
      readerContext = createJazzContext({
        appId,
        app: { wasmSchema: TEST_SCHEMA },
        permissions: {},
        driver: { type: "persistent", dataPath: readerRuntimeData.dataPath },
        serverUrl: server.url,
        backendSecret,
        adminSecret,
      });
      await settleAsyncSyncWork();
      const refreshedReader = readerContext.asBackend();
      await waitForQueryRows(
        refreshedReader,
        allTodosQuery,
        (rows) => !rows.some((row) => row.id === rowId),
      );
    } finally {
      if (writerContext) {
        await writerContext.shutdown();
      }
      if (readerContext) {
        await readerContext.shutdown();
      }
      await settleAsyncSyncWork();
      await cleanupTempRuntimeData(writerRuntimeData);
      await cleanupTempRuntimeData(readerRuntimeData);
      await server.stop();
    }
  }, 60_000);

  it("reopens persistent backend runtimes cleanly and retains local data", async () => {
    const dataRoot = await createTempDir("jazz-napi-persistent-");
    const dataPath = join(dataRoot, "runtime.db");
    const appId = randomUUID();
    let writerContext: {
      db(): Db;
      shutdown(): Promise<void>;
    } | null = null;
    let reopenedContext: {
      db(): Db;
      shutdown(): Promise<void>;
    } | null = null;

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");

      writerContext = createJazzContext({
        appId,
        app: { wasmSchema: TEST_SCHEMA },
        permissions: {},
        driver: { type: "persistent", dataPath },
      });

      const writer = writerContext.db();
      const createdRow = await writer
        .insert(simpleTodosTable, { title: "persisted-local-item", done: false })
        .wait({ tier: "local" });
      const rowId = createdRow.id;

      await waitForQueryRows(
        writer,
        allTodosQuery,
        (rows) => rows.some((row) => row.id === rowId),
        10_000,
        { tier: "local" },
      );

      await writerContext.shutdown();
      writerContext = null;
      await settleAsyncSyncWork();

      reopenedContext = createJazzContext({
        appId,
        app: { wasmSchema: TEST_SCHEMA },
        permissions: {},
        driver: { type: "persistent", dataPath },
      });

      const reopened = reopenedContext.db();
      const reopenedRows = await waitForQueryRows(
        reopened,
        allTodosQuery,
        (rows) => rows.some((row) => row.id === rowId),
        10_000,
        { tier: "local" },
      );

      const reopenedRow = reopenedRows.find((row) => row.id === rowId);
      expect(reopenedRow).toMatchObject({
        id: rowId,
        title: "persisted-local-item",
        done: false,
      });
    } finally {
      if (writerContext) {
        await writerContext.shutdown();
      }
      if (reopenedContext) {
        await reopenedContext.shutdown();
      }
      await rm(dataRoot, { recursive: true, force: true });
    }
  }, 30_000);

  it("accepts modern epoch-millisecond timestamps from the TS value converter on backend durable writes", async () => {
    const dataRoot = await createTempDir("jazz-napi-timestamp-");
    const dataPath = join(dataRoot, "runtime.db");
    const timestamp = 1773285322816;
    let context: {
      db(): Db;
      shutdown(): Promise<void>;
    } | null = null;

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");

      context = createJazzContext({
        appId: randomUUID(),
        app: { wasmSchema: TIMESTAMP_SCHEMA },
        permissions: {},
        driver: { type: "persistent", dataPath },
      });

      await expect(
        context
          .db()
          .insert(timestampProjectsTable, {
            name: "timestamp-probe",
            created_at: new Date(timestamp),
            updated_at: new Date(timestamp),
          })
          .wait({ tier: "local" }),
      ).resolves.toEqual({
        id: expect.any(String),
        name: "timestamp-probe",
        created_at: new Date(timestamp),
        updated_at: new Date(timestamp),
      });
    } finally {
      if (context) {
        await context.shutdown();
      }
      await rm(dataRoot, { recursive: true, force: true });
    }
  }, 30_000);

  it("accepts Uint8Array inserts for direct BYTEA columns through backend Db", async () => {
    const dataRoot = await createTempDir("jazz-napi-bytea-insert-");
    const dataPath = join(dataRoot, "runtime.skv");
    let context: {
      db(): Db;
      shutdown(): Promise<void>;
    } | null = null;

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");

      context = createJazzContext({
        appId: randomUUID(),
        app: { wasmSchema: BYTEA_SCHEMA },
        permissions: {},
        driver: { type: "persistent", dataPath },
      });

      const { value: created } = context.db().insert(byteChunksTable, {
        label: "alpha",
        data: new Uint8Array([1, 2, 3]),
      });

      expect(Array.from(created.data)).toEqual([1, 2, 3]);

      const reloaded = await context.db().one(byteChunksTable.where({ id: created.id }), {
        tier: "local",
      });

      expect(reloaded).not.toBeNull();
      expect(Array.from(reloaded?.data ?? [])).toEqual([1, 2, 3]);
    } finally {
      if (context) {
        await context.shutdown();
      }
      await rm(dataRoot, { recursive: true, force: true });
    }
  }, 30_000);

  it("accepts Uint8Array updates for direct BYTEA columns through backend Db", async () => {
    const dataRoot = await createTempDir("jazz-napi-bytea-update-");
    const dataPath = join(dataRoot, "runtime.skv");
    const appId = randomUUID();
    let context: {
      db(): Db;
      shutdown(): Promise<void>;
    } | null = null;
    let seedRuntime: {
      insert(table: string, values: unknown): { id: string };
      close(): void;
    } | null = null;

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");

      seedRuntime = (await createPersistentNapiNativeRuntimeAdapter(BYTEA_SCHEMA, dataPath, {
        appId,
        env: "dev",
        userBranch: "main",
        tier: "edge",
      })) as unknown as {
        insert(table: string, values: unknown): { id: string };
        close(): void;
      };

      // Seed via the raw N-API shape so this test isolates the update path.
      const created = seedRuntime.insert("byte_chunks", {
        label: { type: "Text", value: "beta" },
        data: { type: "Bytea", value: [1, 2, 3] },
      });
      seedRuntime.close();
      seedRuntime = null;

      context = createJazzContext({
        appId,
        app: { wasmSchema: BYTEA_SCHEMA },
        permissions: {},
        driver: { type: "persistent", dataPath },
      });

      context.db().update(byteChunksTable, created.id, {
        data: new Uint8Array([4, 5, 6]),
      });

      const reloaded = await context.db().one(byteChunksTable.where({ id: created.id }), {
        tier: "local",
      });

      expect(reloaded).not.toBeNull();
      expect(Array.from(reloaded?.data ?? [])).toEqual([4, 5, 6]);
    } finally {
      seedRuntime?.close();
      if (context) {
        await context.shutdown();
      }
      await rm(dataRoot, { recursive: true, force: true });
    }
  }, 30_000);

  it("stores Blob bytes in files.data when using createFileFromBlob", async () => {
    const dataRoot = await createTempDir("jazz-napi-bytea-file-");
    const dataPath = join(dataRoot, "runtime.skv");
    let context: {
      db(): Db;
      shutdown(): Promise<void>;
    } | null = null;

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");

      context = createJazzContext({
        appId: randomUUID(),
        app: { wasmSchema: FILE_STORAGE_SCHEMA },
        permissions: {},
        driver: { type: "persistent", dataPath },
      });

      const file = await context.db().createFileFromBlob(
        {
          files: filesTable,
        },
        new Blob([new Uint8Array([7, 8, 9])], { type: "application/octet-stream" }),
        { name: "probe.bin" },
      );

      const storedFile = await context.db().one(filesTable.where({ id: file.id }), {
        tier: "local",
      });

      expect(storedFile).not.toBeNull();
      expect(storedFile?.mime_type).toBe("application/octet-stream");
      expect(Array.from(storedFile?.data ?? [])).toEqual([7, 8, 9]);
    } finally {
      if (context) {
        await context.shutdown();
      }
      await rm(dataRoot, { recursive: true, force: true });
    }
  }, 30_000);
});
