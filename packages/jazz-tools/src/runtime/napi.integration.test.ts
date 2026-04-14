import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { beforeAll, describe, expect, it, vi } from "vitest";
import { serializeRuntimeSchema } from "../drivers/schema-wire.js";
import type { WasmSchema } from "../drivers/types.js";
import type { Row } from "./client.js";
import type { Db, QueryBuilder, TableProxy } from "./db.js";
import { translateQuery } from "./query-adapter.js";
import { loadCompiledSchema, type LoadedSchemaProject } from "../schema-loader.js";
import { pushSchemaCatalogue, startLocalJazzServer } from "../testing/local-jazz-server.js";
import { loadNapiModule } from "./testing/napi-runtime-test-utils.js";

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
  queryOptions: { tier?: "worker" | "edge" | "global" } = { tier: "edge" },
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
    const { NapiRuntime } = await loadNapiModule();
    const dataDir = await createTempDir("jazz-napi-large-index-");
    const dataPath = join(dataDir, "jazz.db");
    const runtime = new NapiRuntime(
      serializeRuntimeSchema(TEST_SCHEMA),
      `napi-large-index-${randomUUID()}`,
      "test",
      "main",
      dataPath,
    ) as unknown as {
      insert(table: string, values: unknown): Row;
      update(objectId: string, updates: Record<string, unknown>): void;
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

      let rows = await runtime.query(queryJson);
      expect(rows).toHaveLength(1);
      expect(rows[0]).toMatchObject({ id: insertedRow.id });
      expect(rows[0]?.values[0]).toEqual({ type: "Text", value: oversizedTitle });
      expect(rows[0]?.values[1]).toEqual({ type: "Boolean", value: false });

      const secondRow = runtime.insert("todos", {
        title: { type: "Text", value: "kept title" },
        done: { type: "Boolean", value: false },
      });

      runtime.update(secondRow.id, {
        title: { type: "Text", value: updatedOversizedTitle },
      });

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

      await pushSchemaCatalogue({
        serverUrl: server.url,
        appId,
        adminSecret,
        schemaDir: TODO_SERVER_SCHEMA_DIR,
        env: "test",
        userBranch: "main",
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
        aliceDb.insertDurable(
          policyTodosTable,
          {
            title: "session-created-item",
            done: false,
            description: "created via forSession",
            owner_id: "alice",
          },
          { tier: "edge" },
        ),
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

      await expect(
        aliceDb.insertDurable(
          policyTodosTable,
          {
            title: "session-policy-denied",
            done: false,
            description: "",
            owner_id: "bob",
          },
          { tier: "edge" },
        ),
      ).rejects.toThrow();

      await withTimeout(
        aliceDb.updateDurable(policyTodosTable, createdTodo.id, { done: true }, { tier: "edge" }),
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
        aliceDb.deleteDurable(policyTodosTable, createdTodo.id, { tier: "edge" }),
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

      await pushSchemaCatalogue({
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
      });
      readerRuntimeData = await createTempRuntimeData("jazz-napi-sync-reader-");
      readerContext = createJazzContext({
        appId,
        app: { wasmSchema: TEST_SCHEMA },
        permissions: {},
        driver: { type: "persistent", dataPath: readerRuntimeData.dataPath },
        serverUrl: server.url,
        backendSecret,
      });
      await settleAsyncSyncWork();

      const writer = writerContext.asBackend();
      const reader = readerContext.asBackend();

      await waitForQueryRows(reader, allTodosQuery, (rows) => rows.length === 0);

      const createdRow = await writer.insertDurable(
        simpleTodosTable,
        {
          title: "napi-shared-item",
          done: false,
        },
        { tier: "edge" },
      );
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

      await writer.updateDurable(simpleTodosTable, rowId, { done: true }, { tier: "edge" });

      const rowsAfterUpdate = await waitForQueryRows(reader, allTodosQuery, (rows) => {
        const row = rows.find((entry) => entry.id === rowId);
        return row?.done === true;
      });
      const updatedRow = rowsAfterUpdate.find((row) => row.id === rowId);
      expect(updatedRow?.done).toBe(true);

      await writer.deleteDurable(simpleTodosTable, rowId, { tier: "edge" });
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
      const createdRow = await writer.insertDurable(
        simpleTodosTable,
        {
          title: "persisted-local-item",
          done: false,
        },
        { tier: "worker" },
      );
      const rowId = createdRow.id;

      await waitForQueryRows(
        writer,
        allTodosQuery,
        (rows) => rows.some((row) => row.id === rowId),
        10_000,
        { tier: "worker" },
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
        { tier: "worker" },
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
        context.db().insertDurable(
          timestampProjectsTable,
          {
            name: "timestamp-probe",
            created_at: new Date(timestamp),
            updated_at: new Date(timestamp),
          },
          { tier: "worker" },
        ),
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
});
