import type { RowAuthor } from "../magic-columns.js";
import { randomUUID } from "node:crypto";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { WebSocket as UndiciWebSocket } from "undici";
import { beforeAll, describe, expect, it, vi } from "vitest";
import type { WasmSchema } from "../drivers/types.js";
import { type TxId, type Row } from "./client.js";
import type { Session } from "./context.js";
import type { Db, QueryBuilder, TableProxy } from "./db.js";
import { translateQuery } from "./query-adapter.js";
import type { CompiledPermissionsMap } from "../schema-permissions.js";
import { deploy, startLocalJazzServer, testApp, testPermissions } from "../testing/index.js";
import { publishLoadedDeployment } from "../dev/catalogue.js";
import { DeploymentError, publishDeployment } from "../dev/catalogue-api.js";
import type { PermissionsFile } from "../dev/codegen-napi.js";
import { boundApp } from "../typed-app.js";
import {
  createPersistentNapiNativeRuntimeAdapter,
  loadNapiModule,
} from "./testing/napi-runtime-test-utils.js";

type RuntimeCommittedRow = Row & {
  kind: "committed";
  txId: TxId | Promise<TxId>;
};

type TestRuntimeWithTransport = {
  connect(url: string, authJson: string): void;
  close?: () => void | Promise<void>;
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

type PolicyTodo = {
  id: string;
  title: string;
  done: boolean;
  description?: string;
  parentId?: string;
  projectId?: string;
  owner_id: string;
};

type PolicyTodoWithProvenance = PolicyTodo & {
  $createdBy: RowAuthor;
};

type PolicyTodoInit = {
  title: string;
  done: boolean;
  description?: string;
  parentId?: string;
  projectId?: string;
  owner_id: string;
};

type PolicyGraphPerfLocation = {
  id: string;
  c1377?: string;
};

type PolicyGraphPerfLocationInit = Omit<PolicyGraphPerfLocation, "id">;

type PolicyGraphPerfAccessEdge = {
  id: string;
  c456: string;
  c457: string;
  c458: "e17" | "e18" | "e19";
  c459: boolean;
};

type PolicyGraphPerfTemplate = {
  id: string;
  c449: string;
  c450: string;
  c451: string;
  c452: boolean;
  c142: string;
  c453: Date;
  c454: Date;
  c1431: unknown;
};

const TEST_SCHEMA: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
    ],
  },
};

const publicUnionApp = testApp({
  tables: { todos: { columns: { title: "string", done: "boolean" } } },
});

const publicUnionPermissions = testPermissions(publicUnionApp, {
  tables: {
    todos: { read: "done = false", insert: "true", update: "true", delete: "true" },
  },
});

const publicUnionBigIntApp = testApp({
  tables: { metrics: { columns: { label: "string", value: "bigint" } } },
});

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

type TodoServerProject = {
  wasmSchema: WasmSchema;
  permissions: CompiledPermissionsMap;
  /** Its jazz.permissions.json, what a deployment takes. */
  permissionsFile: PermissionsFile;
};

let todoServerProjectPromise: Promise<TodoServerProject> | null = null;

/** The todo-server-ts example's app, from its jazz.app.json and jazz.permissions.json. */
async function loadTodoServerProject(): Promise<TodoServerProject> {
  todoServerProjectPromise ??= (async () => {
    const app = testApp(await readFile(join(TODO_SERVER_SCHEMA_DIR, "jazz.app.json"), "utf8"));
    const permissionsText = await readFile(
      join(TODO_SERVER_SCHEMA_DIR, "jazz.permissions.json"),
      "utf8",
    );
    const permissions = testPermissions(app, permissionsText);
    return {
      wasmSchema: app.wasmSchema,
      permissions,
      permissionsFile: JSON.parse(permissionsText) as PermissionsFile,
    };
  })();
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

function makePolicyTodoProvenanceByIdQuery(
  schema: WasmSchema,
  id: string,
): QueryBuilder<PolicyTodoWithProvenance> {
  return {
    _table: "todos",
    _schema: schema,
    _rowType: undefined as unknown as PolicyTodoWithProvenance,
    _build() {
      return JSON.stringify({
        table: "todos",
        conditions: [{ column: "id", op: "eq", value: id }],
        includes: {},
        orderBy: [],
        offset: 0,
        select: ["*", "$createdBy"],
      });
    },
  };
}

const TODO_SERVER_SCHEMA_DIR = fileURLToPath(
  new URL("../../../../examples/todo-server-ts", import.meta.url),
);
const POLICY_GRAPH_PERF_FIXTURE_DIR = new URL(
  "../testing/fixtures/policy-graph-perf/",
  import.meta.url,
);

beforeAll(async () => {
  if (!globalThis.WebSocket) {
    (globalThis as typeof globalThis & { WebSocket: typeof WebSocket }).WebSocket =
      UndiciWebSocket as unknown as typeof WebSocket;
  }
  await loadNapiModule();
});

async function waitForQueryRows<T>(
  db: Db,
  query: QueryBuilder<T>,
  predicate: (rows: T[]) => boolean,
  timeoutMs = 20_000,
  queryOptions: { tier?: "local-first" | "remote" } = { tier: "remote" },
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

async function loadPolicyGraphPerfSchema(field: "mergedSchema" | "wasmSchema" = "mergedSchema") {
  const source = JSON.parse(
    await readFile(new URL("schema-source.json", POLICY_GRAPH_PERF_FIXTURE_DIR), "utf8"),
  ) as Record<
    typeof field,
    Record<
      string,
      Omit<WasmSchema[string], "policies"> & {
        policies?: CompiledPermissionsMap[string];
      }
    >
  >;
  const schema = Object.fromEntries(
    Object.entries(source[field]).map(([name, { policies: _, ...table }]) => [name, table]),
  );
  const permissions = Object.fromEntries(
    Object.entries(source[field]).flatMap(([name, table]) =>
      table.policies ? [[name, table.policies]] : [],
    ),
  );
  return { schema, permissions };
}

/**
 * Deploy the policy graph perf fixture as its project used to declare it: the
 * fixture's `wasmSchema`, with the policies of its `mergedSchema`.
 */
async function deployPolicyGraphPerfFixture(options: {
  serverUrl: string;
  appId: string;
  adminSecret: string;
}): Promise<void> {
  const { schema } = await loadPolicyGraphPerfSchema("wasmSchema");
  const { permissions } = await loadPolicyGraphPerfSchema("mergedSchema");
  await publishRawDeployment(options, await serverSchemaHash(options, schema), schema, permissions);
}

/**
 * Publish a raw schema with engine policies as a deployment, as `deploy`
 * publishes an app's: `hash` is the schema's hash (the server checks it).
 */
async function publishRawDeployment(
  options: { serverUrl: string; appId: string; adminSecret: string },
  hash: string,
  schema: WasmSchema,
  permissions: CompiledPermissionsMap,
) {
  return publishLoadedDeployment(options, {
    deployment: {
      targetSchemaHash: hash,
      schemas: [{ hash, schema: { tables: schema } }],
      migrations: [],
      permissions,
    },
    warnings: [],
  });
}

/**
 * The server's hash of a raw `schema` (with no app files to compile it
 * from): it names the actual hash when refusing a schema claimed under
 * another, and stores nothing.
 */
async function serverSchemaHash(
  options: { serverUrl: string; appId: string; adminSecret: string },
  schema: WasmSchema,
): Promise<string> {
  const claimed = "0".repeat(64);
  try {
    await publishDeployment(options.serverUrl, options, {
      targetSchemaHash: claimed,
      schemas: [{ hash: claimed, schema: { tables: schema } }],
      migrations: [],
      permissions: {},
    });
  } catch (error) {
    const actual = (error as DeploymentError).details as { actual?: string } | undefined;
    if (
      error instanceof DeploymentError &&
      error.code === "schema_hash_mismatch" &&
      actual?.actual
    ) {
      return actual.actual;
    }
    throw error;
  }
  throw new Error("the server took a schema under a made-up hash");
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
  it("releases the persistent store's lock after closing an upstream transport", async () => {
    const { wasmSchema, permissions } = await loadTodoServerProject();
    const runtimeData = await createTempRuntimeData("jazz-napi-transport-close-reopen-");
    const previousWebSocket = globalThis.WebSocket;
    class OpenWebSocket {
      readyState = 1;
      addEventListener(): void {}
      send(): void {}
      close(): void {
        this.readyState = 3;
      }
    }
    (globalThis as typeof globalThis & { WebSocket: typeof WebSocket }).WebSocket =
      OpenWebSocket as unknown as typeof WebSocket;

    let first: TestRuntimeWithTransport | null = null;
    let second: TestRuntimeWithTransport | null = null;
    try {
      first = (await createPersistentNapiNativeRuntimeAdapter(
        wasmSchema,
        permissions!,
        runtimeData.dataPath,
        {
          appId: randomUUID(),
          env: "test",
        },
      )) as TestRuntimeWithTransport;

      first.connect("ws://127.0.0.1/jazz/ws", "{}");
      await first.close?.();
      first = null;

      second = (await createPersistentNapiNativeRuntimeAdapter(
        wasmSchema,
        permissions!,
        runtimeData.dataPath,
        {
          appId: randomUUID(),
          env: "test",
        },
      )) as TestRuntimeWithTransport;
      expect(second).toBeDefined();
    } finally {
      await first?.close?.();
      await second?.close?.();
      (globalThis as typeof globalThis & { WebSocket?: typeof WebSocket }).WebSocket =
        previousWebSocket;
      await cleanupTempRuntimeData(runtimeData);
    }
  });

  it("runs named and array unions through public Db reads and live order windows", async () => {
    const dataRoot = await createTempDir("jazz-napi-public-union-");
    const dataPath = join(dataRoot, "runtime.db");
    let context: { db(): Db; shutdown(): Promise<void> } | null = null;

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");
      context = createJazzContext({
        appId: randomUUID(),
        app: publicUnionApp,
        permissions: {},
        driver: { type: "persistent", dataPath },
      });
      const db = context.db();
      const alpha = await db
        .insert(publicUnionApp.todos, { title: "alpha", done: false })
        .wait({ tier: "local" });
      await db.insert(publicUnionApp.todos, { title: "beta", done: false }).wait({ tier: "local" });
      await db.insert(publicUnionApp.todos, { title: "gamma", done: true }).wait({ tier: "local" });

      const arms = [
        publicUnionApp.todos.where({ done: false }),
        publicUnionApp.todos.where({ title: "alpha" }),
      ] as const;
      const arrayUnion = publicUnionApp.union(arms).orderBy("title").limit(2);
      const namedUnion = publicUnionApp
        .union({ byDone: arms[0], byTitle: arms[1] })
        .orderBy("title")
        .limit(2);

      await expect(db.all(arrayUnion, { tier: "local-first" })).resolves.toMatchObject([
        { id: alpha.id, title: "alpha" },
        { id: alpha.id, title: "alpha" },
      ]);
      await expect(db.all(namedUnion, { tier: "local-first" })).resolves.toMatchObject([
        { id: alpha.id, title: "alpha" },
        { id: alpha.id, title: "alpha" },
      ]);
      const repeatedNamedArm = publicUnionApp.todos.where({ done: false });
      const namedDuplicateUnion = publicUnionApp
        .union({ first: repeatedNamedArm, second: repeatedNamedArm })
        .orderBy("title")
        .limit(2);
      await expect(db.all(namedDuplicateUnion, { tier: "local-first" })).resolves.toMatchObject([
        { id: alpha.id, title: "alpha" },
        { id: alpha.id, title: "alpha" },
      ]);
      const offsetAcrossDuplicateBoundary = publicUnionApp
        .union(arms)
        .orderBy("title")
        .offset(1)
        .limit(2);
      await expect(
        db.all(offsetAcrossDuplicateBoundary, { tier: "local-first" }),
      ).resolves.toMatchObject([{ id: alpha.id, title: "alpha" }, { title: "beta" }]);

      const limitOneUpdates: string[][] = [];
      const stopLimitOne = db.subscribe(
        publicUnionApp.union(arms).orderBy("title").limit(1),
        (rows) => limitOneUpdates.push(rows.map((row) => row.title)),
        { tier: "local-first" },
      );
      const updates: string[][] = [];
      const unsubscribe = db.subscribe(
        arrayUnion,
        (rows) => updates.push(rows.map((row) => row.title)),
        { tier: "local-first" },
      );
      try {
        await vi.waitFor(() => expect(updates.at(-1)).toEqual(["alpha", "alpha"]));

        await db.update(publicUnionApp.todos, alpha.id, { done: true }).wait({ tier: "local" });
        await vi.waitFor(() => expect(updates.at(-1)).toEqual(["alpha", "beta"]));

        await db
          .insert(publicUnionApp.todos, { title: "aardvark", done: false })
          .wait({ tier: "local" });
        await vi.waitFor(() => expect(updates.at(-1)).toEqual(["aardvark", "alpha"]));
      } finally {
        unsubscribe();
        stopLimitOne();
      }
      expect(limitOneUpdates).toContainEqual(["alpha"]);
      expect(limitOneUpdates.at(-1)).toEqual(["aardvark"]);
    } finally {
      if (context) await context.shutdown();
      await rm(dataRoot, { recursive: true, force: true });
    }
  }, 30_000);

  it("applies ordinary table read policy to every public union arm", async () => {
    const dataRoot = await createTempDir("jazz-napi-public-union-policy-");
    const dataPath = join(dataRoot, "runtime.db");
    let context: {
      db(): Db;
      forSession(session: Session): Db;
      shutdown(): Promise<void>;
    } | null = null;

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");
      context = createJazzContext({
        appId: randomUUID(),
        app: publicUnionApp,
        permissions: publicUnionPermissions,
        driver: { type: "persistent", dataPath },
      });
      const writer = context.db();
      await writer
        .insert(publicUnionApp.todos, { title: "visible", done: false })
        .wait({ tier: "local" });
      await writer
        .insert(publicUnionApp.todos, { title: "hidden", done: true })
        .wait({ tier: "local" });

      const reader = context.forSession({
        issuer: "https://union-policy.test",
        user_id: "reader",
        claims: {},
        authMode: "external",
      });
      const query = publicUnionApp.union([
        publicUnionApp.todos.where({}),
        publicUnionApp.todos.where({ done: { in: [false, true] } }),
      ]);
      await expect(reader.all(query, { tier: "local-first" })).resolves.toMatchObject([
        { title: "visible", done: false },
        { title: "visible", done: false },
      ]);
    } finally {
      if (context) await context.shutdown();
      await rm(dataRoot, { recursive: true, force: true });
    }
  }, 30_000);

  it("evaluates out-of-safe-range signed bigint predicates in public unions", async () => {
    const dataRoot = await createTempDir("jazz-napi-public-union-bigint-");
    const dataPath = join(dataRoot, "runtime.db");
    let context: { db(): Db; shutdown(): Promise<void> } | null = null;
    const positive = 9_007_199_254_740_993n;
    const negative = -9_007_199_254_740_993n;

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");
      context = createJazzContext({
        appId: randomUUID(),
        app: publicUnionBigIntApp,
        permissions: {},
        driver: { type: "persistent", dataPath },
      });
      const db = context.db();
      await db
        .insert(publicUnionBigIntApp.metrics, { label: "positive", value: positive })
        .wait({ tier: "local" });
      await db
        .insert(publicUnionBigIntApp.metrics, { label: "negative", value: negative })
        .wait({ tier: "local" });
      await db
        .insert(publicUnionBigIntApp.metrics, { label: "nearby", value: positive - 1n })
        .wait({ tier: "local" });

      const query = publicUnionBigIntApp
        .union({
          positive: publicUnionBigIntApp.metrics.where({ value: positive }),
          negative: publicUnionBigIntApp.metrics.where({ value: negative }),
        })
        .orderBy("label");
      await expect(db.all(query, { tier: "local-first" })).resolves.toMatchObject([
        { label: "negative", value: negative },
        { label: "positive", value: positive },
      ]);
    } finally {
      if (context) await context.shutdown();
      await rm(dataRoot, { recursive: true, force: true });
    }
  }, 30_000);

  it("supports oversized indexed persistent mutations from JS callers", async () => {
    const dataDir = await createTempDir("jazz-napi-large-index-");
    const dataPath = join(dataDir, "jazz.db");
    const runtime = (await createPersistentNapiNativeRuntimeAdapter(TEST_SCHEMA, {}, dataPath, {
      appId: `napi-large-index-${randomUUID()}`,
      env: "test",
    })) as unknown as {
      insert(table: string, values: unknown): RuntimeCommittedRow;
      update(
        table: string,
        objectId: string,
        updates: Record<string, unknown>,
      ): { kind: "committed"; txId: TxId | Promise<TxId> };
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
      expect(await insertedRow.txId).toEqual(expect.any(String));

      let rows = await runtime.query(queryJson);
      expect(rows).toHaveLength(1);
      expect(rows[0]).toMatchObject({ id: insertedRow.id });
      expect(rows[0]?.values[0]).toEqual({ type: "Text", value: oversizedTitle });
      expect(rows[0]?.values[1]).toEqual({ type: "Boolean", value: false });

      const secondRow = runtime.insert("todos", {
        title: { type: "Text", value: "kept title" },
        done: { type: "Boolean", value: false },
      });
      expect(await secondRow.txId).toEqual(expect.any(String));

      const updateResult = runtime.update("todos", secondRow.id, {
        title: { type: "Text", value: updatedOversizedTitle },
      });
      expect(await updateResult.txId).toEqual(expect.any(String));

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
      forSession(session: Session): Db;
      shutdown(): Promise<void>;
    } | null = null;

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");

      const todoServerProject = await loadTodoServerProject();
      const { bundle } = await deploy({
        serverUrl: server.url,
        appId,
        adminSecret,
        app: todoServerProject.wasmSchema,
        permissions: todoServerProject.permissionsFile,
      });
      const todoServerSchema = todoServerProject.wasmSchema;
      const policyTodosTable = makePolicyTodosTable(todoServerSchema);

      runtimeData = await createTempRuntimeData("jazz-napi-session-runtime-");
      context = createJazzContext({
        bundle,
        appId,
        app: { wasmSchema: todoServerSchema },
        permissions: todoServerProject.permissions ?? {},
        driver: { type: "persistent", dataPath: runtimeData.dataPath },
        serverUrl: server.url,
        backendSecret,
        env: "test",
      });
      await settleAsyncSyncWork();

      const backendDb = context.asBackend();
      const aliceDb = context.forSession({
        issuer: "https://issuer.example",
        user_id: "alice",
        account_id: "00000000-0000-4000-8000-000000000001",
        claims: { role: "editor", team: "alpha" },
        authMode: "external",
      });
      const aliceAuthor = {
        account: "00000000-0000-4000-8000-000000000001",
        identity: { issuer: "https://issuer.example", subject: "alice" },
      };
      const systemAuthor = {
        account: "00000000-0000-0000-0000-000000000000",
        identity: {
          issuer: "urn:jazz:system",
          subject: expect.stringMatching(/^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/),
        },
      };

      const backendCreatedTodo = await withTimeout(
        backendDb
          .insert(policyTodosTable, {
            title: "backend-system-provenance",
            done: false,
            description: "created via asBackend",
            owner_id: "00000000-0000-4000-8000-000000000003",
          })
          .wait({ tier: "global" }),
        10_000,
        "backend insert timed out",
      );

      const createdTodo = await withTimeout(
        aliceDb
          .insert(policyTodosTable, {
            title: "session-created-item",
            done: false,
            description: "created via forSession",
            owner_id: aliceAuthor.account,
          })
          .wait({ tier: "global" }),
        10_000,
        "session insert timed out",
      );

      await vi.waitFor(
        async () => {
          const backendRow = await withTimeout(
            backendDb.one(
              makePolicyTodoProvenanceByIdQuery(todoServerSchema, backendCreatedTodo.id),
              { tier: "remote" },
            ),
            10_000,
            "backend provenance read timed out",
          );
          expect(backendRow).toMatchObject({
            id: backendCreatedTodo.id,
            $createdBy: systemAuthor,
          });
          const sessionRow = await withTimeout(
            backendDb.one(makePolicyTodoProvenanceByIdQuery(todoServerSchema, createdTodo.id), {
              tier: "remote",
            }),
            10_000,
            "session provenance read timed out",
          );
          expect(sessionRow).toMatchObject({ id: createdTodo.id, $createdBy: aliceAuthor });
        },
        { timeout: 20_000 },
      );

      await vi.waitFor(
        async () => {
          expect(
            await withTimeout(
              backendDb.one(makePolicyTodoByIdQuery(todoServerSchema, createdTodo.id), {
                tier: "remote",
              }),
              10_000,
              "backend session read timed out",
            ),
          ).toMatchObject({
            id: createdTodo.id,
            title: "session-created-item",
            done: false,
            owner_id: aliceAuthor.account,
          });
        },
        { timeout: 20_000 },
      );

      await expect(
        aliceDb
          .insert(policyTodosTable, {
            title: "session-policy-denied",
            done: false,
            description: "",
            owner_id: "00000000-0000-4000-8000-000000000002",
          })
          .wait({ tier: "global" }),
      ).rejects.toThrow(/AuthorizationDenied|Write rejected by server authorization/);

      await withTimeout(
        aliceDb.update(policyTodosTable, createdTodo.id, { done: true }).wait({ tier: "global" }),
        10_000,
        "session update timed out",
      );

      await vi.waitFor(
        async () => {
          expect(
            await withTimeout(
              backendDb.one(makePolicyTodoByIdQuery(todoServerSchema, createdTodo.id), {
                tier: "remote",
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
        aliceDb.delete(policyTodosTable, createdTodo.id).wait({ tier: "global" }),
        10_000,
        "session delete timed out",
      );

      await vi.waitFor(
        async () => {
          expect(
            await withTimeout(
              backendDb.one(makePolicyTodoByIdQuery(todoServerSchema, createdTodo.id), {
                tier: "remote",
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
  }, 120_000);

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

      const todoServerProject = await loadTodoServerProject();
      await deploy({
        serverUrl: server.url,
        appId,
        adminSecret,
        app: todoServerProject.wasmSchema,
        permissions: todoServerProject.permissionsFile,
      });
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
      });
      await settleAsyncSyncWork();

      const backendDb = context.asBackend();
      const createdTodo = await withTimeout(
        backendDb
          .insert(policyTodosTable, {
            title: "global-wait-item",
            done: false,
            description: "global wait repro",
            owner_id: "00000000-0000-4000-8000-000000000003",
          })
          .wait({ tier: "global" }),
        15_000,
        "backend global insert wait timed out",
      );

      expect(createdTodo).toMatchObject({
        title: "global-wait-item",
        done: false,
        owner_id: "00000000-0000-4000-8000-000000000003",
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

  it("resolves global waits for backend writes with the policy graph perf fixture", async () => {
    const appId = randomUUID();
    const backendSecret = "napi-policy_graph-global-wait-secret";
    const adminSecret = "napi-policy_graph-global-wait-admin-secret";
    const { schema: policyGraphSchemaForServer, permissions } = await loadPolicyGraphPerfSchema();
    let runtimeData: TempRuntimeData | null = null;
    const server = await startLocalJazzServer({
      appId,
      backendSecret,
      adminSecret,
      schema: policyGraphSchemaForServer,
      permissions,
    });
    let context: {
      asBackend(): Db;
      shutdown(): Promise<void>;
    } | null = null;

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");
      const policyGraphSchema = policyGraphSchemaForServer;
      const locationTable = makeWhereTable<PolicyGraphPerfLocation, PolicyGraphPerfLocationInit>(
        "t111",
        policyGraphSchema,
      );

      runtimeData = await createTempRuntimeData("jazz-napi-policy_graph-global-wait-runtime-");
      context = createJazzContext({
        appId,
        app: { wasmSchema: policyGraphSchema },
        permissions,
        driver: { type: "persistent", dataPath: runtimeData.dataPath },
        serverUrl: server.url,
        backendSecret,
        adminSecret,
        env: "test",
      });
      await settleAsyncSyncWork();

      const backendDb = context.asBackend();
      const createdLocation = await withTimeout(
        backendDb
          .insert(
            locationTable,
            {
              c1377: "policy graph perf fixture global wait",
            },
            { id: randomUUID() },
          )
          .wait({ tier: "global" }),
        90_000,
        "policy graph perf fixture backend global insert wait timed out",
      );

      expect(createdLocation).toMatchObject({
        c1377: "policy graph perf fixture global wait",
      });
    } finally {
      if (context) {
        await context.shutdown();
      }
      await settleAsyncSyncWork();
      await cleanupTempRuntimeData(runtimeData);
      await server.stop();
    }
  }, 120_000);

  it("resolves global waits after deploying the policy graph perf fixture", async () => {
    const appId = randomUUID();
    const backendSecret = "napi-policy_graph-global-wait-deploy-secret";
    const adminSecret = "napi-policy_graph-global-wait-deploy-admin-secret";
    const { schema: policyGraphSchema, permissions } = await loadPolicyGraphPerfSchema();
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
      await deployPolicyGraphPerfFixture({
        serverUrl: server.url,
        appId,
        adminSecret,
      });

      const { createJazzContext } = await import("../backend/create-jazz-context.js");
      const locationTable = makeWhereTable<PolicyGraphPerfLocation, PolicyGraphPerfLocationInit>(
        "t111",
        policyGraphSchema,
      );

      runtimeData = await createTempRuntimeData(
        "jazz-napi-policy_graph-global-wait-deploy-runtime-",
      );
      context = createJazzContext({
        appId,
        app: { wasmSchema: policyGraphSchema },
        permissions,
        driver: { type: "persistent", dataPath: runtimeData.dataPath },
        serverUrl: server.url,
        backendSecret,
        adminSecret,
        env: "test",
      });
      await settleAsyncSyncWork();

      const backendDb = context.asBackend();
      const createdLocation = await withTimeout(
        backendDb
          .insert(
            locationTable,
            {
              c1377: "policy graph deploy schemaDir global wait",
            },
            { id: randomUUID() },
          )
          .wait({ tier: "global" }),
        90_000,
        "policy graph deploy schemaDir backend global insert wait timed out",
      );

      expect(createdLocation).toMatchObject({
        c1377: "policy graph deploy schemaDir global wait",
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

  it("serves policy graph resource-policy holders through the local server route", async () => {
    const appId = randomUUID();
    const backendSecret = "napi-policy_graph-holder-subscription-secret";
    const adminSecret = "napi-policy_graph-holder-subscription-admin-secret";
    const { schema: policyGraphSchema, permissions } =
      await loadPolicyGraphPerfSchema("wasmSchema");
    const memberId = "00000000-0000-4000-8000-000000000001";
    const corporationId = randomUUID();
    const templateId = randomUUID();
    const server = await startLocalJazzServer({
      appId,
      backendSecret,
      adminSecret,
      schema: policyGraphSchema,
      permissions,
    });
    let context: {
      asBackend(): Db;
      forSession(session: Session): Db;
      shutdown(): Promise<void>;
    } | null = null;
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");
      context = createJazzContext({
        appId,
        app: { wasmSchema: policyGraphSchema },
        permissions,
        driver: { type: "memory" },
        serverUrl: server.url,
        backendSecret,
        adminSecret,
        env: "test",
      });
      await settleAsyncSyncWork();

      const backendDb = context.asBackend();
      const teamTable = makeWhereTable<Record<string, unknown>, Record<string, unknown>>(
        "t1",
        policyGraphSchema,
      );
      const teamEntryTable = makeWhereTable<Record<string, unknown>, Record<string, unknown>>(
        "t188",
        policyGraphSchema,
      );
      const templateTable = makeWhereTable<
        PolicyGraphPerfTemplate,
        Omit<PolicyGraphPerfTemplate, "id">
      >("t105", policyGraphSchema);
      const templateAccessEdgesTable = makeWhereTable<
        PolicyGraphPerfAccessEdge,
        Omit<PolicyGraphPerfAccessEdge, "id">
      >("t190", policyGraphSchema);
      const teamAccessEdgesTable = makeWhereTable<
        PolicyGraphPerfAccessEdge,
        Omit<PolicyGraphPerfAccessEdge, "id">
      >("t187", policyGraphSchema);
      const now = new Date("2026-07-10T00:00:00.000Z");
      await backendDb
        .insert(
          teamTable,
          {
            c449: corporationId,
            c450: memberId,
            c451: memberId,
            c452: false,
            c142: "Example Corp",
            c453: now,
            c454: now,
            c146: "fixture corporation",
          },
          { id: corporationId },
        )
        .wait({ tier: "global" });
      await backendDb
        .insert(
          teamTable,
          {
            c449: corporationId,
            c450: memberId,
            c451: memberId,
            c452: false,
            c142: "Jon",
            c453: now,
            c454: now,
            c146: "fixture member",
          },
          { id: memberId },
        )
        .wait({ tier: "global" });
      await backendDb
        .insert(
          teamEntryTable,
          {
            c457: memberId,
            c1948: corporationId,
            c1949: memberId,
            c459: false,
            c1950: now,
          },
          { id: randomUUID() },
        )
        .wait({ tier: "global" });
      await backendDb
        .insert(
          templateTable,
          {
            c449: corporationId,
            c450: memberId,
            c451: memberId,
            c452: false,
            c142: "Visible template",
            c453: now,
            c454: now,
            c1431: {},
          },
          { id: templateId },
        )
        .wait({ tier: "global" });
      await backendDb
        .insert(
          templateAccessEdgesTable,
          {
            c456: templateId,
            c457: corporationId,
            c458: "e19",
            c459: false,
          },
          { id: randomUUID() },
        )
        .wait({ tier: "global" });
      await backendDb
        .insert(
          teamAccessEdgesTable,
          {
            c456: corporationId,
            c457: corporationId,
            c458: "e19",
            c459: false,
          },
          { id: randomUUID() },
        )
        .wait({ tier: "global" });

      const visibleTemplates = await waitForQueryRows(
        backendDb,
        templateTable.where({}),
        (rows) => rows.some((row) => row.id === templateId),
        10_000,
        { tier: "remote" },
      );
      expect(visibleTemplates).toEqual([expect.objectContaining({ id: templateId })]);

      const visibleEdges = await waitForQueryRows(
        backendDb,
        teamAccessEdgesTable.where({}),
        (rows) => rows.some((row) => row.c456 === corporationId),
        10_000,
        { tier: "remote" },
      );
      expect(visibleEdges).toEqual([expect.objectContaining({ c456: corporationId })]);
      expect(consoleError.mock.calls).toEqual([]);
    } finally {
      consoleError.mockRestore();
      if (context) {
        await context.shutdown();
      }
      await settleAsyncSyncWork();
      await server.stop();
    }
  }, 60_000);

  it("reopens a deployed policy graph schema data directory through the local server route", async () => {
    const appId = randomUUID();
    const backendSecret = "napi-policy_graph-reopen-secret";
    const adminSecret = "napi-policy_graph-reopen-admin-secret";
    const dataDir = await createTempDir("jazz-napi-policy_graph-reopen-server-");
    let server: Awaited<ReturnType<typeof startLocalJazzServer>> | null = null;

    try {
      server = await startLocalJazzServer({
        appId,
        backendSecret,
        adminSecret,
        dataDir,
      });
      await deployPolicyGraphPerfFixture({
        serverUrl: server.url,
        appId,
        adminSecret,
      });
      await server.stop();
      server = null;

      server = await startLocalJazzServer({
        appId,
        backendSecret,
        adminSecret,
        dataDir,
      });
      expect(server.url).toContain("http://");
    } finally {
      if (server) {
        await server.stop();
      }
      await rm(dataDir, { recursive: true, force: true });
    }
  }, 60_000);

  it("serves policy graph holder queries after importing data and reopening the local server route", async () => {
    const appId = randomUUID();
    const backendSecret = "napi-policy_graph-chain-secret";
    const adminSecret = "napi-policy_graph-chain-admin-secret";
    const dataDir = await createTempDir("jazz-napi-policy_graph-chain-server-");
    const { schema: policyGraphSchema, permissions } = await loadPolicyGraphPerfSchema();
    const memberId = "00000000-0000-4000-8000-000000000001";
    const corporationId = randomUUID();
    let server: Awaited<ReturnType<typeof startLocalJazzServer>> | null = null;
    let context: {
      asBackend(): Db;
      shutdown(): Promise<void>;
    } | null = null;
    const consoleError = vi.spyOn(console, "error").mockImplementation(() => undefined);

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");

      server = await startLocalJazzServer({
        appId,
        backendSecret,
        adminSecret,
        dataDir,
      });
      await deployPolicyGraphPerfFixture({
        serverUrl: server.url,
        appId,
        adminSecret,
      });
      context = createJazzContext({
        appId,
        app: { wasmSchema: policyGraphSchema },
        permissions,
        driver: { type: "memory" },
        serverUrl: server.url,
        backendSecret,
        adminSecret,
        env: "test",
      });
      await settleAsyncSyncWork();

      const backendDb = context.asBackend();
      const teamTable = makeWhereTable<Record<string, unknown>, Record<string, unknown>>(
        "t1",
        policyGraphSchema,
      );
      const teamEntryTable = makeWhereTable<Record<string, unknown>, Record<string, unknown>>(
        "t188",
        policyGraphSchema,
      );
      const teamAccessEdgesTable = makeWhereTable<
        PolicyGraphPerfAccessEdge,
        Omit<PolicyGraphPerfAccessEdge, "id">
      >("t187", policyGraphSchema);
      const now = new Date("2026-07-10T00:00:00.000Z");

      await (
        await backendDb.transaction((tx) => {
          tx.insert(
            teamTable,
            {
              c449: corporationId,
              c450: memberId,
              c451: memberId,
              c452: false,
              c142: "Example Corp",
              c453: now,
              c454: now,
              c146: "fixture corporation",
            },
            { id: corporationId },
          );
          tx.insert(
            teamTable,
            {
              c449: corporationId,
              c450: memberId,
              c451: memberId,
              c452: false,
              c142: "Jon",
              c453: now,
              c454: now,
              c146: "fixture member",
            },
            { id: memberId },
          );
          tx.insert(
            teamEntryTable,
            {
              c457: memberId,
              c1948: corporationId,
              c1949: memberId,
              c459: false,
              c1950: now,
            },
            { id: randomUUID() },
          );
          tx.insert(
            teamAccessEdgesTable,
            {
              c456: corporationId,
              c457: corporationId,
              c458: "e19",
              c459: false,
            },
            { id: randomUUID() },
          );
        })
      ).wait({ tier: "global" });

      await context.shutdown();
      context = null;
      await server.stop();
      server = null;

      server = await startLocalJazzServer({
        appId,
        backendSecret,
        adminSecret,
        dataDir,
      });
      context = createJazzContext({
        appId,
        app: { wasmSchema: policyGraphSchema },
        permissions,
        driver: { type: "memory" },
        serverUrl: server.url,
        backendSecret,
        adminSecret,
        env: "test",
      });
      await settleAsyncSyncWork();

      const reopenedBackend = context.asBackend();
      const teamRows = await waitForQueryRows(
        reopenedBackend,
        teamTable.where({}),
        (rows) => rows.some((row) => row.id === corporationId),
        10_000,
        { tier: "remote" },
      );
      expect(teamRows).toEqual(
        expect.arrayContaining([expect.objectContaining({ id: corporationId })]),
      );

      const edgeRows = await waitForQueryRows(
        reopenedBackend,
        teamAccessEdgesTable.where({}),
        (rows) => rows.some((row) => row.c456 === corporationId),
        10_000,
        { tier: "remote" },
      );
      expect(edgeRows).toEqual([expect.objectContaining({ c456: corporationId })]);
      expect(consoleError.mock.calls).toEqual([]);
    } finally {
      consoleError.mockRestore();
      if (context) {
        await context.shutdown();
      }
      if (server) {
        await server.stop();
      }
      await rm(dataDir, { recursive: true, force: true });
    }
  }, 90_000);

  it("publishes inherited seeded-reachability permissions through the local server route", async () => {
    const appId = randomUUID();
    const adminSecret = "napi-policy_graph-permissions-admin-secret";
    const app = testApp({
      tables: {
        dropdown_entry: {
          columns: { dropdowns_id: "uuid", label: "string" },
          relations: { dropdowns: { rel: "dropdowns", column: "dropdowns_id" } },
        },
        dropdowns: {
          columns: { name: "string" },
          relations: {
            dropdown_entryViaDropdowns: { reverse: "dropdown_entry", relation: "dropdowns" },
            dropdowns_access_edgesViaResource: {
              reverse: "dropdowns_access_edges",
              relation: "resource",
            },
          },
        },
        dropdowns_access_edges: {
          columns: {
            resource_id: "uuid",
            team_id: "uuid",
            grant_role: "string",
            administrator: "boolean",
          },
          relations: {
            resource: { rel: "dropdowns", column: "resource_id" },
            team: { rel: "team", column: "team_id" },
          },
        },
        team: {
          columns: { identity_key: "string" },
          relations: {
            dropdowns_access_edgesViaTeam: { reverse: "dropdowns_access_edges", relation: "team" },
            team_entryViaTarget: { reverse: "team_entry", relation: "target" },
            team_entryViaTeam: { reverse: "team_entry", relation: "team" },
          },
        },
        team_entry: {
          columns: { team_id: "uuid", target_id: "uuid", administrator: "boolean" },
          relations: {
            target: { rel: "team", column: "target_id" },
            team: { rel: "team", column: "team_id" },
          },
        },
      },
    });
    // Teams reachable from the user's entries along `target` (up to 32 hops),
    // then their EDITOR edges to this dropdown.
    const editorOfReachableTeam =
      "exists (from recursive(seed: from team_entry join team as __hop_0 on team_entry.target_id = __hop_0.id where team_entry.team_id = session.user select __hop_0.id as id, step: from team_entry where team_entry.administrator = bool(false) and team_entry.team_id = id(frontier) select team_entry.target_id as id, frontier: id(row), dedupe: (id(row)), maxDepth: 32) join dropdowns_access_edges as __recursive_join_0 on team.id = __recursive_join_0.team_id where __recursive_join_0.resource_id = outer.id and __recursive_join_0.grant_role in [text('EDITOR')] and __recursive_join_0.administrator = bool(false) select __recursive_join_0.id as id)";
    const permissions = {
      format: 1,
      tables: {
        dropdown_entry: {
          read: "allowedTo.read(dropdowns)",
          insert: "allowedTo.update(dropdowns)",
          update: "allowedTo.update(dropdowns)",
          delete: "allowedTo.update(dropdowns)",
        },
        dropdowns: {
          read: editorOfReachableTeam,
          insert: "true",
          update: {
            old: editorOfReachableTeam,
            new: "true",
          },
          delete: "true",
        },
        dropdowns_access_edges: {
          read: "true",
          insert: "true",
          update: "true",
          delete: "true",
        },
      },
    };
    const server = await startLocalJazzServer({
      appId,
      backendSecret: "napi-policy_graph-permissions-secret",
      adminSecret,
    });

    try {
      await deploy({ serverUrl: server.url, appId, adminSecret, app, permissions });
    } finally {
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
      forSession(session: Session): Db;
      shutdown(): Promise<void>;
    } | null = null;

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");

      const todoServerProject = await loadTodoServerProject();
      const { bundle } = await deploy({
        serverUrl: server.url,
        appId,
        adminSecret,
        app: todoServerProject.wasmSchema,
        permissions: todoServerProject.permissionsFile,
      });
      const todoServerSchema = todoServerProject.wasmSchema;
      const policyTodosTable = makePolicyTodosTable(todoServerSchema);

      runtimeData = await createTempRuntimeData("jazz-napi-null-update-runtime-");
      context = createJazzContext({
        bundle,
        appId,
        app: { wasmSchema: todoServerSchema },
        permissions: todoServerProject.permissions ?? {},
        driver: { type: "persistent", dataPath: runtimeData.dataPath },
        serverUrl: server.url,
        backendSecret,
        env: "test",
      });
      await settleAsyncSyncWork();

      const aliceDb = context.forSession({
        issuer: "https://issuer.example",
        user_id: "alice",
        account_id: "00000000-0000-4000-8000-000000000001",
        claims: { role: "editor", team: "alpha" },
        authMode: "external",
      });
      const aliceAuthor = {
        account: "00000000-0000-4000-8000-000000000001",
        identity: { issuer: "https://issuer.example", subject: "alice" },
      };

      const createdTodo = await aliceDb
        .insert(policyTodosTable, {
          title: "nullable-description-repro",
          done: false,
          description: "server-original",
          owner_id: aliceAuthor.account,
        })
        .wait({ tier: "global" });

      const nullUpdate = aliceDb.update(policyTodosTable, createdTodo.id, {
        description: null,
      } as unknown as Partial<PolicyTodoInit>);
      await nullUpdate.wait({ tier: "local" });

      const rowAfterUpdate = await aliceDb.one(
        makePolicyTodoByIdQuery(todoServerSchema, createdTodo.id),
        {
          tier: "local-first",
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

  it("syncs global create/update/delete flows between real backend NAPI contexts", async () => {
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

      // TEST_SCHEMA as is, with no permissions; publicUnionApp has its columns.
      const { bundle } = await publishRawDeployment(
        { serverUrl: server.url, appId, adminSecret },
        boundApp(publicUnionApp.wasmSchema)!.schema.current,
        TEST_SCHEMA,
        {},
      );

      writerRuntimeData = await createTempRuntimeData("jazz-napi-sync-writer-");
      writerContext = createJazzContext({
        bundle,
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
        bundle,
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
        .wait({ tier: "global" });
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

      await writer.update(simpleTodosTable, rowId, { done: true }).wait({ tier: "global" });

      const rowsAfterUpdate = await waitForQueryRows(reader, allTodosQuery, (rows) => {
        const row = rows.find((entry) => entry.id === rowId);
        return row?.done === true;
      });
      const updatedRow = rowsAfterUpdate.find((row) => row.id === rowId);
      expect(updatedRow?.done).toBe(true);

      await writer.delete(simpleTodosTable, rowId).wait({ tier: "global" });
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
        bundle,
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
        { tier: "local-first" },
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
        { tier: "local-first" },
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
        tier: "local-first",
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
      close(): Promise<undefined>;
    } | null = null;

    try {
      const { createJazzContext } = await import("../backend/create-jazz-context.js");

      seedRuntime = (await createPersistentNapiNativeRuntimeAdapter(BYTEA_SCHEMA, {}, dataPath, {
        appId,
        env: "dev",
      })) as unknown as {
        insert(table: string, values: unknown): { id: string };
        close(): Promise<undefined>;
      };

      // Seed via the raw N-API shape so this test isolates the update path.
      const created = seedRuntime.insert("byte_chunks", {
        label: { type: "Text", value: "beta" },
        data: { type: "Bytea", value: [1, 2, 3] },
      });
      await seedRuntime.close();
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
        tier: "local-first",
      });

      expect(reloaded).not.toBeNull();
      expect(Array.from(reloaded?.data ?? [])).toEqual([4, 5, 6]);
    } finally {
      await seedRuntime?.close();
      if (context) {
        await context.shutdown();
      }
      await rm(dataRoot, { recursive: true, force: true });
    }
  }, 30_000);
});
