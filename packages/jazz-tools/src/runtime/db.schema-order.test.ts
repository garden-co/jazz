import { describe, expect, it, vi } from "vitest";
import { Db, type DbConfig, type QueryBuilder, type TableProxy } from "./db.js";
import type { InsertValues, WasmRow, WasmSchema } from "../drivers/types.js";
import { WriteHandle, WriteResult, JazzClient, type TxId, type InsertResult } from "./client.js";
import type { Session } from "./context.js";
import { RuntimeSource, type RuntimeClientContext } from "./runtime-source.js";

class TestRuntimeSource extends RuntimeSource<DbConfig> {
  constructor(private readonly client: JazzClient) {
    super();
  }

  override createClient(_context: RuntimeClientContext<DbConfig>): JazzClient {
    return this.client;
  }
}

class TestDb extends Db {
  constructor(
    private readonly testClient: JazzClient,
    private readonly testContext: { session?: Session } | null = null,
  ) {
    super({ appId: "schema-order-test" }, new TestRuntimeSource(testClient));
  }

  protected override getClient(_schema: WasmSchema): JazzClient {
    return this.testClient;
  }

  protected override getRuntimeOperationContext(): { session?: Session } | null {
    return this.testContext;
  }
}

function makeHandleClient(): JazzClient {
  return {
    waitForTransaction: vi.fn(async () => undefined),
  } as unknown as JazzClient;
}

function makeWriteResult(row: InsertResult): WriteResult<InsertResult> {
  if (row.kind !== "committed") throw new Error("expected committed fixture");
  return new WriteResult(row, row.txId, makeHandleClient());
}

function makeWriteHandle(transactionId: string): WriteHandle {
  return new WriteHandle(transactionId as TxId, makeHandleClient());
}

describe("Db runtime schema order", () => {
  it("derives a streaming insert branch selector from branchBy cells", async () => {
    const runtimeSchema: WasmSchema = {
      documents: {
        columns: [
          { name: "branch", column_type: { type: "Text" }, nullable: false },
          { name: "body", column_type: { type: "Text" }, nullable: false },
        ],
        branchBy: ["branch"],
      },
    };
    const insertStreaming = vi.fn(async (..._args: unknown[]) =>
      makeWriteHandle("streaming-branch-insert"),
    );
    const db = new TestDb({ insertStreaming } as unknown as JazzClient);
    const table = {
      _table: "documents",
      _schema: runtimeSchema,
      _rowType: {} as { id: string; branch: string; body: string },
      _initType: {} as { branch: string; body: string },
      _streamingInitType: {} as {
        branch: string;
        body: AsyncIterable<string | Uint8Array>;
      },
    } satisfies TableProxy<
      { id: string; branch: string; body: string },
      { branch: string; body: string },
      { branch: string; body: AsyncIterable<string | Uint8Array> }
    >;
    const source = (async function* () {
      yield "draft body";
    })();

    await db.insertStreaming(table, { branch: "draft", body: source }, { updatedAt: 42 });

    const options = insertStreaming.mock.calls[0]?.[4];
    expect(options).toMatchObject({
      updatedAt: 42,
      branch: { values: { branch: { type: "Text", value: "draft" } } },
    });
  });

  it("extracts a schema-derived stream from the ordinary insert payload shape", async () => {
    const runtimeSchema: WasmSchema = {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
        ],
      },
    };
    const handle = makeWriteHandle("streaming-insert");
    const insertStreaming = vi.fn(async () => handle);
    const updateStreaming = vi.fn(async () => handle);
    const upsertStreaming = vi.fn(async () => handle);
    const client = {
      getSchema: () => new Map(Object.entries(runtimeSchema)),
      insertStreaming,
      updateStreaming,
      upsertStreaming,
    } as unknown as JazzClient;
    const db = new TestDb(client);
    const table = {
      _table: "todos",
      _schema: runtimeSchema,
      _rowType: {} as { id: string; title: string; done: boolean },
      _initType: {} as { title: string; done: boolean },
      _streamingInitType: {} as {
        title: AsyncIterable<string | Uint8Array>;
        done: boolean;
      },
      _streamingUpdateType: {} as {
        title: AsyncIterable<string | Uint8Array>;
        done?: boolean;
      },
    } satisfies TableProxy<
      { id: string; title: string; done: boolean },
      { title: string; done: boolean },
      { title: AsyncIterable<string | Uint8Array>; done: boolean },
      { title: AsyncIterable<string | Uint8Array>; done?: boolean }
    >;
    const source = (async function* () {
      yield "streamed title";
    })();

    await expect(db.insertStreaming(table, { title: source, done: false })).resolves.toBe(handle);
    expect(insertStreaming).toHaveBeenCalledWith(
      "todos",
      { done: { type: "Boolean", value: false } },
      "title",
      source,
      undefined,
      undefined,
      undefined,
    );

    await db.updateStreaming(table, "todo-1", { title: source });
    expect(updateStreaming).toHaveBeenCalledWith(
      "todos",
      "todo-1",
      {},
      "title",
      source,
      undefined,
      undefined,
      undefined,
    );

    await db.upsertStreaming(table, "todo-1", { title: source, done: true });
    expect(upsertStreaming).toHaveBeenCalledWith(
      "todos",
      "todo-1",
      { done: { type: "Boolean", value: true } },
      "title",
      source,
      undefined,
      undefined,
      undefined,
    );
  });

  it("uses the generated schema order for inserts when the runtime schema is sorted", async () => {
    const generatedSchema: WasmSchema = {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
        ],
      },
    };
    const runtimeSchema: WasmSchema = {
      todos: {
        columns: [
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
          { name: "title", column_type: { type: "Text" }, nullable: false },
        ],
      },
    };
    const insert = vi.fn<(...args: [string, InsertValues]) => WriteResult<InsertResult>>(() =>
      makeWriteResult({
        id: "todo-1",
        values: [
          { type: "Text", value: "Buy milk" },
          { type: "Boolean", value: false },
        ],
        kind: "committed",
        txId: "transaction-schema-order-runtime" as TxId,
      }),
    );
    const client = {
      getSchema: () => new Map(Object.entries(runtimeSchema)),
      insert,
    } as unknown as JazzClient;
    const db = new TestDb(client);
    const table = {
      _table: "todos",
      _schema: generatedSchema,
      _rowType: {} as { id: string; title: string; done: boolean },
      _initType: {} as { title: string; done: boolean },
    } satisfies TableProxy<
      { id: string; title: string; done: boolean },
      { title: string; done: boolean }
    >;

    const { value: row } = db.insert(table, { title: "Buy milk", done: false });

    expect(insert).toHaveBeenCalledWith(
      "todos",
      {
        title: { type: "Text", value: "Buy milk" },
        done: { type: "Boolean", value: false },
      },
      undefined,
      undefined,
      undefined,
    );
    expect(row).toEqual({
      id: "todo-1",
      title: "Buy milk",
      done: false,
    });
  });

  it("uses the generated schema order when transforming query results", async () => {
    const generatedSchema: WasmSchema = {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
        ],
      },
    };
    const runtimeSchema: WasmSchema = {
      todos: {
        columns: [
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
          { name: "title", column_type: { type: "Text" }, nullable: false },
        ],
      },
    };
    const query = vi.fn<(...args: [string, object?]) => Promise<WasmRow[]>>(async () => [
      {
        id: "todo-1",
        values: [
          { type: "Text", value: "Sorted title" },
          { type: "Boolean", value: true },
        ],
      },
    ]);
    const client = {
      getSchema: () => new Map(Object.entries(runtimeSchema)),
      queryInternal: query,
    } as unknown as JazzClient;
    const db = new TestDb(client);
    const builder = {
      _table: "todos",
      _schema: generatedSchema,
      _rowType: {} as { id: string; title: string; done: boolean },
      _build: () =>
        JSON.stringify({
          table: "todos",
          conditions: [],
          includes: {},
          orderBy: [],
        }),
    } satisfies QueryBuilder<{ id: string; title: string; done: boolean }>;

    const rows = await db.all(builder);

    expect(rows).toEqual([
      {
        id: "todo-1",
        title: "Sorted title",
        done: true,
      },
    ]);
  });

  it("carries session identity through local native runtime queries", async () => {
    const generatedSchema: WasmSchema = {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
        ],
      },
    };
    const session: Session = {
      user_id: "00000000-0000-0000-0000-0000000000a1",
      claims: {},
      issuer: "https://issuer.example",
      authMode: "external",
    };
    const query = vi.fn(
      async (_queryJson: string, _options: unknown, receivedSession?: Session) => {
        expect(receivedSession).toBe(session);
        return [
          {
            id: "todo-1",
            values: [
              { type: "Text", value: "Direct scoped" },
              { type: "Boolean", value: false },
            ],
          },
        ];
      },
    );
    const client = {
      getSchema: () => new Map(Object.entries(generatedSchema)),
      queryInternal: query,
    } as unknown as JazzClient;
    const db = new TestDb(client, { session });
    const builder = {
      _table: "todos",
      _schema: generatedSchema,
      _rowType: {} as { id: string; title: string; done: boolean },
      _build: () =>
        JSON.stringify({
          table: "todos",
          conditions: [],
          includes: {},
          orderBy: [],
        }),
    } satisfies QueryBuilder<{ id: string; title: string; done: boolean }>;

    const rows = await db.all(builder, { tier: "local" });

    expect(query).toHaveBeenCalledTimes(1);
    expect(rows).toEqual([
      {
        id: "todo-1",
        title: "Direct scoped",
        done: false,
      },
    ]);
  });

  it("falls back to the generated schema when the runtime schema is missing a table", async () => {
    const generatedSchema: WasmSchema = {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
        ],
      },
    };
    const insert = vi.fn<(...args: [string, InsertValues]) => WriteResult<InsertResult>>(() =>
      makeWriteResult({
        id: "todo-1",
        values: [
          { type: "Text", value: "Buy milk" },
          { type: "Boolean", value: false },
        ],
        kind: "committed",
        txId: "transaction-schema-order-generated" as TxId,
      }),
    );
    const client = {
      getSchema: () => new Map(),
      insert,
    } as unknown as JazzClient;
    const db = new TestDb(client);
    const table = {
      _table: "todos",
      _schema: generatedSchema,
      _rowType: {} as { id: string; title: string; done: boolean },
      _initType: {} as { title: string; done: boolean },
    } satisfies TableProxy<
      { id: string; title: string; done: boolean },
      { title: string; done: boolean }
    >;

    const { value: row } = db.insert(table, { title: "Buy milk", done: false });

    expect(insert).toHaveBeenCalledWith(
      "todos",
      {
        title: { type: "Text", value: "Buy milk" },
        done: { type: "Boolean", value: false },
      },
      undefined,
      undefined,
      undefined,
    );
    expect(row).toEqual({
      id: "todo-1",
      title: "Buy milk",
      done: false,
    });
  });

  it("forwards a caller-supplied create id to the runtime client", () => {
    const generatedSchema: WasmSchema = {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
        ],
      },
    };
    const externalId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const insert = vi.fn<
      (...args: [string, InsertValues, { id: string }]) => WriteResult<InsertResult>
    >(() =>
      makeWriteResult({
        id: externalId,
        values: [
          { type: "Text", value: "Buy milk" },
          { type: "Boolean", value: false },
        ],
        kind: "committed",
        txId: "transaction-1" as TxId,
      }),
    );
    const client = {
      getSchema: () => new Map(),
      insert,
    } as unknown as JazzClient;
    const db = new TestDb(client);
    const table = {
      _table: "todos",
      _schema: generatedSchema,
      _rowType: {} as { id: string; title: string; done: boolean },
      _initType: {} as { title: string; done: boolean },
    } satisfies TableProxy<
      { id: string; title: string; done: boolean },
      { title: string; done: boolean }
    >;

    const row = db.insert(table, { title: "Buy milk", done: false }, { id: externalId });

    expect(insert).toHaveBeenCalledWith(
      "todos",
      {
        title: { type: "Text", value: "Buy milk" },
        done: { type: "Boolean", value: false },
      },
      { id: externalId },
      undefined,
      undefined,
    );
    expect(row.value).toEqual({
      id: externalId,
      title: "Buy milk",
      done: false,
    });
  });

  it("forwards caller-supplied upsert ids to the runtime client", () => {
    const generatedSchema: WasmSchema = {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
        ],
      },
    };
    const externalId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const upsert = vi.fn<(...args: Parameters<JazzClient["upsert"]>) => WriteHandle>(
      (_table, receivedId) => {
        expect(receivedId).toBe(externalId);
        return makeWriteHandle("transaction-upsert");
      },
    );
    const client = {
      getSchema: () => new Map(),
      upsert,
    } as unknown as JazzClient;
    const db = new TestDb(client);
    const table = {
      _table: "todos",
      _schema: generatedSchema,
      _rowType: {} as { id: string; title: string; done: boolean },
      _initType: {} as { title: string; done: boolean },
    } satisfies TableProxy<
      { id: string; title: string; done: boolean },
      { title: string; done: boolean }
    >;

    expect(db.upsert(table, externalId, { title: "Buy milk", done: false })).toMatchObject({
      txId: Promise.resolve("transaction-upsert" as TxId),
    });

    expect(upsert).toHaveBeenCalledWith(
      "todos",
      externalId,
      {
        title: { type: "Text", value: "Buy milk" },
        done: { type: "Boolean", value: false },
      },
      undefined,
      undefined,
      undefined,
    );
  });

  it("forwards custom updatedAt overrides on insert, update, and upsert", () => {
    const generatedSchema: WasmSchema = {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
        ],
      },
    };
    const updatedAt = 1_764_000_000_000;
    const insert = vi.fn<
      (...args: [string, InsertValues, { updatedAt: number }]) => WriteResult<InsertResult>
    >(() =>
      makeWriteResult({
        id: "todo-1",
        values: [
          { type: "Text", value: "Buy milk" },
          { type: "Boolean", value: false },
        ],
        kind: "committed",
        txId: "transaction-1" as TxId,
      }),
    );
    const update = vi.fn<(...args: [string, InsertValues, { updatedAt: number }]) => WriteHandle>(
      () => makeWriteHandle("transaction-update"),
    );
    const upsert = vi.fn<
      (...args: [string, InsertValues, { id: string; updatedAt: number }]) => WriteHandle
    >(() => makeWriteHandle("transaction-upsert"));
    const client = {
      getSchema: () => new Map(),
      insert,
      update,
      upsert,
    } as unknown as JazzClient;
    const db = new TestDb(client);
    const table = {
      _table: "todos",
      _schema: generatedSchema,
      _rowType: {} as { id: string; title: string; done: boolean },
      _initType: {} as { title: string; done: boolean },
    } satisfies TableProxy<
      { id: string; title: string; done: boolean },
      { title: string; done: boolean }
    >;

    db.insert(table, { title: "Buy milk", done: false }, { updatedAt });
    db.update(table, "todo-1", { done: true }, { updatedAt });
    db.upsert(table, "todo-1", { done: true }, { updatedAt });

    expect(insert).toHaveBeenCalledWith(
      "todos",
      {
        title: { type: "Text", value: "Buy milk" },
        done: { type: "Boolean", value: false },
      },
      { updatedAt },
      undefined,
      undefined,
    );
    expect(update).toHaveBeenCalledWith(
      "todos",
      "todo-1",
      {
        done: { type: "Boolean", value: true },
      },
      { updatedAt },
      undefined,
      undefined,
    );
    expect(upsert).toHaveBeenCalledWith(
      "todos",
      "todo-1",
      {
        done: { type: "Boolean", value: true },
      },
      { updatedAt },
      undefined,
      undefined,
    );
  });

  it("forwards custom updatedAt overrides through client-backed db mutations", () => {
    const generatedSchema: WasmSchema = {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
        ],
      },
    };
    const updatedAt = 1_764_000_000_000;
    const insert = vi.fn(() =>
      makeWriteResult({
        id: "todo-1",
        values: [
          { type: "Text", value: "Buy milk" },
          { type: "Boolean", value: false },
        ],
        kind: "committed",
        txId: "transaction-insert" as TxId,
      }),
    );
    const update = vi.fn<() => WriteHandle>(() => makeWriteHandle("transaction-update"));
    const upsert = vi.fn<() => WriteHandle>(() => makeWriteHandle("transaction-upsert"));
    const client = {
      getSchema: () => new Map(Object.entries(generatedSchema)),
      insert,
      update,
      upsert,
    } as unknown as JazzClient;
    const db = new TestDb(client);
    const table = {
      _table: "todos",
      _schema: generatedSchema,
      _rowType: {} as { id: string; title: string; done: boolean },
      _initType: {} as { title: string; done: boolean },
    } satisfies TableProxy<
      { id: string; title: string; done: boolean },
      { title: string; done: boolean }
    >;

    db.insert(table, { title: "Buy milk", done: false }, { updatedAt });
    db.update(table, "todo-1", { done: true }, { updatedAt });
    db.upsert(table, "todo-1", { done: true }, { updatedAt });

    expect(insert).toHaveBeenCalledWith(
      "todos",
      {
        title: { type: "Text", value: "Buy milk" },
        done: { type: "Boolean", value: false },
      },
      { updatedAt },
      undefined,
      undefined,
    );
    expect(update).toHaveBeenCalledWith(
      "todos",
      "todo-1",
      {
        done: { type: "Boolean", value: true },
      },
      { updatedAt },
      undefined,
      undefined,
    );
    expect(upsert).toHaveBeenCalledWith(
      "todos",
      "todo-1",
      {
        done: { type: "Boolean", value: true },
      },
      { updatedAt },
      undefined,
      undefined,
    );
  });

  it("falls back to the generated schema for query results when the runtime schema is missing a table", async () => {
    const generatedSchema: WasmSchema = {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
        ],
      },
    };
    const query = vi.fn<(...args: [string, object?]) => Promise<WasmRow[]>>(async () => [
      {
        id: "todo-1",
        values: [
          { type: "Text", value: "Generated title" },
          { type: "Boolean", value: true },
        ],
      },
    ]);
    const client = {
      getSchema: () => new Map(),
      queryInternal: query,
    } as unknown as JazzClient;
    const db = new TestDb(client);
    const builder = {
      _table: "todos",
      _schema: generatedSchema,
      _rowType: {} as { id: string; title: string; done: boolean },
      _build: () =>
        JSON.stringify({
          table: "todos",
          conditions: [],
          includes: {},
          orderBy: [],
        }),
    } satisfies QueryBuilder<{ id: string; title: string; done: boolean }>;

    const rows = await db.all(builder);

    expect(rows).toEqual([
      {
        id: "todo-1",
        title: "Generated title",
        done: true,
      },
    ]);
  });

  it("rejects queries whose public schema is missing the built table", async () => {
    const runtimeSchema: WasmSchema = {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
        ],
      },
    };
    const query = vi.fn<(...args: [string, object?]) => Promise<WasmRow[]>>(async () => []);
    const client = {
      getSchema: () => new Map(Object.entries(runtimeSchema)),
      query,
    } as unknown as JazzClient;
    const db = new TestDb(client);
    const builder = {
      _table: "todos",
      _schema: {},
      _rowType: {} as { id: string; title: string; done: boolean },
      _build: () =>
        JSON.stringify({
          table: "todos",
          conditions: [],
          includes: {},
          orderBy: [],
        }),
    } satisfies QueryBuilder<{ id: string; title: string; done: boolean }>;

    await expect(db.all(builder)).rejects.toThrow('Query schema is missing table "todos".');
    expect(query).not.toHaveBeenCalled();
  });

  it("rejects queries whose built public shape omits the table", async () => {
    const generatedSchema: WasmSchema = {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
        ],
      },
    };
    const query = vi.fn<(...args: [string, object?]) => Promise<WasmRow[]>>(async () => []);
    const client = {
      getSchema: () => new Map(Object.entries(generatedSchema)),
      query,
    } as unknown as JazzClient;
    const db = new TestDb(client);
    const builder = {
      _table: "todos",
      _schema: generatedSchema,
      _rowType: {} as { id: string; title: string; done: boolean },
      _build: () =>
        JSON.stringify({
          conditions: [],
          includes: {},
          orderBy: [],
        }),
    } satisfies QueryBuilder<{ id: string; title: string; done: boolean }>;

    await expect(db.all(builder)).rejects.toThrow(
      "QueryBuilder._build() must include a non-empty table.",
    );
    expect(query).not.toHaveBeenCalled();
  });
});
