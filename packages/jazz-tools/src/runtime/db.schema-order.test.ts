import { describe, expect, it, vi } from "vitest";
import { Db, type QueryBuilder, type TableProxy } from "./db.js";
import type { InsertValues, WasmRow, WasmSchema } from "../drivers/types.js";
import type { JazzClient, Row } from "./client.js";

class TestDb extends Db {
  constructor(private readonly testClient: JazzClient) {
    super({ appId: "schema-order-test" }, null);
  }

  protected override getClient(_schema: WasmSchema): JazzClient {
    return this.testClient;
  }
}

describe("Db runtime schema order", () => {
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
    const create = vi.fn<(...args: [string, InsertValues]) => Row>(() => ({
      id: "todo-1",
      values: [
        { type: "Text", value: "Buy milk" },
        { type: "Boolean", value: false },
      ],
    }));
    const client = {
      getSchema: () => new Map(Object.entries(runtimeSchema)),
      create,
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

    const row = db.insert(table, { title: "Buy milk", done: false });

    expect(create).toHaveBeenCalledWith("todos", {
      title: { type: "Text", value: "Buy milk" },
      done: { type: "Boolean", value: false },
    });
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
      query,
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

  it("falls back to the generated schema when the runtime schema is missing a table", async () => {
    const generatedSchema: WasmSchema = {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
        ],
      },
    };
    const create = vi.fn<(...args: [string, InsertValues]) => Row>(() => ({
      id: "todo-1",
      values: [
        { type: "Text", value: "Buy milk" },
        { type: "Boolean", value: false },
      ],
    }));
    const client = {
      getSchema: () => new Map(),
      create,
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

    const row = db.insert(table, { title: "Buy milk", done: false });

    expect(create).toHaveBeenCalledWith("todos", {
      title: { type: "Text", value: "Buy milk" },
      done: { type: "Boolean", value: false },
    });
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
    const create = vi.fn<(...args: [string, InsertValues, { id: string }]) => Row>(() => ({
      id: externalId,
      values: [
        { type: "Text", value: "Buy milk" },
        { type: "Boolean", value: false },
      ],
    }));
    const client = {
      getSchema: () => new Map(),
      create,
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

    expect(create).toHaveBeenCalledWith(
      "todos",
      {
        title: { type: "Text", value: "Buy milk" },
        done: { type: "Boolean", value: false },
      },
      { id: externalId },
    );
    expect(row).toEqual({
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
    const upsert = vi.fn<(...args: [string, InsertValues, { id: string }]) => void>();
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

    expect(
      db.upsert(table, { title: "Buy milk", done: false }, { id: externalId }),
    ).toBeUndefined();

    expect(upsert).toHaveBeenCalledWith(
      "todos",
      {
        title: { type: "Text", value: "Buy milk" },
        done: { type: "Boolean", value: false },
      },
      { id: externalId },
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
      query,
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
});
