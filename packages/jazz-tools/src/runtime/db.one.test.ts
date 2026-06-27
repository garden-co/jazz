import { describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { Db, type DbConfig } from "./db.js";
import type { JazzClient, Row } from "./client.js";
import { RuntimeSource, type RuntimeClientContext } from "./runtime-source.js";
import type { WasmSchema } from "../drivers/types.js";

const todoSchema = {
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
  }),
};
type TodoSchema = s.Schema<typeof todoSchema>;
const app: s.App<TodoSchema> = s.defineApp(todoSchema);

const todoRow: Row = {
  id: "todo-1",
  values: [
    { type: "Text", value: "Buy milk" },
    { type: "Boolean", value: false },
  ],
};

type QuerySpy = ReturnType<typeof vi.fn<(queryJson: string, options?: unknown) => Promise<Row[]>>>;

class TestRuntimeSource extends RuntimeSource<DbConfig> {
  constructor(private readonly client: JazzClient) {
    super();
  }

  override createClient(_context: RuntimeClientContext<DbConfig>): JazzClient {
    return this.client;
  }
}

class TestDb extends Db {
  constructor(client: JazzClient) {
    super({ appId: "db-one-limit-test" }, new TestRuntimeSource(client));
  }

  protected override getClient(_schema: WasmSchema): JazzClient {
    return super.getClient(_schema);
  }
}

function makeClient() {
  const query = vi.fn(async (_queryJson: string, _options?: unknown) => [todoRow]);
  const beginTransaction = vi.fn(() => "transaction-1");
  const client = {
    getSchema: () => new Map(Object.entries(app.wasmSchema)),
    query,
    beginTransaction,
  } as unknown as JazzClient;

  return { client, query, beginTransaction };
}

function firstQueryJson(query: QuerySpy): string {
  const firstCall = query.mock.calls[0];
  expect(firstCall).toBeDefined();
  return firstCall![0] as string;
}

function rootLimit(queryJson: string): number | undefined {
  const parsed = JSON.parse(queryJson) as {
    relation_ir?: { Limit?: { limit?: unknown } };
  };
  const limit = parsed.relation_ir?.Limit?.limit;
  return typeof limit === "number" ? limit : undefined;
}

describe("Db.one", () => {
  it("adds limit 1 before executing an unbounded query", async () => {
    const { client, query } = makeClient();
    const db = new TestDb(client);

    await db.one(app.todos.where({ done: false }));

    expect(rootLimit(firstQueryJson(query))).toBe(1);
  });

  it("narrows explicit limits above one", async () => {
    const { client, query } = makeClient();
    const db = new TestDb(client);

    await db.one(app.todos.limit(10));

    expect(rootLimit(firstQueryJson(query))).toBe(1);
  });

  it("overrides explicit limit 0", async () => {
    const { client, query } = makeClient();
    const db = new TestDb(client);

    await db.one(app.todos.limit(0));

    expect(rootLimit(firstQueryJson(query))).toBe(1);
  });

  it("adds limit 1 before executing an explicit mergeable transaction query", async () => {
    const { client, query, beginTransaction } = makeClient();
    const db = new TestDb(client);
    const tx = db.beginTransaction();

    await tx.one(app.todos.where({ done: false }));

    expect(beginTransaction).toHaveBeenCalledWith("mergeable");
    expect(rootLimit(firstQueryJson(query))).toBe(1);
    expect(query.mock.calls[0]?.[1]).toMatchObject({
      transactionId: "transaction-1",
    });
  });
});
