import { describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { createDbFromClient } from "./db.js";
import type { JazzClient, Row } from "./client.js";

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

function makeClient() {
  const query = vi.fn(async (_queryJson: string, _options?: unknown) => [todoRow]);
  const beginBatch = vi.fn(() => "batch-1");
  const client = {
    getSchema: () => new Map(Object.entries(app.wasmSchema)),
    query,
    beginBatch,
  } as unknown as JazzClient;

  return { client, query, beginBatch };
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
    const db = createDbFromClient({ appId: "db-one-limit-test" }, client);

    await db.one(app.todos.where({ done: false }));

    expect(rootLimit(firstQueryJson(query))).toBe(1);
  });

  it("narrows explicit limits above one", async () => {
    const { client, query } = makeClient();
    const db = createDbFromClient({ appId: "db-one-limit-test" }, client);

    await db.one(app.todos.limit(10));

    expect(rootLimit(firstQueryJson(query))).toBe(1);
  });

  it("overrides explicit limit 0", async () => {
    const { client, query } = makeClient();
    const db = createDbFromClient({ appId: "db-one-limit-test" }, client);

    await db.one(app.todos.limit(0));

    expect(rootLimit(firstQueryJson(query))).toBe(1);
  });

  it("adds limit 1 before executing an explicit batch query", async () => {
    const { client, query, beginBatch } = makeClient();
    const db = createDbFromClient({ appId: "db-one-limit-test" }, client);
    const batch = db.beginBatch();

    await batch.one(app.todos.where({ done: false }));

    expect(beginBatch).toHaveBeenCalledWith("direct");
    expect(rootLimit(firstQueryJson(query))).toBe(1);
    expect(query.mock.calls[0]?.[1]).toMatchObject({
      transactionBatchId: "batch-1",
    });
  });
});
