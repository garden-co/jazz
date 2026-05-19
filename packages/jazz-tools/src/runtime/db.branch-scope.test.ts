import { describe, expect, it, vi } from "vitest";
import { Db, type QueryBuilder, type TableProxy } from "./db.js";
import type { JazzClient } from "./client.js";
import type { WasmSchema } from "../drivers/types.js";

type Todo = { id: string; projectId: string; title: string; done: boolean };
type TodoInit = Omit<Todo, "id">;

const schema: WasmSchema = {
  todos: {
    columns: [
      { name: "projectId", column_type: { type: "Uuid" }, nullable: false },
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
    ],
  },
};

class TestDb extends Db {
  constructor(private readonly testClient: JazzClient) {
    super({ appId: "branch-scope-test" }, null);
  }

  protected override getClient(_schema: WasmSchema): JazzClient {
    return this.testClient;
  }
}

function todosTable(): TableProxy<Todo, TodoInit> & {
  where(input: Partial<Todo>): QueryBuilder<Todo>;
} {
  return {
    _table: "todos",
    _schema: schema,
    _rowType: {} as Todo,
    _initType: {} as TodoInit,
    where(input: Partial<Todo>) {
      return {
        _table: "todos",
        _schema: schema,
        _rowType: {} as Todo,
        _build: () =>
          JSON.stringify({
            table: "todos",
            conditions: Object.entries(input).map(([column, value]) => ({
              column,
              op: "eq",
              value,
            })),
            includes: {},
            orderBy: [],
          }),
      };
    },
  };
}

describe("branch scoped Db", () => {
  it("creates a branch from a query and scopes branch reads", async () => {
    const client = {
      createBranchScope: vi.fn(),
      getSchema: vi.fn(() => schema),
      query: vi.fn(async () => []),
    } as unknown as JazzClient & {
      createBranchScope: ReturnType<typeof vi.fn>;
      query: ReturnType<typeof vi.fn>;
    };
    const db = new TestDb(client);
    const app = { todos: todosTable() };

    const draft = await db.createBranch("branch-1", app.todos.where({ projectId: "project-1" }));
    await draft.all(app.todos.where({ projectId: "project-1" }));

    expect(client.createBranchScope).toHaveBeenCalledTimes(1);
    const queryJson = client.query.mock.calls[0][0] as string;
    expect(JSON.parse(queryJson).branch_scope.branch_id).toBe("branch-1");
  });
});
