import { describe, expect, it, vi } from "vitest";
import { Db, type QueryBuilder, type TableProxy } from "./db.js";
import { JazzClient, type JazzClient as JazzClientType, type Runtime } from "./client.js";
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

const projectId = "00000000-0000-7000-8000-000000000001";
const branchId = "00000000-0000-7000-8000-000000000010";

class TestDb extends Db {
  constructor(private readonly testClient: JazzClientType) {
    super({ appId: "branch-scope-test" }, null);
  }

  protected override getClient(_schema: WasmSchema): JazzClientType {
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

    const draft = await db.createBranch(branchId, app.todos.where({ projectId }));
    await draft.all(app.todos.where({ projectId }));

    expect(client.createBranchScope).toHaveBeenCalledTimes(1);
    const queryJson = client.query.mock.calls[0][0] as string;
    expect(JSON.parse(queryJson).branch_scope.branch_id).toBe(branchId);
  });

  it("marks branch diff queries", async () => {
    const client = {
      getSchema: vi.fn(() => schema),
      query: vi.fn(async () => []),
    } as unknown as JazzClient & {
      query: ReturnType<typeof vi.fn>;
    };
    const db = new TestDb(client);
    const app = { todos: todosTable() };

    const draft = db.branch(branchId);
    await draft.diff(app.todos.where({ projectId }));

    const queryJson = client.query.mock.calls[0][0] as string;
    expect(JSON.parse(queryJson)).toMatchObject({
      branch_scope: { branch_id: branchId },
      diff: true,
    });
  });

  it("sends branch id for branch-scoped inserts", () => {
    const runtime = {
      insert: vi.fn(),
      insertWithSession: vi.fn(() => ({
        id: "todo-1",
        batchId: "batch-1",
        values: [
          { type: "Uuid", value: projectId },
          { type: "Text", value: "Draft" },
          { type: "Boolean", value: false },
        ],
      })),
      sealBatch: vi.fn(),
      waitForBatch: vi.fn(async () => undefined),
      onMutationError: vi.fn(),
      getSchema: vi.fn(() => schema),
    } as unknown as Runtime & {
      insertWithSession: ReturnType<typeof vi.fn>;
    };
    const client = JazzClient.connectWithRuntime(runtime, {
      appId: "branch-scope-test",
      schema,
    });
    const db = new TestDb(client);
    const app = { todos: todosTable() };

    const draft = db.branch(branchId);
    draft.insert(app.todos, { projectId, title: "Draft", done: false });

    const writeContextJson = runtime.insertWithSession.mock.calls[0][2] as string;
    expect(JSON.parse(writeContextJson).target_branch_name).toBe(branchId);
  });

  it("sends branch id for branch-scoped updates and deletes", () => {
    const runtime = {
      update: vi.fn(),
      updateWithSession: vi.fn(() => ({ batchId: "batch-update" })),
      delete: vi.fn(),
      deleteWithSession: vi.fn(() => ({ batchId: "batch-delete" })),
      sealBatch: vi.fn(),
      waitForBatch: vi.fn(async () => undefined),
      onMutationError: vi.fn(),
      getSchema: vi.fn(() => schema),
    } as unknown as Runtime & {
      updateWithSession: ReturnType<typeof vi.fn>;
      deleteWithSession: ReturnType<typeof vi.fn>;
    };
    const client = JazzClient.connectWithRuntime(runtime, {
      appId: "branch-scope-test",
      schema,
    });
    const db = new TestDb(client);
    const app = { todos: todosTable() };

    const draft = db.branch(branchId);
    draft.update(app.todos, "todo-1", { title: "Changed" });
    draft.delete(app.todos, "todo-1");

    const updateContextJson = runtime.updateWithSession.mock.calls[0][2] as string;
    const deleteContextJson = runtime.deleteWithSession.mock.calls[0][1] as string;
    expect(JSON.parse(updateContextJson).target_branch_name).toBe(branchId);
    expect(JSON.parse(deleteContextJson).target_branch_name).toBe(branchId);
  });
});
