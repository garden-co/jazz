import { describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { Db } from "./db.js";
import { WriteHandle, type JazzClient, type MutationErrorEvent, type Row } from "./client.js";
import type { WasmSchema } from "../drivers/types.js";

const app = s.defineApp({
  projects: s.table({
    name: s.string(),
  }),
  todos: s.table({
    projectId: s.ref("projects"),
    title: s.string(),
  }),
});

class TestDb extends Db {
  constructor(private readonly testClient: JazzClient) {
    super({ appId: "branch-db-test", env: "test", userBranch: "main" }, null);
  }

  protected override getClient(_schema: WasmSchema): JazzClient {
    return this.testClient;
  }
}

function makeClient() {
  const writeHandle = new WriteHandle("batch-1", {
    waitForBatch: vi.fn(async () => undefined),
    localBatchRecord: vi.fn(),
  } as unknown as JazzClient);

  const directBatch = {
    batchId: vi.fn(() => "batch-1"),
    create: vi.fn(
      (): Row =>
        ({
          id: "todo-1",
          values: [{ type: "Text", value: "Draft todo" }],
          batchId: "batch-1",
        }) as Row,
    ),
    upsert: vi.fn(),
    update: vi.fn(),
    delete: vi.fn(),
    query: vi.fn(async () => []),
    commit: vi.fn(() => writeHandle),
    rollback: vi.fn(),
  };

  const client = {
    getSchema: () => new Map(Object.entries(app.wasmSchema)),
    getSchemaContext: () => ({
      env: "test",
      schema_hash: "1234567890abcdef",
      user_branch: "main",
    }),
    branchNameForUserBranch: vi.fn((userBranch: string) => `test-1234567890ab-${userBranch}`),
    query: vi.fn(async () => []),
    queryInternal: vi.fn(async () => []),
    beginBatchInternal: vi.fn(() => directBatch),
    waitForBatch: vi.fn(async () => undefined),
  } as unknown as JazzClient;

  return { client, directBatch };
}

describe("Db.branch", () => {
  it("returns a db view that injects the selected branch into reads", async () => {
    const { client } = makeClient();
    const db = new TestDb(client);

    await db.branch("branch-row-1").all(app.todos);

    expect(client.branchNameForUserBranch).toHaveBeenCalledWith("branch-row-1");
    expect(client.query).toHaveBeenCalledWith(
      expect.stringContaining('"branches":["test-1234567890ab-branch-row-1"]'),
      expect.anything(),
    );
  });

  it("lets query-level branch selection override the db branch view", async () => {
    const { client } = makeClient();
    const db = new TestDb(client);

    await db.branch("branch-row-1").all(app.todos.branch("query-branch"));

    expect(client.branchNameForUserBranch).toHaveBeenCalledWith("query-branch");
    expect(client.query).toHaveBeenCalledWith(
      expect.stringContaining('"branches":["test-1234567890ab-query-branch"]'),
      expect.anything(),
    );
    expect(client.query).not.toHaveBeenCalledWith(
      expect.stringContaining('"branches":["test-1234567890ab-branch-row-1"]'),
      expect.anything(),
    );
  });

  it("uses a branch-targeted direct batch for immediate inserts", () => {
    const { client, directBatch } = makeClient();
    const db = new TestDb(client);

    db.branch("branch-row-1").insert(app.todos, {
      projectId: "project-1",
      title: "Draft todo",
    });

    expect(client.beginBatchInternal).toHaveBeenCalledWith(
      undefined,
      undefined,
      "test-1234567890ab-branch-row-1",
    );
    expect(directBatch.create).toHaveBeenCalledWith(
      "todos",
      {
        projectId: { type: "Uuid", value: "project-1" },
        title: { type: "Text", value: "Draft todo" },
      },
      undefined,
    );
    expect(directBatch.commit).toHaveBeenCalled();
  });

  it("serializes explicit query branches from table builders", () => {
    expect(JSON.parse(app.todos.branch("query-branch")._build())).toMatchObject({
      branches: ["query-branch"],
    });
  });

  it("preserves explicit branches on included relation builders", async () => {
    const { client } = makeClient();
    const db = new TestDb(client);

    await db.branch("outer-branch").all(
      app.projects.include({
        todosViaProject: app.todos.branch("included-branch"),
      }),
    );

    expect(client.branchNameForUserBranch).toHaveBeenCalledWith("outer-branch");
    expect(client.branchNameForUserBranch).toHaveBeenCalledWith("included-branch");

    const [queryJson] = vi.mocked(client.query).mock.calls.at(-1)! as [string];
    const query = JSON.parse(queryJson);
    expect(query.branches).toEqual(["test-1234567890ab-outer-branch"]);
    expect(query.array_subqueries[0].branches).toEqual(["test-1234567890ab-included-branch"]);
  });

  it("preserves explicit branches on each union relation seed", async () => {
    const { client } = makeClient();
    const db = new TestDb(client);
    const queryBuilder = app.union([
      app.todos.where({ title: "Draft A" }).branch("union-branch-a"),
      app.todos.where({ title: "Draft B" }).branch("union-branch-b"),
    ]);

    const builtQuery = JSON.parse(queryBuilder._build());
    expect(builtQuery.branches).toBeUndefined();
    expect(builtQuery.union.inputs[0].branches).toEqual(["union-branch-a"]);
    expect(builtQuery.union.inputs[1].branches).toEqual(["union-branch-b"]);

    await db.branch("outer-branch").all(queryBuilder);

    expect(client.branchNameForUserBranch).toHaveBeenCalledWith("union-branch-a");
    expect(client.branchNameForUserBranch).toHaveBeenCalledWith("union-branch-b");

    const [queryJson] = vi.mocked(client.queryInternal).mock.calls.at(-1)! as [string];
    const query = JSON.parse(queryJson);
    expect(query.branches).toEqual(["test-1234567890ab-outer-branch"]);
    expect(query.relation_ir.Union.inputs[0].Branch.branches).toEqual([
      "test-1234567890ab-union-branch-a",
    ]);
    expect(query.relation_ir.Union.inputs[1].Branch.branches).toEqual([
      "test-1234567890ab-union-branch-b",
    ]);
  });

  it("delegates branch mutation error listeners to the parent db", () => {
    const { client } = makeClient();
    const db = new TestDb(client);
    const branchDb = db.branch("branch-row-1");
    const listener = vi.fn();
    const unsubscribeFromParent = vi.fn();
    const event = {
      code: "permission_denied",
      reason: "denied",
      batch: {
        batchId: "batch-1",
        mode: "direct",
        sealed: true,
        latestSettlement: null,
      },
    } as MutationErrorEvent;
    let parentListener: ((event: MutationErrorEvent) => void) | undefined;
    const parentOnMutationError = vi.spyOn(db, "onMutationError").mockImplementation((callback) => {
      parentListener = callback;
      return unsubscribeFromParent;
    });

    const unsubscribe = branchDb.onMutationError(listener);
    parentListener?.(event);
    unsubscribe();

    expect(parentOnMutationError).toHaveBeenCalledWith(listener);
    expect(listener).toHaveBeenCalledWith(event);
    expect(unsubscribeFromParent).toHaveBeenCalled();
  });
});
