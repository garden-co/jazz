import { describe, expect, it, vi } from "vitest";
import { schema as s } from "../index.js";
import { WriteHandle, type JazzClient, type Row } from "./client.js";
import { Db } from "./db.js";
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
  it("returns a prototype-scoped db view without shadowing the branch method", () => {
    const { client } = makeClient();
    const db = new TestDb(client);

    const branchDb = db.branch("branch-row-1");
    const nestedBranchDb = branchDb.branch("branch-row-2");

    expect(Object.getPrototypeOf(branchDb)).toBe(db);
    expect(Object.getPrototypeOf(nestedBranchDb)).toBe(branchDb);
    expect(typeof branchDb.branch).toBe("function");
  });

  it("injects the selected logical branch into reads", async () => {
    const { client } = makeClient();
    const db = new TestDb(client);

    await db.branch("branch-row-1").all(app.todos);

    expect(client.branchNameForUserBranch).not.toHaveBeenCalled();
    expect(client.query).toHaveBeenCalledWith(
      expect.stringContaining('"branches":["branch-row-1"]'),
      expect.anything(),
    );
  });

  it("uses the selected branch for immediate writes", () => {
    const { client, directBatch } = makeClient();
    const db = new TestDb(client);

    db.branch("branch-row-1").insert(app.todos, {
      projectId: "project-1",
      title: "Draft todo",
    });

    expect(client.branchNameForUserBranch).toHaveBeenCalledWith("branch-row-1");
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

  it("applies the db branch view to included relations", async () => {
    const { client } = makeClient();
    vi.mocked(client.query).mockResolvedValueOnce([
      {
        id: "project-1",
        values: [
          { type: "Text", value: "Draft project" },
          {
            type: "Array",
            value: [
              {
                type: "Row",
                value: {
                  id: "todo-1",
                  values: [
                    { type: "Uuid", value: "project-1" },
                    { type: "Text", value: "Draft todo" },
                  ],
                },
              },
            ],
          },
        ],
      },
    ]);
    const db = new TestDb(client);

    const rows = await db.branch("outer-branch").all(
      app.projects.include({
        todosViaProject: true,
      }),
    );

    const [queryJson] = vi.mocked(client.query).mock.calls.at(-1)! as [string];
    const query = JSON.parse(queryJson);
    expect(query.branches).toEqual(["outer-branch"]);
    expect(query.array_subqueries[0].branches).toBeUndefined();
    expect(rows).toEqual([
      {
        id: "project-1",
        name: "Draft project",
        todosViaProject: [
          {
            id: "todo-1",
            projectId: "project-1",
            title: "Draft todo",
          },
        ],
      },
    ]);
  });
});
