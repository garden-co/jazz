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
    branchNameForBranchId: vi.fn((branchId: string) => `test-1234567890ab-${branchId}`),
    query: vi.fn(async () => []),
    queryInternal: vi.fn(async () => []),
    beginBatchInternal: vi.fn(() => directBatch),
    waitForBatch: vi.fn(async () => undefined),
  } as unknown as JazzClient;

  return { client, directBatch };
}

describe("Db.branch", () => {
  it("returns a prototype-derived db view without shadowing branch()", () => {
    const { client } = makeClient();
    const db = new TestDb(client);

    const branchDb = db.branch("branch-row-1");

    expect(Object.getPrototypeOf(branchDb)).toBe(db);
    expect(Object.prototype.hasOwnProperty.call(branchDb, "branch")).toBe(false);
    expect(typeof branchDb.branch).toBe("function");
    expect(branchDb.getConfig()).toEqual(db.getConfig());
  });

  it("returns a db view that injects the selected branch into reads", async () => {
    const { client } = makeClient();
    const db = new TestDb(client);

    await db.branch("branch-row-1").all(app.todos);

    expect(client.branchNameForBranchId).not.toHaveBeenCalled();
    expect(client.query).toHaveBeenCalledWith(
      expect.stringContaining('"branches":["branch-row-1"]'),
      expect.anything(),
    );
  });

  it("lets nested branch views override the inherited branch scope", async () => {
    const { client } = makeClient();
    const db = new TestDb(client);
    const branchA = db.branch("branch-row-1");
    const branchB = branchA.branch("branch-row-2");

    await branchB.all(app.todos);

    expect(Object.getPrototypeOf(branchB)).toBe(branchA);
    expect(client.query).toHaveBeenCalledWith(
      expect.stringContaining('"branches":["branch-row-2"]'),
      expect.anything(),
    );
    expect(client.query).not.toHaveBeenCalledWith(
      expect.stringContaining('"branches":["branch-row-1"]'),
      expect.anything(),
    );
  });

  it("treats branch-looking scope input as a logical branch id", async () => {
    const { client } = makeClient();
    const db = new TestDb(client);
    const branchLikeInput = "otherenv-deadbeefcafe-branch-row-1";

    await db.branch(branchLikeInput).all(app.todos);

    expect(client.branchNameForBranchId).not.toHaveBeenCalled();
    expect(client.query).toHaveBeenCalledWith(
      expect.stringContaining(`"branches":["${branchLikeInput}"]`),
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

  it("applies the db branch view to plain included relations", async () => {
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

    expect(client.branchNameForBranchId).not.toHaveBeenCalled();

    const [queryJson] = vi.mocked(client.query).mock.calls.at(-1)! as [string];
    const query = JSON.parse(queryJson);
    expect(query.branches).toEqual(["outer-branch"]);
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
