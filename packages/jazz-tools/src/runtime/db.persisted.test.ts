import { describe, expect, it, vi } from "vitest";
import { Db, createDbFromClient, type QueryBuilder, type TableProxy } from "./db.js";
import type { WasmSchema } from "../drivers/types.js";
import {
  WriteResult,
  WriteHandle,
  composeStorageBranchName,
  type JazzClient,
  type LocalBatchRecord,
  type Row,
} from "./client.js";
import type { Session } from "./context.js";

class TestDb extends Db {
  constructor(private readonly testClient: JazzClient) {
    super({ appId: "persisted-db-test" }, null);
  }

  protected override getClient(_schema: WasmSchema): JazzClient {
    return this.testClient;
  }
}

function todoSchema(): WasmSchema {
  return {
    todos: {
      columns: [
        { name: "title", column_type: { type: "Text" }, nullable: false },
        { name: "done", column_type: { type: "Boolean" }, nullable: false },
      ],
    },
  };
}

function todoTable() {
  const schema = todoSchema();
  return {
    _table: "todos",
    _schema: schema,
    _rowType: {} as { id: string; title: string; done: boolean },
    _initType: {} as { title: string; done: boolean },
  } satisfies TableProxy<
    { id: string; title: string; done: boolean },
    { title: string; done: boolean }
  >;
}

function todoQuery(extra: Record<string, unknown> = {}): QueryBuilder<{
  id: string;
  title: string;
  done: boolean;
}> {
  const schema = todoSchema();
  return {
    _table: "todos",
    _schema: schema,
    _rowType: {} as { id: string; title: string; done: boolean },
    _build() {
      return JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        orderBy: [],
        ...extra,
      });
    },
  };
}

const TEST_SCHEMA_HASH = "aaaaaaaaaaaabbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

function testBranchTargetName(branchId: string): string {
  return composeStorageBranchName({ env: "dev", schema_hash: TEST_SCHEMA_HASH }, branchId);
}

function branchTargetNameSupport() {
  return {
    getSchemaHash: () => TEST_SCHEMA_HASH,
    branchTargetName: vi.fn((branchId: string) => testBranchTargetName(branchId)),
  };
}

function makeLocalBatchRecord(batchId: string): LocalBatchRecord {
  return {
    batchId,
    mode: "direct",
    sealed: true,
    latestSettlement: null,
  };
}

function makeHandleClient(localBatchRecord: LocalBatchRecord) {
  return {
    waitForPersistedBatch: vi.fn(async () => undefined),
    localBatchRecord: vi.fn(() => localBatchRecord),
    acknowledgeRejectedBatch: vi.fn(() => false),
  };
}

function makeWriteResult(
  value: Row,
  batchId: string,
  localBatchRecord = makeLocalBatchRecord(batchId),
) {
  const client = makeHandleClient(localBatchRecord);
  return {
    handle: new WriteResult(value, batchId, client as unknown as JazzClient),
    client,
  };
}

function makeWriteHandle(batchId: string, localBatchRecord = makeLocalBatchRecord(batchId)) {
  const client = makeHandleClient(localBatchRecord);
  return {
    handle: new WriteHandle(batchId, client as unknown as JazzClient),
    client,
  };
}

describe("Db write handles", () => {
  it("validates branch ids for branch-scoped views", () => {
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
    } as unknown as JazzClient;
    const db = new TestDb(client);

    expect(db.branch("main").branchId).toBe("main");
    expect(db.branch("01963f3e-5cbe-7a62-8d7c-123456789abc").branchId).toBe(
      "01963f3e-5cbe-7a62-8d7c-123456789abc",
    );
    expect(db.branch("01963F3E-5CBE-7A62-8D7C-123456789ABC").branchId).toBe(
      "01963f3e-5cbe-7a62-8d7c-123456789abc",
    );
    expect(() => db.branch("")).toThrow("Invalid branch id");
    expect(() => db.branch("feature")).toThrow("Invalid branch id");
  });

  it("routes branch-scoped writes through branch write context", () => {
    const table = todoTable();
    const branchId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const { handle: insertHandle } = makeWriteResult(
      {
        id: "todo-branch",
        values: [
          { type: "Text", value: "Branch write" },
          { type: "Boolean", value: false },
        ],
      },
      "batch-branch-insert",
    );
    const { handle: updateHandle } = makeWriteHandle("batch-branch-update");
    const { handle: deleteHandle } = makeWriteHandle("batch-branch-delete");
    const createHandleInternal = vi.fn(() => insertHandle);
    const updateHandleInternal = vi.fn(() => updateHandle);
    const deleteHandleInternal = vi.fn(() => deleteHandle);
    const client = {
      ...branchTargetNameSupport(),
      getSchema: () => new Map(Object.entries(todoSchema())),
      createHandleInternal,
      updateHandleInternal,
      deleteHandleInternal,
    } as unknown as JazzClient;
    const db = new TestDb(client);
    const branch = db.branch(branchId);

    branch.insert(table, { title: "Branch write", done: false });
    branch.update(table, "todo-branch", { done: true });
    branch.delete(table, "todo-branch");

    expect(createHandleInternal).toHaveBeenCalledWith(
      "todos",
      {
        title: { type: "Text", value: "Branch write" },
        done: { type: "Boolean", value: false },
      },
      undefined,
      undefined,
      undefined,
      undefined,
      testBranchTargetName(branchId),
    );
    expect(updateHandleInternal).toHaveBeenCalledWith(
      "todo-branch",
      {
        done: { type: "Boolean", value: true },
      },
      undefined,
      undefined,
      undefined,
      undefined,
      testBranchTargetName(branchId),
    );
    expect(deleteHandleInternal).toHaveBeenCalledWith(
      "todo-branch",
      undefined,
      undefined,
      undefined,
      undefined,
      testBranchTargetName(branchId),
    );
  });

  it("routes client-backed branch writes through composed branch context", () => {
    const table = todoTable();
    const branchId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const { handle: insertHandle } = makeWriteResult(
      {
        id: "todo-client-backed-branch",
        values: [
          { type: "Text", value: "Client-backed branch write" },
          { type: "Boolean", value: false },
        ],
      },
      "batch-client-backed-branch-insert",
    );
    const createHandleInternal = vi.fn(() => insertHandle);
    const db = createDbFromClient({ appId: "client-backed-branch-write-test" }, {
      ...branchTargetNameSupport(),
      getSchema: () => new Map(Object.entries(todoSchema())),
      createHandleInternal,
    } as unknown as JazzClient);

    db.branch(branchId).insert(table, {
      title: "Client-backed branch write",
      done: false,
    });

    expect(createHandleInternal).toHaveBeenCalledWith(
      "todos",
      {
        title: { type: "Text", value: "Client-backed branch write" },
        done: { type: "Boolean", value: false },
      },
      undefined,
      undefined,
      undefined,
      undefined,
      testBranchTargetName(branchId),
    );
  });

  it("adds branch selection to branch-scoped queries only when query has no branch metadata", async () => {
    const branchId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const queryCalls: string[] = [];
    const client = {
      ...branchTargetNameSupport(),
      getSchema: () => new Map(Object.entries(todoSchema())),
      query: vi.fn(async (queryJson: string) => {
        queryCalls.push(queryJson);
        return [];
      }),
    } as unknown as JazzClient;
    const db = new TestDb(client);
    const branch = db.branch(branchId);

    await branch.all(todoQuery());
    await branch.all(todoQuery({ branches: ["main"] }));

    expect(JSON.parse(queryCalls[0] ?? "{}").branches).toEqual([testBranchTargetName(branchId)]);
    expect(JSON.parse(queryCalls[1] ?? "{}").branches).toEqual([testBranchTargetName("main")]);
  });

  it("resolves branch ids in subscription queries", () => {
    const branchId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const subscribe = vi.fn((_query: string) => 7);
    const client = {
      ...branchTargetNameSupport(),
      getSchema: () => new Map(Object.entries(todoSchema())),
      subscribe,
      unsubscribe: vi.fn(),
    } as unknown as JazzClient;
    const db = new TestDb(client);

    const unsubscribe = db.subscribeAll(todoQuery({ branches: [branchId] }), () => undefined);

    expect(JSON.parse(subscribe.mock.calls[0]?.[0] ?? "{}").branches).toEqual([
      testBranchTargetName(branchId),
    ]);
    unsubscribe();
  });

  it("rejects invalid branch ids from raw query metadata", async () => {
    const client = {
      ...branchTargetNameSupport(),
      getSchema: () => new Map(Object.entries(todoSchema())),
      query: vi.fn(async () => []),
    } as unknown as JazzClient;
    const db = new TestDb(client);

    await expect(db.all(todoQuery({ branches: ["draft"] }))).rejects.toThrow("Invalid branch id");
  });

  it("exposes honest branch merge plumbing", () => {
    const mergeBranch = vi.fn();
    const db = createDbFromClient({ appId: "branch-merge-test" }, {
      ...branchTargetNameSupport(),
      getSchema: () => new Map(Object.entries(todoSchema())),
      mergeBranch,
    } as unknown as JazzClient);

    db.branch("main").merge();

    expect(mergeBranch).toHaveBeenCalledWith(testBranchTargetName("main"));

    const dbWithoutMerge = createDbFromClient({ appId: "branch-merge-missing-test" }, {
      getSchema: () => new Map(Object.entries(todoSchema())),
    } as unknown as JazzClient);

    expect(() => dbWithoutMerge.branch("main").merge()).toThrow(
      "Branch merge is not implemented by this runtime yet.",
    );
  });

  it("transforms inserted rows and waits for durability on the insert handle", async () => {
    const table = todoTable();
    const runtimeRow: Row = {
      id: "todo-1",
      values: [
        { type: "Text", value: "Buy milk" },
        { type: "Boolean", value: false },
      ],
    };
    const { handle: writeResult, client: handleClient } = makeWriteResult(
      runtimeRow,
      "batch-insert",
    );
    const create = vi.fn(() => writeResult);
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      create,
    } as unknown as JazzClient;
    const db = new TestDb(client);

    const pending = db.insert(table, { title: "Buy milk", done: false });

    expect(create).toHaveBeenCalledWith(
      "todos",
      {
        title: { type: "Text", value: "Buy milk" },
        done: { type: "Boolean", value: false },
      },
      undefined,
    );
    expect(pending.batchId).toBe("batch-insert");
    expect(pending.value).toEqual({
      id: "todo-1",
      title: "Buy milk",
      done: false,
    });
    await expect(pending.wait({ tier: "global" })).resolves.toEqual({
      id: "todo-1",
      title: "Buy milk",
      done: false,
    });
    expect(handleClient.waitForPersistedBatch).toHaveBeenCalledWith("batch-insert", "global");
  });

  it("keeps update and delete handles waitable by durability tier", async () => {
    const table = todoTable();
    const { handle: updateHandle, client: updateClient } = makeWriteHandle("batch-update");
    const { handle: deleteHandle, client: deleteClient } = makeWriteHandle("batch-delete");
    const update = vi.fn(() => updateHandle);
    const remove = vi.fn(() => deleteHandle);
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      update,
      delete: remove,
    } as unknown as JazzClient;
    const db = new TestDb(client);

    const updated = db.update(table, "todo-1", { done: true });
    const deleted = db.delete(table, "todo-1");

    expect(update).toHaveBeenCalledWith(
      "todo-1",
      {
        done: { type: "Boolean", value: true },
      },
      undefined,
    );
    expect(remove).toHaveBeenCalledWith("todo-1");
    await expect(updated.wait({ tier: "edge" })).resolves.toBeUndefined();
    await expect(deleted.wait({ tier: "global" })).resolves.toBeUndefined();
    expect(updateClient.waitForPersistedBatch).toHaveBeenCalledWith("batch-update", "edge");
    expect(deleteClient.waitForPersistedBatch).toHaveBeenCalledWith("batch-delete", "global");
  });

  it("routes write handles through the session-aware client-backed db path", async () => {
    const table = todoTable();
    const session: Session = {
      user_id: "alice",
      claims: { role: "writer" },
      authMode: "external",
    };
    const { handle: insertHandle, client: insertClient } = makeWriteResult(
      {
        id: "todo-2",
        values: [
          { type: "Text", value: "With session" },
          { type: "Boolean", value: true },
        ],
      },
      "batch-session-insert",
    );
    const { handle: updateHandle, client: updateClient } = makeWriteHandle("batch-session-update");
    const { handle: deleteHandle, client: deleteClient } = makeWriteHandle("batch-session-delete");
    const createHandleInternal = vi.fn(() => insertHandle);
    const updateHandleInternal = vi.fn(() => updateHandle);
    const deleteHandleInternal = vi.fn(() => deleteHandle);
    const runtimeClient = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      createHandleInternal,
      updateHandleInternal,
      deleteHandleInternal,
    };

    const db = createDbFromClient(
      { appId: "client-backed-persisted" },
      runtimeClient as unknown as JazzClient,
      session,
      "alice@writer",
    );

    const inserted = db.insert(table, { title: "With session", done: true });
    const updated = db.update(table, "todo-2", { done: false });
    const deleted = db.delete(table, "todo-2");

    expect(createHandleInternal).toHaveBeenCalledWith(
      "todos",
      {
        title: { type: "Text", value: "With session" },
        done: { type: "Boolean", value: true },
      },
      session,
      "alice@writer",
      undefined,
    );
    expect(updateHandleInternal).toHaveBeenCalledWith(
      "todo-2",
      {
        done: { type: "Boolean", value: false },
      },
      session,
      "alice@writer",
      undefined,
      undefined,
    );
    expect(deleteHandleInternal).toHaveBeenCalledWith("todo-2", session, "alice@writer");
    expect(inserted.value).toEqual({
      id: "todo-2",
      title: "With session",
      done: true,
    });
    await expect(inserted.wait({ tier: "global" })).resolves.toEqual({
      id: "todo-2",
      title: "With session",
      done: true,
    });
    await expect(updated.wait({ tier: "edge" })).resolves.toBeUndefined();
    await expect(deleted.wait({ tier: "local" })).resolves.toBeUndefined();
    expect(insertClient.waitForPersistedBatch).toHaveBeenCalledWith(
      "batch-session-insert",
      "global",
    );
    expect(updateClient.waitForPersistedBatch).toHaveBeenCalledWith("batch-session-update", "edge");
    expect(deleteClient.waitForPersistedBatch).toHaveBeenCalledWith(
      "batch-session-delete",
      "local",
    );
  });
});
