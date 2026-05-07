import { describe, expect, it, vi } from "vitest";
import { Db, createDbFromClient, type TableProxy } from "./db.js";
import type { WasmSchema } from "../drivers/types.js";
import {
  WriteResult,
  WriteHandle,
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
    waitForBatch: vi.fn(async () => undefined),
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
    expect(handleClient.waitForBatch).toHaveBeenCalledWith("batch-insert", "global");
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
    expect(updateClient.waitForBatch).toHaveBeenCalledWith("batch-update", "edge");
    expect(deleteClient.waitForBatch).toHaveBeenCalledWith("batch-delete", "global");
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
    expect(insertClient.waitForBatch).toHaveBeenCalledWith("batch-session-insert", "global");
    expect(updateClient.waitForBatch).toHaveBeenCalledWith("batch-session-update", "edge");
    expect(deleteClient.waitForBatch).toHaveBeenCalledWith("batch-session-delete", "local");
  });
});
