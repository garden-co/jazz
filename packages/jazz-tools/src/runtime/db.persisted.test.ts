import { describe, expect, it, vi } from "vitest";
import {
  Db,
  createDbFromClient,
  type TableProxy,
} from "./db.js";
import type { WasmSchema } from "../drivers/types.js";
import type { JazzClient, LocalBatchRecord, Row } from "./client.js";
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
    requestedTier: "global",
    latestSettlement: null,
  };
}

function makePendingWrite<T>(batchId: string, value: T) {
  return {
    batchId: () => batchId,
    value: () => value,
    wait: vi.fn(async () => value),
  };
}

describe("Db persisted writes", () => {
  it("transforms persisted insert rows and exposes batch helpers", async () => {
    const table = todoTable();
    const runtimeRow: Row = {
      id: "todo-1",
      values: [
        { type: "Text", value: "Buy milk" },
        { type: "Boolean", value: false },
      ],
    };
    const pendingWrite = makePendingWrite("batch-insert", runtimeRow);
    const localBatchRecord = makeLocalBatchRecord("batch-insert");
    const createPersisted = vi.fn(() => pendingWrite);
    const loadLocalBatchRecord = vi.fn(() => localBatchRecord);
    const acknowledgeRejectedBatch = vi.fn(() => false);
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      createPersisted,
      localBatchRecord: loadLocalBatchRecord,
      acknowledgeRejectedBatch,
    } as unknown as JazzClient;
    const db = new TestDb(client);

    const pending = db.insertPersisted(
      table,
      { title: "Buy milk", done: false },
      { tier: "global" },
    );

    expect(createPersisted).toHaveBeenCalledWith(
      "todos",
      {
        title: { type: "Text", value: "Buy milk" },
        done: { type: "Boolean", value: false },
      },
      { tier: "global" },
    );
    expect(pending.batchId()).toBe("batch-insert");
    expect(pending.value()).toEqual({
      id: "todo-1",
      title: "Buy milk",
      done: false,
    });
    await expect(pending.wait()).resolves.toEqual({
      id: "todo-1",
      title: "Buy milk",
      done: false,
    });
    expect(pending.localBatchRecord()).toEqual(localBatchRecord);
    expect(loadLocalBatchRecord).toHaveBeenCalledWith("batch-insert");
    expect(pending.acknowledgeRejectedBatch()).toBe(false);
    expect(acknowledgeRejectedBatch).toHaveBeenCalledWith("batch-insert");
  });

  it("keeps persisted update and delete handles batch-addressable", async () => {
    const table = todoTable();
    const updatePending = makePendingWrite("batch-update", undefined);
    const deletePending = makePendingWrite("batch-delete", undefined);
    const updatePersisted = vi.fn(() => updatePending);
    const deletePersisted = vi.fn(() => deletePending);
    const localBatchRecord = vi.fn((batchId: string) => makeLocalBatchRecord(batchId));
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      updatePersisted,
      deletePersisted,
      localBatchRecord,
      acknowledgeRejectedBatch: vi.fn(() => true),
    } as unknown as JazzClient;
    const db = new TestDb(client);

    const updated = db.updatePersisted(
      table,
      "todo-1",
      { done: true },
      { tier: "edge" },
    );
    const deleted = db.deletePersisted(table, "todo-1", { tier: "global" });

    expect(updatePersisted).toHaveBeenCalledWith(
      "todo-1",
      {
        done: { type: "Boolean", value: true },
      },
      { tier: "edge" },
    );
    expect(deletePersisted).toHaveBeenCalledWith("todo-1", { tier: "global" });
    await expect(updated.wait()).resolves.toBeUndefined();
    await expect(deleted.wait()).resolves.toBeUndefined();
    expect(updated.localBatchRecord()).toMatchObject({ batchId: "batch-update" });
    expect(deleted.localBatchRecord()).toMatchObject({ batchId: "batch-delete" });
  });

  it("routes persisted writes through the session-aware client-backed db path", () => {
    const table = todoTable();
    const session: Session = {
      user_id: "alice",
      claims: { role: "writer" },
    };
    const insertPending = makePendingWrite("batch-session-insert", {
      id: "todo-2",
      values: [
        { type: "Text", value: "With session" },
        { type: "Boolean", value: true },
      ],
    } satisfies Row);
    const updatePending = makePendingWrite("batch-session-update", undefined);
    const deletePending = makePendingWrite("batch-session-delete", undefined);
    const createPersistedInternal = vi.fn(() => insertPending);
    const updatePersistedInternal = vi.fn(() => updatePending);
    const deletePersistedInternal = vi.fn(() => deletePending);
    const runtimeClient = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      createPersistedInternal,
      updatePersistedInternal,
      deletePersistedInternal,
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };

    const db = createDbFromClient(
      { appId: "client-backed-persisted" },
      runtimeClient as unknown as JazzClient,
      session,
      "alice@writer",
    );

    const inserted = db.insertPersisted(
      table,
      { title: "With session", done: true },
      { tier: "global" },
    );
    const updated = db.updatePersisted(
      table,
      "todo-2",
      { done: false },
      { tier: "edge" },
    );
    const deleted = db.deletePersisted(table, "todo-2", { tier: "worker" });

    expect(createPersistedInternal).toHaveBeenCalledWith(
      "todos",
      {
        title: { type: "Text", value: "With session" },
        done: { type: "Boolean", value: true },
      },
      session,
      "alice@writer",
      { tier: "global" },
    );
    expect(updatePersistedInternal).toHaveBeenCalledWith(
      "todo-2",
      {
        done: { type: "Boolean", value: false },
      },
      session,
      "alice@writer",
      { tier: "edge" },
    );
    expect(deletePersistedInternal).toHaveBeenCalledWith(
      "todo-2",
      session,
      "alice@writer",
      { tier: "worker" },
    );
    expect(inserted.value()).toEqual({
      id: "todo-2",
      title: "With session",
      done: true,
    });
    expect(updated.batchId()).toBe("batch-session-update");
    expect(deleted.batchId()).toBe("batch-session-delete");
  });
});
