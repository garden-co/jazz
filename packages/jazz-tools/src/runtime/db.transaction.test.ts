import { describe, expect, it, vi } from "vitest";
import { Db, createDbFromClient, type TableProxy } from "./db.js";
import type { WasmSchema } from "../drivers/types.js";
import type { JazzClient, LocalBatchRecord, Row } from "./client.js";
import type { Session } from "./context.js";

class TestDb extends Db {
  constructor(private readonly testClient: JazzClient) {
    super({ appId: "transaction-db-test" }, null);
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
    mode: "transactional",
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

describe("Db transactions", () => {
  it("creates a typed db transaction seeded by a table schema", async () => {
    const table = todoTable();
    const runtimeRow: Row = {
      id: "todo-1",
      values: [
        { type: "Text", value: "Transactional" },
        { type: "Boolean", value: false },
      ],
    };
    const persistedInsert = makePendingWrite("batch-tx-insert", runtimeRow);
    const persistedUpdate = makePendingWrite("batch-tx-update", undefined);
    const persistedDelete = makePendingWrite("batch-tx-delete", undefined);
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-tx"),
      create: vi.fn(() => runtimeRow),
      createPersisted: vi.fn(() => persistedInsert),
      update: vi.fn(),
      updatePersisted: vi.fn(() => persistedUpdate),
      delete: vi.fn(),
      deletePersisted: vi.fn(() => persistedDelete),
      localBatchRecord: vi.fn((batchId = "batch-tx") => makeLocalBatchRecord(batchId)),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-tx")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const beginTransactionInternal = vi.fn(() => runtimeTransaction);
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginTransactionInternal,
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const db = new TestDb(client);

    const tx = db.beginTransaction(table);
    const inserted = tx.insert(table, { title: "Transactional", done: false });
    tx.update(table, "todo-1", { done: true });
    tx.delete(table, "todo-1");
    const persisted = tx.insertPersisted(
      table,
      { title: "Transactional", done: false },
      { tier: "global" },
    );
    const updated = tx.updatePersisted(
      table,
      "todo-1",
      { done: true },
      { tier: "edge" },
    );
    const deleted = tx.deletePersisted(table, "todo-1", { tier: "worker" });

    expect(beginTransactionInternal).toHaveBeenCalledWith();
    expect(tx.batchId()).toBe("batch-tx");
    expect(inserted).toEqual({
      id: "todo-1",
      title: "Transactional",
      done: false,
    });
    expect(runtimeTransaction.create).toHaveBeenCalledWith("todos", {
      title: { type: "Text", value: "Transactional" },
      done: { type: "Boolean", value: false },
    });
    expect(runtimeTransaction.update).toHaveBeenCalledWith("todo-1", {
      done: { type: "Boolean", value: true },
    });
    expect(runtimeTransaction.delete).toHaveBeenCalledWith("todo-1");
    expect(runtimeTransaction.createPersisted).toHaveBeenCalledWith(
      "todos",
      {
        title: { type: "Text", value: "Transactional" },
        done: { type: "Boolean", value: false },
      },
      { tier: "global" },
    );
    expect(runtimeTransaction.updatePersisted).toHaveBeenCalledWith(
      "todo-1",
      {
        done: { type: "Boolean", value: true },
      },
      { tier: "edge" },
    );
    expect(runtimeTransaction.deletePersisted).toHaveBeenCalledWith(
      "todo-1",
      { tier: "worker" },
    );
    expect(persisted.value()).toEqual({
      id: "todo-1",
      title: "Transactional",
      done: false,
    });
    await expect(updated.wait()).resolves.toBeUndefined();
    await expect(deleted.wait()).resolves.toBeUndefined();
    expect(tx.localBatchRecord()).toMatchObject({ batchId: "batch-tx" });
    expect(tx.localBatchRecords()).toEqual([makeLocalBatchRecord("batch-tx")]);
    expect(tx.acknowledgeRejectedBatch()).toBe(false);
  });

  it("threads session-backed db transactions through beginTransactionInternal", () => {
    const table = todoTable();
    const session: Session = {
      user_id: "alice",
      claims: { role: "writer" },
    };
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-session-tx"),
      create: vi.fn(() => ({
        id: "todo-2",
        values: [
          { type: "Text", value: "Session transaction" },
          { type: "Boolean", value: true },
        ],
      })),
      createPersisted: vi.fn(() =>
        makePendingWrite("batch-session-persisted", {
          id: "todo-2",
          values: [
            { type: "Text", value: "Session transaction" },
            { type: "Boolean", value: true },
          ],
        } satisfies Row),
      ),
      update: vi.fn(),
      updatePersisted: vi.fn(() => makePendingWrite("batch-session-update", undefined)),
      delete: vi.fn(),
      deletePersisted: vi.fn(() => makePendingWrite("batch-session-delete", undefined)),
      localBatchRecord: vi.fn((batchId = "batch-session-tx") => makeLocalBatchRecord(batchId)),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-session-tx")]),
      acknowledgeRejectedBatch: vi.fn(() => true),
    };
    const runtimeClient = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginTransactionInternal: vi.fn(() => runtimeTransaction),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const db = createDbFromClient(
      { appId: "client-backed-transaction" },
      runtimeClient as unknown as JazzClient,
      session,
      "alice@writer",
    );

    const tx = db.beginTransaction(table);
    const inserted = tx.insert(table, { title: "Session transaction", done: true });
    const persisted = tx.insertPersisted(table, {
      title: "Session transaction",
      done: true,
    });

    expect(runtimeClient.beginTransactionInternal).toHaveBeenCalledWith(
      session,
      "alice@writer",
    );
    expect(inserted).toEqual({
      id: "todo-2",
      title: "Session transaction",
      done: true,
    });
    expect(persisted.batchId()).toBe("batch-session-persisted");
  });
});
