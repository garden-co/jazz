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

class MultiClientDb extends Db {
  constructor(private readonly clientsBySchema: Map<WasmSchema, JazzClient>) {
    super({ appId: "transaction-db-test" }, null);
  }

  protected override getClient(schema: WasmSchema): JazzClient {
    const client = this.clientsBySchema.get(schema);
    if (!client) {
      throw new Error("missing test client for schema");
    }
    return client;
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

function makeLocalBatchRecord(
  batchId: string,
  mode: LocalBatchRecord["mode"] = "transactional",
): LocalBatchRecord {
  return {
    batchId,
    mode,
    requestedTier: "global",
    sealed: false,
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
      commit: vi.fn(() => "batch-tx"),
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
    const updated = tx.updatePersisted(table, "todo-1", { done: true }, { tier: "edge" });
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
    expect(runtimeTransaction.deletePersisted).toHaveBeenCalledWith("todo-1", { tier: "worker" });
    expect(persisted.value()).toEqual({
      id: "todo-1",
      title: "Transactional",
      done: false,
    });
    await expect(updated.wait()).resolves.toBeUndefined();
    await expect(deleted.wait()).resolves.toBeUndefined();
    expect(tx.commit()).toBe("batch-tx");
    expect(runtimeTransaction.commit).toHaveBeenCalledWith();
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
      commit: vi.fn(() => "batch-session-tx"),
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

    expect(runtimeClient.beginTransactionInternal).toHaveBeenCalledWith(session, "alice@writer");
    expect(tx.commit()).toBe("batch-session-tx");
    expect(runtimeTransaction.commit).toHaveBeenCalledWith();
    expect(inserted).toEqual({
      id: "todo-2",
      title: "Session transaction",
      done: true,
    });
    expect(persisted.batchId()).toBe("batch-session-persisted");
  });

  it("rejects db transaction writes after commit", () => {
    const table = todoTable();
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-closed"),
      create: vi.fn(() => ({
        id: "todo-closed",
        values: [
          { type: "Text", value: "Closed" },
          { type: "Boolean", value: false },
        ],
      })),
      createPersisted: vi.fn(),
      update: vi.fn(),
      updatePersisted: vi.fn(),
      delete: vi.fn(),
      deletePersisted: vi.fn(),
      commit: vi.fn(() => "batch-closed"),
      localBatchRecord: vi.fn((batchId = "batch-closed") => makeLocalBatchRecord(batchId)),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-closed")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
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
    );

    const tx = db.beginTransaction(table);
    expect(tx.commit()).toBe("batch-closed");

    expect(() => tx.insert(table, { title: "Nope", done: false })).toThrow(/committed/i);
    expect(runtimeTransaction.create).not.toHaveBeenCalled();
  });

  it("rejects db transaction writes against a different client/schema", () => {
    const primaryTable = todoTable();
    const secondaryTable = {
      ...todoTable(),
      _schema: todoSchema(),
    };
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-cross-client"),
      create: vi.fn(),
      createPersisted: vi.fn(),
      update: vi.fn(),
      updatePersisted: vi.fn(),
      delete: vi.fn(),
      deletePersisted: vi.fn(),
      commit: vi.fn(() => "batch-cross-client"),
      localBatchRecord: vi.fn((batchId = "batch-cross-client") => makeLocalBatchRecord(batchId)),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-cross-client")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const primaryClient = {
      getSchema: () => new Map(Object.entries(primaryTable._schema)),
      beginTransactionInternal: vi.fn(() => runtimeTransaction),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const secondaryClient = {
      getSchema: () => new Map(Object.entries(secondaryTable._schema)),
      beginTransactionInternal: vi.fn(),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const db = new MultiClientDb(
      new Map([
        [primaryTable._schema, primaryClient],
        [secondaryTable._schema, secondaryClient],
      ]),
    );

    const tx = db.beginTransaction(primaryTable);

    expect(() => tx.insert(secondaryTable, { title: "Wrong client", done: false })).toThrow(
      /cannot be used with table "todos" from a different schema\/client/,
    );
    expect(runtimeTransaction.create).not.toHaveBeenCalled();
  });

  it("creates a typed db direct batch seeded by a table schema", async () => {
    const table = todoTable();
    const runtimeRow: Row = {
      id: "todo-direct-1",
      values: [
        { type: "Text", value: "Direct batch" },
        { type: "Boolean", value: false },
      ],
    };
    const persistedInsert = makePendingWrite("batch-direct-insert", runtimeRow);
    const persistedUpdate = makePendingWrite("batch-direct-update", undefined);
    const persistedDelete = makePendingWrite("batch-direct-delete", undefined);
    const runtimeBatch = {
      batchId: vi.fn(() => "batch-direct"),
      create: vi.fn(() => runtimeRow),
      createPersisted: vi.fn(() => persistedInsert),
      update: vi.fn(),
      updatePersisted: vi.fn(() => persistedUpdate),
      delete: vi.fn(),
      deletePersisted: vi.fn(() => persistedDelete),
      localBatchRecord: vi.fn((batchId = "batch-direct") =>
        makeLocalBatchRecord(batchId, "direct"),
      ),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-direct", "direct")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const beginDirectBatchInternal = vi.fn(() => runtimeBatch);
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginDirectBatchInternal,
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId, "direct")),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const db = new TestDb(client);

    const batch = db.beginDirectBatch(table);
    const inserted = batch.insert(table, { title: "Direct batch", done: false });
    batch.update(table, "todo-direct-1", { done: true });
    batch.delete(table, "todo-direct-1");
    const persisted = batch.insertPersisted(
      table,
      { title: "Direct batch", done: false },
      { tier: "global" },
    );
    const updated = batch.updatePersisted(table, "todo-direct-1", { done: true }, { tier: "edge" });
    const deleted = batch.deletePersisted(table, "todo-direct-1", { tier: "worker" });

    expect(beginDirectBatchInternal).toHaveBeenCalledWith();
    expect(batch.batchId()).toBe("batch-direct");
    expect(inserted).toEqual({
      id: "todo-direct-1",
      title: "Direct batch",
      done: false,
    });
    expect(runtimeBatch.create).toHaveBeenCalledWith("todos", {
      title: { type: "Text", value: "Direct batch" },
      done: { type: "Boolean", value: false },
    });
    expect(runtimeBatch.update).toHaveBeenCalledWith("todo-direct-1", {
      done: { type: "Boolean", value: true },
    });
    expect(runtimeBatch.delete).toHaveBeenCalledWith("todo-direct-1");
    expect(runtimeBatch.createPersisted).toHaveBeenCalledWith(
      "todos",
      {
        title: { type: "Text", value: "Direct batch" },
        done: { type: "Boolean", value: false },
      },
      { tier: "global" },
    );
    expect(runtimeBatch.updatePersisted).toHaveBeenCalledWith(
      "todo-direct-1",
      {
        done: { type: "Boolean", value: true },
      },
      { tier: "edge" },
    );
    expect(runtimeBatch.deletePersisted).toHaveBeenCalledWith("todo-direct-1", { tier: "worker" });
    expect(persisted.value()).toEqual({
      id: "todo-direct-1",
      title: "Direct batch",
      done: false,
    });
    await expect(updated.wait()).resolves.toBeUndefined();
    await expect(deleted.wait()).resolves.toBeUndefined();
    expect(batch.localBatchRecord()).toMatchObject({
      batchId: "batch-direct",
      mode: "direct",
    });
    expect(batch.localBatchRecords()).toEqual([makeLocalBatchRecord("batch-direct", "direct")]);
    expect(batch.acknowledgeRejectedBatch()).toBe(false);
  });

  it("rejects db direct batch writes against a different client/schema", () => {
    const primaryTable = todoTable();
    const secondaryTable = {
      ...todoTable(),
      _schema: todoSchema(),
    };
    const runtimeBatch = {
      batchId: vi.fn(() => "batch-cross-client-direct"),
      create: vi.fn(),
      createPersisted: vi.fn(),
      update: vi.fn(),
      updatePersisted: vi.fn(),
      delete: vi.fn(),
      deletePersisted: vi.fn(),
      localBatchRecord: vi.fn((batchId = "batch-cross-client-direct") =>
        makeLocalBatchRecord(batchId, "direct"),
      ),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-cross-client-direct", "direct")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const primaryClient = {
      getSchema: () => new Map(Object.entries(primaryTable._schema)),
      beginDirectBatchInternal: vi.fn(() => runtimeBatch),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId, "direct")),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const secondaryClient = {
      getSchema: () => new Map(Object.entries(secondaryTable._schema)),
      beginDirectBatchInternal: vi.fn(),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId, "direct")),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const db = new MultiClientDb(
      new Map([
        [primaryTable._schema, primaryClient],
        [secondaryTable._schema, secondaryClient],
      ]),
    );

    const batch = db.beginDirectBatch(primaryTable);

    expect(() => batch.insert(secondaryTable, { title: "Wrong client", done: false })).toThrow(
      /cannot be used with table "todos" from a different schema\/client/,
    );
    expect(runtimeBatch.create).not.toHaveBeenCalled();
  });
});
