import { describe, expect, it, vi } from "vitest";
import { Db, createDbFromClient, type TableProxy } from "./db.js";
import type { WasmSchema } from "../drivers/types.js";
import {
  InsertHandle,
  WriteHandle,
  type JazzClient,
  type LocalBatchRecord,
  type Row,
} from "./client.js";
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

function todoQuery() {
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
      });
    },
  };
}

function makeLocalBatchRecord(
  batchId: string,
  mode: LocalBatchRecord["mode"] = "transactional",
): LocalBatchRecord {
  return {
    batchId,
    mode,
    sealed: false,
    latestSettlement: null,
  };
}

function makeHandleClient(mode: LocalBatchRecord["mode"] = "transactional", acknowledged = false) {
  return {
    waitForPersistedBatch: vi.fn(async () => undefined),
    localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId, mode)),
    acknowledgeRejectedBatch: vi.fn(() => acknowledged),
  };
}

function makeWriteHandle(batchId: string, mode: LocalBatchRecord["mode"] = "transactional") {
  const client = makeHandleClient(mode);
  return {
    handle: new WriteHandle(batchId, client as unknown as JazzClient),
    client,
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
      batchId: "batch-tx",
    } as Row;
    const committedRuntime = makeWriteHandle("batch-tx");
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-tx"),
      create: vi.fn(() => runtimeRow),
      update: vi.fn(() => undefined),
      delete: vi.fn(() => undefined),
      commit: vi.fn(() => committedRuntime.handle),
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
    const updated = tx.update(table, "todo-1", { done: true });
    const deleted = tx.delete(table, "todo-1");

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
    expect(updated).toBeUndefined();
    expect(deleted).toBeUndefined();
    const committed = tx.commit();
    expect(committed).toBeInstanceOf(WriteHandle);
    expect(committed.batchId).toBe("batch-tx");
    await expect(committed.wait({ tier: "global" })).resolves.toBeUndefined();
    expect(committedRuntime.client.waitForPersistedBatch).toHaveBeenCalledWith(
      "batch-tx",
      "global",
    );
    expect(runtimeTransaction.commit).toHaveBeenCalledWith();
    expect(tx.localBatchRecord()).toMatchObject({ batchId: "batch-tx" });
    expect(tx.localBatchRecords()).toEqual([makeLocalBatchRecord("batch-tx")]);
    expect(tx.acknowledgeRejectedBatch()).toBe(false);
  });

  it("threads session-backed db transactions through beginTransactionInternal", async () => {
    const table = todoTable();
    const session: Session = {
      user_id: "alice",
      claims: { role: "writer" },
      authMode: "external",
    };
    const runtimeRow = {
      id: "todo-2",
      values: [
        { type: "Text", value: "Session transaction" },
        { type: "Boolean", value: true },
      ],
      batchId: "batch-session-tx",
    } as Row;
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-session-tx"),
      create: vi.fn(() => runtimeRow),
      update: vi.fn(() => undefined),
      delete: vi.fn(() => undefined),
      commit: vi.fn(() => makeWriteHandle("batch-session-tx").handle),
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

    expect(runtimeClient.beginTransactionInternal).toHaveBeenCalledWith(session, "alice@writer");
    const committed = tx.commit();
    expect(committed).toBeInstanceOf(WriteHandle);
    expect(committed.batchId).toBe("batch-session-tx");
    expect(runtimeTransaction.commit).toHaveBeenCalledWith();
    expect(inserted).toEqual({
      id: "todo-2",
      title: "Session transaction",
      done: true,
    });
  });

  it("commits a typed callback transaction and returns the callback result handle", async () => {
    const table = todoTable();
    const runtimeRow: Row = {
      id: "todo-callback",
      values: [
        { type: "Text", value: "Callback transaction" },
        { type: "Boolean", value: false },
      ],
      batchId: "batch-callback",
    } as Row;
    const committedRuntime = makeWriteHandle("batch-callback");
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-callback"),
      create: vi.fn(() => runtimeRow),
      update: vi.fn(() => undefined),
      delete: vi.fn(() => undefined),
      commit: vi.fn(() => committedRuntime.handle),
      localBatchRecord: vi.fn((batchId = "batch-callback") => makeLocalBatchRecord(batchId)),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-callback")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      waitForPersistedBatch: vi.fn(async () => undefined),
      beginTransactionInternal: vi.fn(() => runtimeTransaction),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const db = new TestDb(client);

    const handle = await db.transaction(table, (tx) => {
      expect("commit" in tx).toBe(false);
      const todo = tx.insert(table, { title: "Callback transaction", done: false });
      return todo;
    });

    expect(handle).toBeInstanceOf(InsertHandle);
    expect(handle.batchId).toBe("batch-callback");
    expect(handle.value).toEqual({
      id: "todo-callback",
      title: "Callback transaction",
      done: false,
    });
    expect(runtimeTransaction.commit).toHaveBeenCalledTimes(1);
    await expect(handle.wait({ tier: "global" })).resolves.toEqual({
      id: "todo-callback",
      title: "Callback transaction",
      done: false,
    });
    expect(client.waitForPersistedBatch).toHaveBeenCalledWith("batch-callback", "global");
  });

  it("does not commit a typed callback transaction when the callback rejects", async () => {
    const table = todoTable();
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-callback-rejected"),
      create: vi.fn(),
      update: vi.fn(),
      delete: vi.fn(),
      commit: vi.fn(() => makeWriteHandle("batch-callback-rejected").handle),
      localBatchRecord: vi.fn((batchId = "batch-callback-rejected") =>
        makeLocalBatchRecord(batchId),
      ),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-callback-rejected")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginTransactionInternal: vi.fn(() => runtimeTransaction),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const db = new TestDb(client);
    const error = new Error("callback failed");

    await expect(db.transaction(table, async () => Promise.reject(error))).rejects.toBe(error);

    expect(runtimeTransaction.commit).not.toHaveBeenCalled();
  });

  it("rejects db transaction writes after commit", () => {
    const table = todoTable();
    const runtimeRow = {
      id: "todo-closed",
      values: [
        { type: "Text", value: "Closed" },
        { type: "Boolean", value: false },
      ],
      batchId: "batch-closed",
    } as Row;
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-closed"),
      create: vi.fn(() => runtimeRow),
      update: vi.fn(),
      delete: vi.fn(),
      commit: vi.fn(() => makeWriteHandle("batch-closed").handle),
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
    const committed = tx.commit();
    expect(committed).toBeInstanceOf(WriteHandle);
    expect(committed.batchId).toBe("batch-closed");

    expect(() => tx.insert(table, { title: "Nope", done: false })).toThrow(/committed/i);
    expect(runtimeTransaction.create).not.toHaveBeenCalled();
  });

  it("supports typed reads scoped to the open transaction", async () => {
    const table = todoTable();
    const query = todoQuery();
    const runtimeRow: Row = {
      id: "todo-read-1",
      values: [
        { type: "Text", value: "Transactional read" },
        { type: "Boolean", value: false },
      ],
    };
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-read"),
      create: vi.fn(() => runtimeRow),
      update: vi.fn(),
      delete: vi.fn(),
      query: vi.fn(async () => [runtimeRow]),
      commit: vi.fn(() => makeWriteHandle("batch-read").handle),
      localBatchRecord: vi.fn((batchId = "batch-read") => makeLocalBatchRecord(batchId)),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-read")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginTransactionInternal: vi.fn(() => runtimeTransaction),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const db = new TestDb(client);

    const tx = db.beginTransaction(table);
    tx.insert(table, { title: "Transactional read", done: false });

    await expect(tx.all(query)).resolves.toEqual([
      {
        id: "todo-read-1",
        title: "Transactional read",
        done: false,
      },
    ]);
    await expect(tx.one(query)).resolves.toEqual({
      id: "todo-read-1",
      title: "Transactional read",
      done: false,
    });

    expect(runtimeTransaction.query).toHaveBeenCalledTimes(2);
  });

  it("rejects db transaction reads after commit", async () => {
    const table = todoTable();
    const query = todoQuery();
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-read-closed"),
      create: vi.fn(),
      update: vi.fn(),
      delete: vi.fn(),
      query: vi.fn(),
      commit: vi.fn(() => makeWriteHandle("batch-read-closed").handle),
      localBatchRecord: vi.fn((batchId = "batch-read-closed") => makeLocalBatchRecord(batchId)),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-read-closed")]),
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
    const committed = tx.commit();
    expect(committed).toBeInstanceOf(WriteHandle);
    expect(committed.batchId).toBe("batch-read-closed");

    await expect(tx.all(query)).rejects.toThrow(/committed/i);
    expect(runtimeTransaction.query).not.toHaveBeenCalled();
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
      update: vi.fn(),
      delete: vi.fn(),
      commit: vi.fn(() => makeWriteHandle("batch-cross-client").handle),
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
      batchId: "batch-direct",
    } as Row;
    const committedRuntime = makeWriteHandle("batch-direct", "direct");
    const runtimeBatch = {
      batchId: vi.fn(() => "batch-direct"),
      create: vi.fn(() => runtimeRow),
      update: vi.fn(() => undefined),
      delete: vi.fn(() => undefined),
      commit: vi.fn(() => committedRuntime.handle),
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
    const updated = batch.update(table, "todo-direct-1", { done: true });
    const deleted = batch.delete(table, "todo-direct-1");

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
    expect(updated).toBeUndefined();
    expect(deleted).toBeUndefined();
    const committed = batch.commit();
    expect(committed).toBeInstanceOf(WriteHandle);
    expect(committed.batchId).toBe("batch-direct");
    await expect(committed.wait({ tier: "global" })).resolves.toBeUndefined();
    expect(committedRuntime.client.waitForPersistedBatch).toHaveBeenCalledWith(
      "batch-direct",
      "global",
    );
    expect(runtimeBatch.commit).toHaveBeenCalledWith();
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
      update: vi.fn(),
      delete: vi.fn(),
      commit: vi.fn(() => makeWriteHandle("batch-cross-client-direct", "direct").handle),
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
