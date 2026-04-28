import { describe, expect, it, vi } from "vitest";
import { Db, createDbFromClient, type TableProxy } from "./db.js";
import type { WasmSchema } from "../drivers/types.js";
import { WriteHandle, type JazzClient, type LocalBatchRecord, type Row } from "./client.js";
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
  it("runs a typed transaction callback and returns a waitable committed handle", async () => {
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

    const committed = db.transaction((tx) => {
      const inserted = tx.insert(table, { title: "Transactional", done: false });
      const updated = tx.update(table, "todo-1", { done: true });
      const deleted = tx.delete(table, "todo-1");

      expect(tx.batchId()).toBe("batch-tx");
      expect(inserted).toEqual({
        id: "todo-1",
        title: "Transactional",
        done: false,
      });
      expect(updated).toBeUndefined();
      expect(deleted).toBeUndefined();
      expect(tx.localBatchRecord()).toMatchObject({ batchId: "batch-tx" });
      expect(tx.localBatchRecords()).toEqual([makeLocalBatchRecord("batch-tx")]);
      expect(tx.acknowledgeRejectedBatch()).toBe(false);
    });

    expect(beginTransactionInternal).toHaveBeenCalledWith();
    expect(runtimeTransaction.create).toHaveBeenCalledWith("todos", {
      title: { type: "Text", value: "Transactional" },
      done: { type: "Boolean", value: false },
    });
    expect(runtimeTransaction.update).toHaveBeenCalledWith("todo-1", {
      done: { type: "Boolean", value: true },
    });
    expect(runtimeTransaction.delete).toHaveBeenCalledWith("todo-1");
    expect(committed).toBeInstanceOf(WriteHandle);
    expect(committed.batchId).toBe("batch-tx");
    await expect(committed.wait({ tier: "global" })).resolves.toBeUndefined();
    expect(committedRuntime.client.waitForPersistedBatch).toHaveBeenCalledWith(
      "batch-tx",
      "global",
    );
    expect(runtimeTransaction.commit).toHaveBeenCalledWith();
  });

  it("threads session-backed transaction callbacks through beginTransactionInternal", () => {
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

    const committed = db.transaction((tx) => {
      expect(tx.insert(table, { title: "Session transaction", done: true })).toEqual({
        id: "todo-2",
        title: "Session transaction",
        done: true,
      });
    });

    expect(runtimeClient.beginTransactionInternal).toHaveBeenCalledWith(session, "alice@writer");
    expect(committed).toBeInstanceOf(WriteHandle);
    expect(committed.batchId).toBe("batch-session-tx");
    expect(runtimeTransaction.commit).toHaveBeenCalledWith();
  });

  it("keeps typed reads scoped to an async transaction callback", async () => {
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

    const committed = await db.transaction(async (tx) => {
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
    });

    expect(committed.batchId).toBe("batch-read");
    expect(runtimeTransaction.query).toHaveBeenCalledTimes(2);
    expect(runtimeTransaction.commit).toHaveBeenCalledWith();
  });

  it("does not commit when a transaction callback fails", () => {
    const table = todoTable();
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-fail"),
      create: vi.fn(),
      update: vi.fn(),
      delete: vi.fn(),
      commit: vi.fn(() => makeWriteHandle("batch-fail").handle),
      localBatchRecord: vi.fn((batchId = "batch-fail") => makeLocalBatchRecord(batchId)),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-fail")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginTransactionInternal: vi.fn(() => runtimeTransaction),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const db = new TestDb(client);

    expect(() =>
      db.transaction((tx) => {
        tx.update(table, "todo-1", { done: true });
        throw new Error("policy preflight failed");
      }),
    ).toThrow("policy preflight failed");
    expect(runtimeTransaction.commit).not.toHaveBeenCalled();
  });

  it("rejects transaction writes against a different client/schema", () => {
    const primaryTable = todoTable();
    const secondaryTable = {
      ...todoTable(),
      _schema: todoSchema(),
    };
    const runtimeRow: Row = {
      id: "todo-cross-client",
      values: [
        { type: "Text", value: "Right client" },
        { type: "Boolean", value: false },
      ],
      batchId: "batch-cross-client",
    } as Row;
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-cross-client"),
      create: vi.fn(() => runtimeRow),
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

    expect(() =>
      db.transaction((tx) => {
        tx.insert(primaryTable, { title: "Right client", done: false });
        tx.insert(secondaryTable, { title: "Wrong client", done: false });
      }),
    ).toThrow(/cannot be used with table "todos" from a different schema\/client/);
    expect(runtimeTransaction.create).toHaveBeenCalledTimes(1);
    expect(runtimeTransaction.commit).not.toHaveBeenCalled();
  });

  it("runs a typed direct batch callback and returns a waitable committed handle", async () => {
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

    const committed = db.batch((batch) => {
      const inserted = batch.insert(table, { title: "Direct batch", done: false });
      const updated = batch.update(table, "todo-direct-1", { done: true });
      const deleted = batch.delete(table, "todo-direct-1");

      expect(batch.batchId()).toBe("batch-direct");
      expect(inserted).toEqual({
        id: "todo-direct-1",
        title: "Direct batch",
        done: false,
      });
      expect(updated).toBeUndefined();
      expect(deleted).toBeUndefined();
      expect(batch.localBatchRecord()).toMatchObject({
        batchId: "batch-direct",
        mode: "direct",
      });
      expect(batch.localBatchRecords()).toEqual([makeLocalBatchRecord("batch-direct", "direct")]);
      expect(batch.acknowledgeRejectedBatch()).toBe(false);
    });

    expect(beginDirectBatchInternal).toHaveBeenCalledWith();
    expect(runtimeBatch.create).toHaveBeenCalledWith("todos", {
      title: { type: "Text", value: "Direct batch" },
      done: { type: "Boolean", value: false },
    });
    expect(runtimeBatch.update).toHaveBeenCalledWith("todo-direct-1", {
      done: { type: "Boolean", value: true },
    });
    expect(runtimeBatch.delete).toHaveBeenCalledWith("todo-direct-1");
    expect(committed).toBeInstanceOf(WriteHandle);
    expect(committed.batchId).toBe("batch-direct");
    await expect(committed.wait({ tier: "global" })).resolves.toBeUndefined();
    expect(committedRuntime.client.waitForPersistedBatch).toHaveBeenCalledWith(
      "batch-direct",
      "global",
    );
    expect(runtimeBatch.commit).toHaveBeenCalledWith();
  });

  it("rejects direct batch writes against a different client/schema", () => {
    const primaryTable = todoTable();
    const secondaryTable = {
      ...todoTable(),
      _schema: todoSchema(),
    };
    const runtimeRow: Row = {
      id: "todo-cross-client-direct",
      values: [
        { type: "Text", value: "Right client" },
        { type: "Boolean", value: false },
      ],
      batchId: "batch-cross-client-direct",
    } as Row;
    const runtimeBatch = {
      batchId: vi.fn(() => "batch-cross-client-direct"),
      create: vi.fn(() => runtimeRow),
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

    expect(() =>
      db.batch((batch) => {
        batch.insert(primaryTable, { title: "Right client", done: false });
        batch.insert(secondaryTable, { title: "Wrong client", done: false });
      }),
    ).toThrow(/cannot be used with table "todos" from a different schema\/client/);
    expect(runtimeBatch.create).toHaveBeenCalledTimes(1);
    expect(runtimeBatch.commit).not.toHaveBeenCalled();
  });
});
