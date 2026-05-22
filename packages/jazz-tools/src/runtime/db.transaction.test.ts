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

function makeHandleClient(mode: LocalBatchRecord["mode"] = "transactional") {
  return {
    waitForBatch: vi.fn(async () => undefined),
    localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId, mode)),
  };
}

function makeWriteHandle(batchId: string, mode: LocalBatchRecord["mode"] = "transactional") {
  const client = makeHandleClient(mode);
  return {
    handle: new WriteHandle(batchId, client as unknown as JazzClient),
    client,
  };
}

type TestTransactionStatus = "active" | "committed" | "rolledBack";

function assertTestTransactionActive(status: TestTransactionStatus, batchId: string): void {
  if (status === "committed") {
    throw new Error(`Transaction ${batchId} is already committed`);
  }
  if (status === "rolledBack") {
    throw new Error(`Transaction ${batchId} has already been rolled back`);
  }
}

describe("Db transactions", () => {
  it("cannot commit a callback transaction by calling commit()", async () => {
    const table = todoTable();
    const runtimeRow: Row = {
      id: "todo-callback-rejected",
      values: [
        { type: "Text", value: "Rejected callback transaction" },
        { type: "Boolean", value: false },
      ],
      batchId: "batch-callback-rejected",
    } as Row;
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-callback-rejected"),
      create: vi.fn(() => runtimeRow),
      update: vi.fn(),
      delete: vi.fn(),
      commit: vi.fn(() => makeWriteHandle("batch-callback-rejected").handle),
      rollback: vi.fn(),
      localBatchRecord: vi.fn((batchId = "batch-callback-rejected") =>
        makeLocalBatchRecord(batchId),
      ),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-callback-rejected")]),
    };
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginTransactionInternal: vi.fn(() => runtimeTransaction),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
    } as unknown as JazzClient;
    const db = new TestDb(client);

    await expect(
      db.transaction(async (tx) => {
        tx.insert(table, { title: "Rejected callback transaction", done: false });
        // @ts-expect-error - commit is not available on DbTransactionScope
        return tx.commit();
      }),
    ).rejects.toEqual(new TypeError("tx.commit is not a function"));
  });

  it("cannot roll back a callback transaction by calling rollback()", async () => {
    const table = todoTable();
    const runtimeRow: Row = {
      id: "todo-callback-rejected",
      values: [
        { type: "Text", value: "Rejected callback transaction" },
        { type: "Boolean", value: false },
      ],
      batchId: "batch-callback-rejected",
    } as Row;
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-callback-rejected"),
      create: vi.fn(() => runtimeRow),
      update: vi.fn(),
      delete: vi.fn(),
      commit: vi.fn(() => makeWriteHandle("batch-callback-rejected").handle),
      rollback: vi.fn(),
      localBatchRecord: vi.fn((batchId = "batch-callback-rejected") =>
        makeLocalBatchRecord(batchId),
      ),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-callback-rejected")]),
    };
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginTransactionInternal: vi.fn(() => runtimeTransaction),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
    } as unknown as JazzClient;
    const db = new TestDb(client);

    await expect(
      db.transaction(async (tx) => {
        tx.insert(table, { title: "Rejected callback transaction", done: false });
        // @ts-expect-error - rollback is not available on DbTransactionScope
        return tx.rollback();
      }),
    ).rejects.toEqual(new TypeError("tx.rollback is not a function"));
  });

  it("throws when committing a db transaction before any actions", () => {
    const beginTransactionInternal = vi.fn();
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginTransactionInternal,
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
    } as unknown as JazzClient;
    const db = new TestDb(client);

    const tx = db.beginTransaction();

    expect(() => tx.commit()).toThrow(
      "DbTransaction.commit() requires at least one table operation first",
    );
    expect(beginTransactionInternal).not.toHaveBeenCalled();
  });

  it("rejects db transaction reads after commit", async () => {
    const table = todoTable();
    const query = todoQuery();
    let status: TestTransactionStatus = "active";
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-read-closed"),
      create: vi.fn(),
      update: vi.fn(),
      delete: vi.fn(),
      query: vi.fn(async () => {
        assertTestTransactionActive(status, "batch-read-closed");
        return [];
      }),
      commit: vi.fn(() => {
        assertTestTransactionActive(status, "batch-read-closed");
        status = "committed";
        return makeWriteHandle("batch-read-closed").handle;
      }),
      localBatchRecord: vi.fn((batchId = "batch-read-closed") => makeLocalBatchRecord(batchId)),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-read-closed")]),
    };
    const runtimeClient = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginTransactionInternal: vi.fn(() => runtimeTransaction),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
    };
    const db = createDbFromClient(
      { appId: "client-backed-transaction" },
      runtimeClient as unknown as JazzClient,
    );

    const tx = db.beginTransaction();
    tx.update(table, "todo-read-closed", { done: false });
    const committed = tx.commit();
    expect(committed).toBeInstanceOf(WriteHandle);
    expect(committed.batchId).toBe("batch-read-closed");

    await expect(tx.all(query)).rejects.toThrow(/committed/i);
    expect(runtimeTransaction.query).toHaveBeenCalledTimes(1);
  });

  it("rejects db transaction rollback after commit", () => {
    const table = todoTable();
    let status: TestTransactionStatus = "active";
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-commit-before-rollback"),
      create: vi.fn(),
      update: vi.fn(),
      delete: vi.fn(),
      commit: vi.fn(() => {
        assertTestTransactionActive(status, "batch-commit-before-rollback");
        status = "committed";
        return makeWriteHandle("batch-commit-before-rollback").handle;
      }),
      rollback: vi.fn(() => {
        assertTestTransactionActive(status, "batch-commit-before-rollback");
        status = "rolledBack";
      }),
      localBatchRecord: vi.fn((batchId = "batch-commit-before-rollback") =>
        makeLocalBatchRecord(batchId),
      ),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-commit-before-rollback")]),
    };
    const runtimeClient = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginTransactionInternal: vi.fn(() => runtimeTransaction),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
    };
    const db = createDbFromClient(
      { appId: "client-backed-commit-before-rollback" },
      runtimeClient as unknown as JazzClient,
    );

    const tx = db.beginTransaction();
    tx.update(table, "todo-commit-before-rollback", { done: false });
    tx.commit();

    expect(() => tx.rollback()).toThrow(/committed/i);
    expect(runtimeTransaction.rollback).toHaveBeenCalledTimes(1);
  });

  it("delegates terminal transaction errors to runtime operations", () => {
    const table = todoTable();
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-runtime-rolled-back"),
      create: vi.fn(),
      update: vi.fn(),
      delete: vi.fn(),
      commit: vi.fn(() => {
        throw new Error("runtime transaction has already been rolled back");
      }),
      rollback: vi.fn(),
      localBatchRecord: vi.fn((batchId = "batch-runtime-rolled-back") =>
        makeLocalBatchRecord(batchId),
      ),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-runtime-rolled-back")]),
    };
    const runtimeClient = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginTransactionInternal: vi.fn(() => runtimeTransaction),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
    };
    const db = createDbFromClient(
      { appId: "client-backed-runtime-status" },
      runtimeClient as unknown as JazzClient,
    );

    const tx = db.beginTransaction();
    tx.update(table, "todo-runtime-status", { done: false });

    expect(() => tx.commit()).toThrow(/runtime transaction has already been rolled back/);
    expect(runtimeTransaction.commit).toHaveBeenCalledTimes(1);
  });

  it("delegates terminal write errors to runtime transaction operations", () => {
    const table = todoTable();
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-runtime-write-rolled-back"),
      create: vi.fn(() => {
        throw new Error("runtime write rejected after rollback");
      }),
      update: vi.fn(),
      delete: vi.fn(),
      commit: vi.fn(() => makeWriteHandle("batch-runtime-write-rolled-back").handle),
      rollback: vi.fn(),
      localBatchRecord: vi.fn((batchId = "batch-runtime-write-rolled-back") =>
        makeLocalBatchRecord(batchId),
      ),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-runtime-write-rolled-back")]),
    };
    const runtimeClient = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginTransactionInternal: vi.fn(() => runtimeTransaction),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
    };
    const db = createDbFromClient(
      { appId: "client-backed-runtime-write-status" },
      runtimeClient as unknown as JazzClient,
    );

    const tx = db.beginTransaction();

    expect(() => tx.insert(table, { title: "Nope", done: false })).toThrow(
      /runtime write rejected after rollback/,
    );
    expect(runtimeTransaction.create).toHaveBeenCalledTimes(1);
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
    };
    const primaryClient = {
      getSchema: () => new Map(Object.entries(primaryTable._schema)),
      beginTransactionInternal: vi.fn(() => runtimeTransaction),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
    } as unknown as JazzClient;
    const secondaryClient = {
      getSchema: () => new Map(Object.entries(secondaryTable._schema)),
      beginTransactionInternal: vi.fn(),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
    } as unknown as JazzClient;
    const db = new MultiClientDb(
      new Map([
        [primaryTable._schema, primaryClient],
        [secondaryTable._schema, secondaryClient],
      ]),
    );

    const tx = db.beginTransaction();
    tx.update(primaryTable, "todo-cross-client", { done: true });

    expect(() => tx.insert(secondaryTable, { title: "Wrong client", done: false })).toThrow(
      /cannot be used with table "todos" from a different schema\/client/,
    );
    expect(runtimeTransaction.update).toHaveBeenCalledTimes(1);
    expect(runtimeTransaction.create).not.toHaveBeenCalled();
  });

  it("throws when committing a db batch before any actions", () => {
    const beginBatchInternal = vi.fn();
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginBatchInternal,
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId, "direct")),
    } as unknown as JazzClient;
    const db = new TestDb(client);

    const batch = db.beginBatch();

    expect(() => batch.commit()).toThrow(
      "DbDirectBatch.commit() requires at least one table operation first",
    );
    expect(beginBatchInternal).not.toHaveBeenCalled();
  });

  it("rejects db batch rollback after commit", () => {
    const table = todoTable();
    let status: TestTransactionStatus = "active";
    const runtimeBatch = {
      batchId: vi.fn(() => "batch-direct-commit-before-rollback"),
      create: vi.fn(),
      update: vi.fn(() => {
        assertTestTransactionActive(status, "batch-direct-commit-before-rollback");
      }),
      delete: vi.fn(),
      commit: vi.fn(() => {
        assertTestTransactionActive(status, "batch-direct-commit-before-rollback");
        status = "committed";
        return makeWriteHandle("batch-direct-commit-before-rollback", "direct").handle;
      }),
      rollback: vi.fn(() => {
        assertTestTransactionActive(status, "batch-direct-commit-before-rollback");
        status = "rolledBack";
      }),
      localBatchRecord: vi.fn((batchId = "batch-direct-commit-before-rollback") =>
        makeLocalBatchRecord(batchId, "direct"),
      ),
      localBatchRecords: vi.fn(() => [
        makeLocalBatchRecord("batch-direct-commit-before-rollback", "direct"),
      ]),
    };
    const runtimeClient = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginBatchInternal: vi.fn(() => runtimeBatch),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId, "direct")),
    };
    const db = createDbFromClient(
      { appId: "client-backed-batch-commit-before-rollback" },
      runtimeClient as unknown as JazzClient,
    );

    const batch = db.beginBatch();
    batch.update(table, "todo-direct-commit-before-rollback", { done: false });
    batch.commit();

    expect(() => batch.rollback()).toThrow(/committed/i);
  });

  it("rolls back a callback batch when the callback throws after a write", () => {
    const table = todoTable();
    const runtimeBatch = {
      batchId: vi.fn(() => "batch-direct-thrown-callback"),
      create: vi.fn(() => ({
        id: "todo-direct-thrown-callback",
        values: [
          { type: "Text", value: "Thrown callback batch" },
          { type: "Boolean", value: false },
        ],
        batchId: "batch-direct-thrown-callback",
      })),
      update: vi.fn(),
      delete: vi.fn(),
      commit: vi.fn(() => makeWriteHandle("batch-direct-thrown-callback", "direct").handle),
      rollback: vi.fn(),
      localBatchRecord: vi.fn((batchId = "batch-direct-thrown-callback") =>
        makeLocalBatchRecord(batchId, "direct"),
      ),
      localBatchRecords: vi.fn(() => [
        makeLocalBatchRecord("batch-direct-thrown-callback", "direct"),
      ]),
    };
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginBatchInternal: vi.fn(() => runtimeBatch),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId, "direct")),
    } as unknown as JazzClient;
    const db = new TestDb(client);
    const error = new Error("callback failed");

    expect(() =>
      db.batch((batch) => {
        batch.insert(table, { title: "Thrown callback batch", done: false });
        throw error;
      }),
    ).toThrow(error);

    expect(runtimeBatch.commit).not.toHaveBeenCalled();
    expect(runtimeBatch.rollback).toHaveBeenCalledTimes(1);
  });

  it("rejects db batch writes against a different client/schema", () => {
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
    };
    const primaryClient = {
      getSchema: () => new Map(Object.entries(primaryTable._schema)),
      beginBatchInternal: vi.fn(() => runtimeBatch),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId, "direct")),
    } as unknown as JazzClient;
    const secondaryClient = {
      getSchema: () => new Map(Object.entries(secondaryTable._schema)),
      beginBatchInternal: vi.fn(),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId, "direct")),
    } as unknown as JazzClient;
    const db = new MultiClientDb(
      new Map([
        [primaryTable._schema, primaryClient],
        [secondaryTable._schema, secondaryClient],
      ]),
    );

    const batch = db.beginBatch();
    batch.update(primaryTable, "todo-cross-client-direct", { done: true });

    expect(() => batch.insert(secondaryTable, { title: "Wrong client", done: false })).toThrow(
      /cannot be used with table "todos" from a different schema\/client/,
    );
    expect(runtimeBatch.update).toHaveBeenCalledTimes(1);
    expect(runtimeBatch.create).not.toHaveBeenCalled();
  });
});
