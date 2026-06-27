import { describe, expect, it, vi } from "vitest";
import { Db, type DbConfig, type TableProxy } from "./db.js";
import type { WasmSchema } from "../drivers/types.js";
import {
  WriteResult,
  WriteHandle,
  type JazzClient,
  type LocalTransactionRecord,
  type Row,
} from "./client.js";
import type { Session } from "./context.js";
import { CoreSource, type CoreClientContext } from "./core-source.js";

class TestCoreSource extends CoreSource<DbConfig> {
  constructor(private readonly client: JazzClient) {
    super();
  }

  override createClient(_context: CoreClientContext<DbConfig>): JazzClient {
    return this.client;
  }
}

class TestDb extends Db {
  constructor(
    private readonly testClient: JazzClient,
    private readonly context: { session?: Session; attribution?: string } | null = null,
  ) {
    super({ appId: "persisted-db-test" }, new TestCoreSource(testClient));
  }

  protected override getClient(_schema: WasmSchema): JazzClient {
    return this.testClient;
  }

  protected override getRuntimeOperationContext(): {
    session?: Session;
    attribution?: string;
  } | null {
    return this.context;
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

function makeLocalTransactionRecord(transactionId: string): LocalTransactionRecord {
  return {
    transactionId,
    kind: "mergeable",
    sealed: true,
    latestSettlement: null,
  };
}

function makeHandleClient(localTransactionRecord: LocalTransactionRecord) {
  return {
    waitForTransaction: vi.fn(async () => undefined),
    localTransactionRecord: vi.fn(() => localTransactionRecord),
  };
}

function makeWriteResult(
  value: Row,
  transactionId: string,
  localTransactionRecord = makeLocalTransactionRecord(transactionId),
) {
  const client = makeHandleClient(localTransactionRecord);
  return {
    handle: new WriteResult(value, transactionId, client as unknown as JazzClient),
    client,
  };
}

function makeWriteHandle(
  transactionId: string,
  localTransactionRecord = makeLocalTransactionRecord(transactionId),
) {
  const client = makeHandleClient(localTransactionRecord);
  return {
    handle: new WriteHandle(transactionId, client as unknown as JazzClient),
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
      "transaction-insert",
    );
    const insert = vi.fn(() => writeResult);
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      insert,
    } as unknown as JazzClient;
    const db = new TestDb(client);

    const pending = db.insert(table, { title: "Buy milk", done: false });

    expect(insert).toHaveBeenCalledWith(
      "todos",
      {
        title: { type: "Text", value: "Buy milk" },
        done: { type: "Boolean", value: false },
      },
      undefined,
      undefined,
      undefined,
    );
    expect(pending.transactionId).toBe("transaction-insert");
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
    expect(handleClient.waitForTransaction).toHaveBeenCalledWith("transaction-insert", "global");
  });

  it("keeps update and delete handles waitable by durability tier", async () => {
    const table = todoTable();
    const { handle: updateHandle, client: updateClient } = makeWriteHandle("transaction-update");
    const { handle: deleteHandle, client: deleteClient } = makeWriteHandle("transaction-delete");
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
      "todos",
      "todo-1",
      {
        done: { type: "Boolean", value: true },
      },
      undefined,
      undefined,
      undefined,
    );
    expect(remove).toHaveBeenCalledWith("todos", "todo-1", undefined, undefined, undefined);
    await expect(updated.wait({ tier: "edge" })).resolves.toBeUndefined();
    await expect(deleted.wait({ tier: "global" })).resolves.toBeUndefined();
    expect(updateClient.waitForTransaction).toHaveBeenCalledWith("transaction-update", "edge");
    expect(deleteClient.waitForTransaction).toHaveBeenCalledWith("transaction-delete", "global");
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
      "transaction-session-insert",
    );
    const { handle: updateHandle, client: updateClient } = makeWriteHandle(
      "transaction-session-update",
    );
    const { handle: deleteHandle, client: deleteClient } = makeWriteHandle(
      "transaction-session-delete",
    );
    const insert = vi.fn(() => insertHandle);
    const update = vi.fn(() => updateHandle);
    const deleteRow = vi.fn(() => deleteHandle);
    const runtimeClient = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      insert,
      update,
      delete: deleteRow,
    };

    const db = new TestDb(runtimeClient as unknown as JazzClient, {
      session,
      attribution: "alice@writer",
    });

    const inserted = db.insert(table, { title: "With session", done: true });
    const updated = db.update(table, "todo-2", { done: false });
    const deleted = db.delete(table, "todo-2");

    expect(insert).toHaveBeenCalledWith(
      "todos",
      {
        title: { type: "Text", value: "With session" },
        done: { type: "Boolean", value: true },
      },
      undefined,
      session,
      "alice@writer",
    );
    expect(update).toHaveBeenCalledWith(
      "todos",
      "todo-2",
      {
        done: { type: "Boolean", value: false },
      },
      undefined,
      session,
      "alice@writer",
    );
    expect(deleteRow).toHaveBeenCalledWith("todos", "todo-2", undefined, session, "alice@writer");
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
    expect(insertClient.waitForTransaction).toHaveBeenCalledWith(
      "transaction-session-insert",
      "global",
    );
    expect(updateClient.waitForTransaction).toHaveBeenCalledWith(
      "transaction-session-update",
      "edge",
    );
    expect(deleteClient.waitForTransaction).toHaveBeenCalledWith(
      "transaction-session-delete",
      "local",
    );
  });
});
