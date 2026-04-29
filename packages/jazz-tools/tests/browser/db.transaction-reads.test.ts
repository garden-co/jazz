import { afterEach, describe, expect, it } from "vitest";
import { createDb, type Db, type QueryBuilder, type TableProxy } from "../../src/runtime/db.js";
import type { WasmSchema } from "../../src/drivers/types.js";
import type { CompiledPermissions } from "../../src/permissions/index.js";
import { generateAuthSecret } from "../../src/runtime/auth-secret-store.js";
import {
  fetchPermissionsHead,
  publishStoredPermissions,
  publishStoredSchema,
} from "../../src/runtime/schema-fetch.js";
import { uniqueDbName, waitForQuery, withTimeout } from "./support.js";
import { getTestingServerInfo, type TestingServerInfo } from "./testing-server.js";

const schema: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
    ],
  },
};

interface Todo {
  id: string;
  title: string;
  done: boolean;
}

const todos: TableProxy<Todo, Omit<Todo, "id">> = {
  _table: "todos",
  _schema: schema,
  _rowType: {} as Todo,
  _initType: {} as Omit<Todo, "id">,
};

const allowAllPermissions: CompiledPermissions = {
  todos: {
    select: { using: { type: "True" } },
    insert: { with_check: { type: "True" } },
    update: {
      using: { type: "True" },
      with_check: { type: "True" },
    },
    delete: { using: { type: "True" } },
  },
};

function makeTodoQuery(
  conditions: Array<{ column: string; op: string; value?: unknown }> = [],
): QueryBuilder<Todo> {
  return {
    _table: "todos",
    _schema: schema,
    _rowType: {} as Todo,
    _build() {
      return JSON.stringify({
        table: "todos",
        conditions,
        includes: {},
        orderBy: [],
      });
    },
  };
}

async function publishTransactionReadServer(scope: string): Promise<TestingServerInfo> {
  const testingServer = await getTestingServerInfo(uniqueDbName(`tx-reads-${scope}`));
  const { appId, serverUrl, adminSecret } = testingServer;
  const { hash: schemaHash } = await publishStoredSchema(serverUrl, {
    appId,
    adminSecret,
    schema,
  });
  const { head } = await fetchPermissionsHead(serverUrl, { appId, adminSecret });
  await publishStoredPermissions(serverUrl, {
    appId,
    adminSecret,
    schemaHash,
    permissions: allowAllPermissions,
    expectedParentBundleObjectId: head?.bundleObjectId ?? null,
  });
  return testingServer;
}

describe("db transaction reads browser integration", () => {
  const dbs: Db[] = [];

  function track(db: Db): Db {
    dbs.push(db);
    return db;
  }

  afterEach(async () => {
    for (const db of dbs.splice(0).reverse()) {
      await db.shutdown();
    }
  });

  it("shows only the current transaction's staged inserts through tx.all", async () => {
    const db = track(
      await createDb({
        appId: "db-transaction-reads-test",
        driver: { type: "persistent", dbName: uniqueDbName("tx-insert-reads") },
      }),
    );

    const aliceTx = db.beginTransaction(todos);
    const bobTx = db.beginTransaction(todos);

    const aliceDraft = aliceTx.insert(todos, { title: "Alice draft", done: false });
    bobTx.insert(todos, { title: "Bob draft", done: false });

    const aliceRows = await aliceTx.all<Todo>(makeTodoQuery());
    expect(aliceRows).toEqual([aliceDraft]);

    const bobRows = await bobTx.all<Todo>(makeTodoQuery());
    expect(bobRows.map((row) => row.title)).toEqual(["Bob draft"]);
  });

  it("keeps same-row staged updates isolated to the transaction that issued them", async () => {
    const db = track(
      await createDb({
        appId: "db-transaction-reads-test",
        driver: { type: "persistent", dbName: uniqueDbName("tx-update-reads") },
      }),
    );

    const { value: base } = db.insert(todos, { title: "Shared", done: false });

    const aliceTx = db.beginTransaction(todos);
    const bobTx = db.beginTransaction(todos);

    aliceTx.update(todos, base.id, { title: "Alice draft" });
    bobTx.update(todos, base.id, { title: "Bob draft" });

    expect(await db.one<Todo>(makeTodoQuery())).toEqual(base);

    await expect(aliceTx.one<Todo>(makeTodoQuery())).resolves.toMatchObject({
      id: base.id,
      title: "Alice draft",
      done: false,
    });
    await expect(bobTx.one<Todo>(makeTodoQuery())).resolves.toMatchObject({
      id: base.id,
      title: "Bob draft",
      done: false,
    });
  });

  it("cleans up staged rows when a transaction is rolled back", async () => {
    const db = track(
      await createDb({
        appId: "db-transaction-reads-test",
        driver: { type: "persistent", dbName: uniqueDbName("tx-rollback-reads") },
      }),
    );

    const tx = db.beginTransaction(todos);
    const draft = tx.insert(todos, { title: "Discard me", done: false });

    await expect(tx.all<Todo>(makeTodoQuery())).resolves.toEqual([draft]);

    tx.rollback();

    await expect(tx.all<Todo>(makeTodoQuery())).rejects.toThrow(/rolled back/i);
    await expect(db.all<Todo>(makeTodoQuery())).resolves.toEqual([]);
  });

  it("keeps other open transaction overlays after rolling one transaction back", async () => {
    const db = track(
      await createDb({
        appId: "db-transaction-reads-test",
        driver: { type: "persistent", dbName: uniqueDbName("tx-rollback-isolated") },
      }),
    );

    const aliceTx = db.beginTransaction(todos);
    const bobTx = db.beginTransaction(todos);

    aliceTx.insert(todos, { title: "Alice draft", done: false });
    const bobDraft = bobTx.insert(todos, { title: "Bob draft", done: false });

    aliceTx.rollback();

    await expect(db.all<Todo>(makeTodoQuery())).resolves.toEqual([bobDraft]);
    await expect(bobTx.all<Todo>(makeTodoQuery())).resolves.toEqual([bobDraft]);
  });

  it("does not write rolled-back transaction data for other clients", async () => {
    const syncServer = await publishTransactionReadServer("rollback-other-clients");
    const sharedSecret = generateAuthSecret();
    const writer = track(
      await createDb({
        appId: syncServer.appId,
        driver: { type: "persistent", dbName: uniqueDbName("tx-rollback-writer") },
        serverUrl: syncServer.serverUrl,
        secret: sharedSecret,
      }),
    );
    const reader = track(
      await createDb({
        appId: syncServer.appId,
        driver: { type: "persistent", dbName: uniqueDbName("tx-rollback-reader") },
        serverUrl: syncServer.serverUrl,
        secret: sharedSecret,
      }),
    );

    const baseTitle = `rollback-base-${Date.now()}`;
    const baseWrite = writer.insert(todos, { title: baseTitle, done: false });
    const base = baseWrite.value;
    await withTimeout(baseWrite.wait({ tier: "local" }), 10000, "base insert did not persist");
    await waitForQuery(
      reader,
      makeTodoQuery(),
      (rows) => rows.some((row) => row.id === base.id && row.title === baseTitle),
      "reader should see the baseline row before rollback",
      20000,
      "edge",
    );

    const draftInsertTitle = `rolled-back-insert-${Date.now()}`;
    const insertTx = writer.beginTransaction(todos);
    const draft = insertTx.insert(todos, { title: draftInsertTitle, done: true });
    insertTx.rollback();

    const draftUpdateTitle = `rolled-back-update-${Date.now()}`;
    const updateTx = writer.beginTransaction(todos);
    updateTx.update(todos, base.id, { title: draftUpdateTitle });
    updateTx.rollback();

    const controlTitle = `rollback-control-${Date.now()}`;
    const controlWrite = writer.insert(todos, { title: controlTitle, done: false });
    const control = controlWrite.value;
    await withTimeout(
      controlWrite.wait({ tier: "local" }),
      10000,
      "control insert did not persist",
    );

    const probe = track(
      await createDb({
        appId: syncServer.appId,
        driver: { type: "persistent", dbName: uniqueDbName("tx-rollback-probe") },
        serverUrl: syncServer.serverUrl,
        secret: sharedSecret,
      }),
    );
    const probeRows = await waitForQuery(
      probe,
      makeTodoQuery(),
      (rows) => rows.some((row) => row.id === control.id && row.title === controlTitle),
      "fresh client should see the post-rollback control row",
      20000,
      "edge",
    );

    expect(probeRows.some((row) => row.id === draft.id)).toBe(false);
    expect(probeRows.some((row) => row.title === draftInsertTitle)).toBe(false);
    expect(probeRows.find((row) => row.id === base.id)?.title).toBe(baseTitle);
    expect(probeRows.some((row) => row.title === draftUpdateTitle)).toBe(false);
  }, 60000);
});
