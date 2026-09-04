import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { createDb, schema, type Db, type RowOf } from "../../src/index.js";
import { uniqueDbName } from "./support.js";
import { deploy } from "../../src/dev/catalogue.js";
import { getJazzServerInfo } from "./testing-server.js";

const app = schema.defineApp({
  todos: schema.table({
    title: schema.string(),
    done: schema.boolean(),
  }),
});

type Todo = RowOf<typeof app.todos>;

let db: Db;

beforeEach(async () => {
  db = await createDb({
    appId: "db-transaction-reads-test",
    driver: { type: "persistent", dbName: uniqueDbName("db-transaction-reads-test") },
  });
});

afterEach(async () => {
  await db.shutdown();
});

describe("db exclusive transaction initialization browser integration", () => {
  it("rejects beginning before the JazzClient exists", () => {
    expect(() => db.beginExclusiveTransaction()).toThrow(
      "Cannot begin an exclusive transaction before the JazzClient has been created. Run a query or mutation first.",
    );
  });
});

describe("db exclusive transaction reads browser integration", () => {
  beforeEach(async () => {
    await db.all(app.todos);
  });

  it("anchors a read-only transaction snapshot when begin is called", async () => {
    const { value: beforeBegin } = db.insert(app.todos, {
      title: "visible at begin",
      done: false,
    });
    const tx = db.beginExclusiveTransaction();
    db.insert(app.todos, { title: "committed after begin", done: false });

    await expect(tx.all(app.todos)).resolves.toEqual([beforeBegin]);
    await tx.rollback();
  });

  it("shows only the current transaction's staged inserts through tx.all", async () => {
    const aliceTx = db.beginExclusiveTransaction();
    const bobTx = db.beginExclusiveTransaction();

    const aliceDraft = aliceTx.insert(app.todos, { title: "Alice draft", done: false });
    const bobDraft = bobTx.insert(app.todos, { title: "Bob draft", done: false });

    const aliceRows = await aliceTx.all(app.todos);
    expect(aliceRows).toEqual([aliceDraft]);

    const bobRows = await bobTx.all(app.todos);
    expect(bobRows).toEqual([bobDraft]);

    const globalRows = await db.all(app.todos);
    expect(globalRows).toEqual([]);
  });

  it("keeps same-row staged updates isolated to the transaction that issued them", async () => {
    const { value: base } = db.insert(app.todos, { title: "Shared", done: false });

    const aliceTx = db.beginExclusiveTransaction();
    const bobTx = db.beginExclusiveTransaction();

    aliceTx.update(app.todos, base.id, { title: "Alice draft" });
    bobTx.update(app.todos, base.id, { title: "Bob draft" });

    expect(await db.one(app.todos)).toEqual(base);

    await expect(aliceTx.one(app.todos)).resolves.toMatchObject({
      id: base.id,
      title: "Alice draft",
      done: false,
    });
    await expect(bobTx.one(app.todos)).resolves.toMatchObject({
      id: base.id,
      title: "Bob draft",
      done: false,
    });
  });

  it("keeps staged deletes isolated to the transaction that issued them", async () => {
    const { value: todo } = db.insert(app.todos, { title: "Shared", done: false });
    const tx = db.beginExclusiveTransaction();

    tx.delete(app.todos, todo.id);

    expect(await db.one(app.todos)).toEqual(todo);
    expect(await tx.one(app.todos)).toBeNull();

    await tx.commit();

    expect(await db.one(app.todos)).toBeNull();
  });

  it("makes transaction writes visible globally once the transaction commits and the authority accepts the transaction", async () => {
    const tx = db.beginExclusiveTransaction();
    const insertedTodo = tx.insert(app.todos, { title: "Exclusive transaction", done: false });

    expect(await db.one(app.todos)).toBeNull();

    await tx.commit();

    expect(await db.one(app.todos)).toMatchObject(insertedTodo);
  });

  it("rejects transaction operations after commit", async () => {
    const tx = db.beginExclusiveTransaction();
    tx.insert(app.todos, { title: "Committed transaction", done: false });
    const openTransactionId = tx.openTransactionId();

    await tx.commit();

    const coreError = `open transaction ${openTransactionId} is already committed`;
    expect(() => tx.commit()).toThrow(`Write error: ${coreError}`);
    expect(() => tx.rollback()).toThrow(`Write error: ${coreError}`);
    expect(() => tx.insert(app.todos, { title: "Nope", done: false })).toThrow(
      `Insert failed: WriteError("${coreError}")`,
    );
    await expect(tx.all(app.todos)).rejects.toThrow(
      `Query setup failed: Write error: ${coreError}`,
    );
  });

  it("changes from rolled-back transactions are not visible globally", async () => {
    const tx = db.beginExclusiveTransaction();
    tx.insert(app.todos, { title: "Exclusive transaction", done: false });

    await tx.rollback();

    expect(await db.one(app.todos)).toBeNull();
  });

  it("rejects transaction operations after rollback", async () => {
    const tx = db.beginExclusiveTransaction();
    tx.insert(app.todos, { title: "Rolled-back transaction", done: false });
    const openTransactionId = tx.openTransactionId();

    await tx.rollback();

    const coreError = `open transaction ${openTransactionId} has already been completed or was never opened`;
    expect(() => tx.commit()).toThrow(`Commit transaction failed: Write error: ${coreError}`);
    expect(() => tx.rollback()).toThrow(`Rollback transaction failed: Write error: ${coreError}`);
    expect(() => tx.insert(app.todos, { title: "Nope", done: false })).toThrow(
      `Insert failed: WriteError("${coreError}")`,
    );
    await expect(tx.all(app.todos)).rejects.toThrow(
      `Query setup failed: Write error: ${coreError}`,
    );
  });

  it("supports custom ids and upserts inside transactions", async () => {
    const { value: existingTodo } = db.insert(app.todos, {
      title: "Bob drafted release notes",
      done: false,
    });

    const tx = db.beginExclusiveTransaction();

    const customId = "00000000-0000-0000-0000-000000000123";
    const insertedTodo = tx.insert(
      app.todos,
      { title: "Alice planned the launch", done: false },
      { id: customId },
    );

    const createdByUpsertId = "00000000-0000-0000-0000-000000000124";
    tx.upsert(app.todos, createdByUpsertId, { title: "Bob wrote release notes", done: false });
    tx.upsert(app.todos, existingTodo.id, { title: "Bob drafted release notes", done: true });

    expect(insertedTodo).toEqual({
      id: customId,
      title: "Alice planned the launch",
      done: false,
    });
    expect(await db.all(app.todos)).toEqual([existingTodo]);

    await tx.commit();

    const committedRows = await db.all(app.todos);
    expect(committedRows).toHaveLength(3);
    expect(committedRows).toEqual(
      expect.arrayContaining([
        insertedTodo,
        {
          id: createdByUpsertId,
          title: "Bob wrote release notes",
          done: false,
        },
        {
          id: existingTodo.id,
          title: "Bob drafted release notes",
          done: true,
        },
      ]),
    );
  });

  it("rejects partial upserts for missing rows inside transactions", async () => {
    const tx = db.beginExclusiveTransaction();

    expect(() =>
      tx.upsert(app.todos, "00000000-0000-0000-0000-000000000125", { done: true }),
    ).toThrow("missing required field `title`");
  });

  describe("db.exclusiveTransaction(cb)", () => {
    it("returns the callback value when an async transaction only reads", async () => {
      const { value: existingTodo } = db.insert(app.todos, {
        title: "Alice checked the roadmap",
        done: false,
      });

      const result = await db.exclusiveTransaction(async (tx) => {
        const rows = await tx.all(app.todos);
        expect(rows).toEqual([existingTodo]);
        return "no writes needed";
      });
      expect(result.value).toEqual("no writes needed");
      await expect(result.wait()).resolves.toEqual("no writes needed");
    });

    it("rolls back cleanly when an async transaction reads then throws before writing", async () => {
      const { value: existingTodo } = db.insert(app.todos, {
        title: "Alice checked rollback",
        done: false,
      });
      const error = new Error("no write transaction failed");

      await expect(
        db.exclusiveTransaction(async (tx) => {
          const rows = await tx.all(app.todos);
          expect(rows).toEqual([existingTodo]);
          throw error;
        }),
      ).rejects.toBe(error);

      await expect(db.all(app.todos)).resolves.toEqual([existingTodo]);
    });

    it("commits changes once the callback resolves and the authority accepts the transaction", async () => {
      const txResult = await db.exclusiveTransaction((tx) => {
        return tx.insert(app.todos, { title: "Exclusive transaction", done: false });
      });
      const insertedTodo = txResult.value;

      expect(await db.one(app.todos)).toMatchObject(insertedTodo);
    });

    describe("rolls back changes if the callback rejects", () => {
      it("insert", async () => {
        await expect(() =>
          db.exclusiveTransaction(async (tx) => {
            const todo = tx.insert(app.todos, { title: "Todo", done: false });
            expect(await tx.one(app.todos)).toEqual(todo);
            expect(await db.one(app.todos)).toBeNull();
            throw new Error("callback failed");
          }),
        ).rejects.toThrow("callback failed");

        expect(await db.one(app.todos)).toBeNull();
      });

      it("update", async () => {
        const { value: todo } = db.insert(app.todos, { title: "Todo", done: false });

        await expect(() =>
          db.exclusiveTransaction(async (tx) => {
            tx.update(app.todos, todo.id, { title: "Updated todo" });
            expect((await tx.one(app.todos))?.title).toEqual("Updated todo");
            expect((await db.one(app.todos))?.title).toEqual("Todo");
            throw new Error("callback failed");
          }),
        ).rejects.toThrow("callback failed");

        expect((await db.one(app.todos))?.title).toEqual("Todo");
      });

      it("delete", async () => {
        const { value: todo } = db.insert(app.todos, { title: "Todo", done: false });

        await expect(() =>
          db.exclusiveTransaction(async (tx) => {
            tx.delete(app.todos, todo.id);
            expect(await tx.one(app.todos)).toBeNull();
            expect(await db.one(app.todos)).toEqual(todo);
            throw new Error("callback failed");
          }),
        ).rejects.toThrow("callback failed");

        expect(await db.one(app.todos)).toEqual(todo);
      });
    });
  });

  it("concurrent transactions cannot modify the same data", async () => {
    const { value: base } = db.insert(app.todos, { title: "Shared", done: false });

    const aliceTx = db.beginExclusiveTransaction();
    const bobTx = db.beginExclusiveTransaction();

    aliceTx.update(app.todos, base.id, { title: "Alice's title" });
    bobTx.update(app.todos, base.id, { title: "Bob's title" });

    await (await aliceTx.commit()).wait();
    await expect(async () => bobTx.commit().wait()).rejects.toThrow(
      "(transaction_conflict): row visible parent changed since transaction write was staged",
    );

    expect((await db.one(app.todos))?.title).toEqual("Alice's title");
  });

  it("reads a cold remote row", async () => {
    const { appId, serverUrl, adminSecret } = await getJazzServerInfo(
      uniqueDbName("exclusive-cold-read"),
    );
    await deploy({
      appId,
      serverUrl,
      adminSecret,
      schema: app.wasmSchema,
      permissions: {},
    });

    const writer = await createDb({
      appId,
      serverUrl,
      adminSecret,
      driver: { type: "memory" },
    });
    const reader = await createDb({
      appId,
      serverUrl,
      driver: { type: "memory" },
    });
    try {
      const inserted = writer.insert(app.todos, { title: "remote", done: false });
      await inserted.wait({ tier: "global" });
      await reader.all(app.todos.where({ id: "00000000-0000-4000-8000-000000000000" }), {
        tier: "edge",
      });

      const transaction = reader.beginExclusiveTransaction();
      let transactionRows: Todo[];
      try {
        transactionRows = await transaction.all(app.todos.where({ id: inserted.value.id }), {
          tier: "edge",
        });
      } finally {
        await transaction.rollback();
      }
      const directRows = await reader.all(app.todos.where({ id: inserted.value.id }), {
        tier: "edge",
      });

      expect(transactionRows).toEqual([inserted.value]);
      expect(directRows).toEqual([inserted.value]);
    } finally {
      await Promise.all([writer.shutdown(), reader.shutdown()]);
    }
  }, 60_000);
});

describe("db mergeable transaction reads browser integration", () => {
  it("keeps uncommitted mergeable transaction changes out of global reads", async () => {
    const tx = db.beginTransaction();
    const insertedTodo = tx.insert(app.todos, { title: "Mergeable transaction", done: false });

    expect(await db.one(app.todos)).toBeNull();

    await tx.commit();
    expect(await db.one(app.todos)).toMatchObject(insertedTodo);
  });

  it("rejects mergeable transaction operations after commit", async () => {
    const tx = db.beginTransaction();
    tx.insert(app.todos, { title: "Committed mergeable transaction", done: false });
    const openTransactionId = tx.openTransactionId();

    await tx.commit();

    const coreError = `open transaction ${openTransactionId} is already committed`;
    expect(() => tx.commit()).toThrow(`Write error: ${coreError}`);
    expect(() => tx.rollback()).toThrow(`Write error: ${coreError}`);
    expect(() => tx.insert(app.todos, { title: "Nope", done: false })).toThrow(
      `Insert failed: WriteError("${coreError}")`,
    );
    await expect(tx.all(app.todos)).rejects.toThrow(
      `Query setup failed: Write error: ${coreError}`,
    );
  });

  it("rejects mergeable transaction operations after rollback", async () => {
    const tx = db.beginTransaction();
    tx.insert(app.todos, { title: "Rolled-back mergeable transaction", done: false });
    const openTransactionId = tx.openTransactionId();

    await tx.rollback();

    const coreError = `open transaction ${openTransactionId} has already been completed or was never opened`;
    expect(() => tx.commit()).toThrow(`Commit transaction failed: Write error: ${coreError}`);
    expect(() => tx.rollback()).toThrow(`Rollback transaction failed: Write error: ${coreError}`);
    expect(() => tx.insert(app.todos, { title: "Nope", done: false })).toThrow(
      `Insert failed: WriteError("${coreError}")`,
    );
    await expect(tx.all(app.todos)).rejects.toThrow(
      `Query setup failed: Write error: ${coreError}`,
    );
  });

  it("supports custom ids and upserts inside mergeable transactions", async () => {
    const { value: existingTodo } = db.insert(app.todos, {
      title: "Bob queued docs review",
      done: false,
    });

    const tx = db.beginTransaction();

    const customId = "00000000-0000-0000-0000-000000000223";
    const insertedTodo = tx.insert(
      app.todos,
      { title: "Alice staged screenshots", done: false },
      { id: customId },
    );

    const createdByUpsertId = "00000000-0000-0000-0000-000000000224";
    tx.upsert(app.todos, createdByUpsertId, { title: "Bob checked the docs", done: false });
    tx.upsert(app.todos, existingTodo.id, { title: "Bob queued docs review", done: true });

    expect(insertedTodo).toEqual({
      id: customId,
      title: "Alice staged screenshots",
      done: false,
    });
    expect(await db.all(app.todos)).toEqual([existingTodo]);

    await tx.commit();

    const committedRows = await db.all(app.todos);
    expect(committedRows).toHaveLength(3);
    expect(committedRows).toEqual(
      expect.arrayContaining([
        {
          id: existingTodo.id,
          title: "Bob queued docs review",
          done: true,
        },
        insertedTodo,
        {
          id: createdByUpsertId,
          title: "Bob checked the docs",
          done: false,
        },
      ]),
    );
  });

  it("rejects partial upserts for missing rows inside mergeable transactions", async () => {
    const tx = db.beginTransaction();

    expect(() =>
      tx.upsert(app.todos, "00000000-0000-0000-0000-000000000225", { done: true }),
    ).toThrow("missing required field `title`");
  });

  describe("db.transaction(cb)", () => {
    it("rejects an async mergeable transaction that only reads because it has no commit", async () => {
      const { value: existingTodo } = db.insert(app.todos, {
        title: "Alice reviewed the plan",
        done: false,
      });

      await expect(
        db.transaction(async (tx) => {
          const rows = await tx.all(app.todos);
          expect(rows).toEqual([existingTodo]);
          return "no writes needed";
        }),
      ).rejects.toThrow("empty mergeable transaction has no committed unit; roll it back instead");
    });

    it("rolls back cleanly when an async mergeable transaction reads then throws before writing", async () => {
      const { value: existingTodo } = db.insert(app.todos, {
        title: "Alice reviewed rollback",
        done: false,
      });
      const error = new Error("no write transaction failed");

      await expect(
        db.transaction(async (tx) => {
          const rows = await tx.all(app.todos);
          expect(rows).toEqual([existingTodo]);
          throw error;
        }),
      ).rejects.toBe(error);

      await expect(db.all(app.todos)).resolves.toEqual([existingTodo]);
    });

    it("commits changes once the callback resolves", async () => {
      const txResult = await db.transaction((tx) => {
        return tx.insert(app.todos, { title: "Mergeable transaction", done: false });
      });
      const insertedTodo = txResult.value;

      expect(await db.one(app.todos)).toMatchObject(insertedTodo);
    });

    describe("rolls back changes if the callback rejects", () => {
      it("insert", async () => {
        await expect(() =>
          db.transaction(async (tx) => {
            const todo = tx.insert(app.todos, { title: "Mergeable transaction", done: false });
            expect(await tx.one(app.todos)).toEqual(todo);
            expect(await db.one(app.todos)).toBeNull();
            throw new Error("callback failed");
          }),
        ).rejects.toThrow("callback failed");

        expect(await db.one(app.todos)).toBeNull();
      });

      it("update", async () => {
        const { value: todo } = db.insert(app.todos, { title: "Todo", done: false });

        await expect(() =>
          db.transaction(async (tx) => {
            tx.update(app.todos, todo.id, { title: "Updated todo" });
            expect((await tx.one(app.todos))?.title).toEqual("Updated todo");
            expect((await db.one(app.todos))?.title).toEqual("Todo");
            throw new Error("callback failed");
          }),
        ).rejects.toThrow("callback failed");

        expect((await db.one(app.todos))?.title).toEqual("Todo");
      });

      it("delete", async () => {
        const { value: todo } = db.insert(app.todos, { title: "Todo", done: false });

        await expect(() =>
          db.transaction(async (tx) => {
            tx.delete(app.todos, todo.id);
            expect(await tx.one(app.todos)).toBeNull();
            expect(await db.one(app.todos)).toEqual(todo);
            throw new Error("callback failed");
          }),
        ).rejects.toThrow("callback failed");

        expect(await db.one(app.todos)).toEqual(todo);
      });
    });
  });
});
