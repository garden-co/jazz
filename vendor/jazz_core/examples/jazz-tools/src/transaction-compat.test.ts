import assert from "node:assert/strict";
import test from "node:test";
import { createDb, schema as s } from "./jazz-tools.js";

type Note = {
  id: string;
  title: string;
  body: string;
  done: boolean;
};

const app = s.defineApp({
  notes: s.table({
    title: s.string(),
    body: s.string(),
    done: s.boolean(),
  }),
});

const ids = {
  seed: "10000000-0000-0000-0000-000000000001",
  staged: "10000000-0000-0000-0000-000000000002",
  deleted: "10000000-0000-0000-0000-000000000003",
  restored: "10000000-0000-0000-0000-000000000004",
  rolledBack: "10000000-0000-0000-0000-000000000005",
  upsertCreated: "10000000-0000-0000-0000-000000000006",
  exclusiveStaged: "10000000-0000-0000-0000-000000000007",
  insertThenUpdate: "10000000-0000-0000-0000-000000000008",
  updateThenDelete: "10000000-0000-0000-0000-000000000009",
  restoreThenUpdate: "10000000-0000-0000-0000-00000000000a",
  rollbackCoalesced: "10000000-0000-0000-0000-00000000000b",
  rollbackCoalescedRestore: "10000000-0000-0000-0000-00000000000c",
  upsertSequenceCreated: "10000000-0000-0000-0000-00000000000d",
  upsertSequenceExisting: "10000000-0000-0000-0000-00000000000e",
  callbackRollback: "10000000-0000-0000-0000-00000000000f",
} as const;

test("public transactions stage writes and expose commit/rollback", async () => {
  const db = await createDb({ schema: app._schema, appId: "transaction-compat" });
  const notes = db.table<Note, Omit<Note, "id">>("notes");

  db.insert(notes, { id: ids.seed, title: "Seed", body: "before", done: false });
  db.insert(notes, { id: ids.deleted, title: "Delete me", body: "visible", done: false });
  db.insert(notes, { id: ids.restored, title: "Restore me", body: "old", done: false });
  db.delete(notes, ids.restored);
  const findNote = (id: string) => db.all(notes).find((note) => note.id === id) ?? null;

  const result = db.transaction((tx) => {
    const inserted = tx.insert(
      notes,
      { title: "Inserted", body: "inside tx", done: false },
      { id: ids.staged },
    );
    assert.equal(inserted.id, ids.staged);
    assert.equal(findNote(ids.staged), null, "staged rows are not visible through Db reads yet");

    tx.update(notes, ids.seed, { body: "after", done: true });
    tx.delete(notes, ids.deleted);
    tx.restore(notes, ids.restored, { title: "Restored", body: "back", done: true });
    tx.upsert(
      notes,
      { title: "Upsert created", body: "new", done: false },
      { id: ids.upsertCreated },
    );
    return "committed";
  });

  assert.equal(result, "committed");
  assert.deepEqual(
    db
      .all(notes)
      .map((note) => note.id)
      .sort(),
    [ids.restored, ids.seed, ids.staged, ids.upsertCreated].sort(),
  );
  assert.equal(findNote(ids.seed)?.body, "after");
  assert.equal(findNote(ids.deleted), null);
  assert.equal(findNote(ids.restored)?.title, "Restored");

  const manual = db.beginTransaction();
  manual.insert(notes, { id: ids.rolledBack, title: "Rollback", body: "nope", done: false });
  manual.rollback();
  assert.equal(findNote(ids.rolledBack), null);

  const committed = db.beginTransaction();
  committed.upsert(
    notes,
    { id: ids.staged, title: "Inserted", body: "upsert updated", done: true },
    { id: ids.staged },
  );
  const write = committed.commit();
  assert.equal(write.value, undefined);
  assert.notEqual(write.handle, null);
  assert.equal(await write.wait({ tier: "local" }), undefined);
  assert.equal(findNote(ids.staged)?.done, true);
  assert.equal(findNote(ids.staged)?.body, "upsert updated");
});

test("exclusive transactions are explicitly unsupported by the core facade", async () => {
  const db = await createDb({
    schema: app._schema,
    appId: "transaction-compat-exclusive-unsupported",
  });
  const notes = db.table<Note, Omit<Note, "id">>("notes");

  assert.throws(
    () => db.beginTransaction({ kind: "exclusive" }),
    /exclusive transactions are not supported/,
  );
  assert.equal(db.all(notes).length, 0);
});

test("mergeable transaction reads are unsupported for staged updates", async () => {
  const db = await createDb({
    schema: app._schema,
    appId: "transaction-compat-mergeable-update-read-error",
  });
  const notes = db.table<Note, Omit<Note, "id">>("notes");

  db.insert(notes, { id: ids.seed, title: "Seed", body: "before", done: false });
  const tx = db.beginTransaction();
  tx.update(notes, ids.seed, { body: "after", done: true });

  assert.throws(() => tx.one(notes), /mergeable transaction reads are not supported/);
  assert.equal(db.one(notes)?.body, "before");

  tx.rollback();
});

test("transactions commit insert then update on the same row as one materialized row", async () => {
  const db = await createDb({
    schema: app._schema,
    appId: "transaction-compat-insert-update-same-row",
  });
  const notes = db.table<Note, Omit<Note, "id">>("notes");

  const tx = db.beginTransaction();
  tx.insert(notes, { id: ids.insertThenUpdate, title: "Draft", body: "before", done: false });
  tx.update(notes, ids.insertThenUpdate, { body: "after", done: true });

  await tx.commit().wait({ tier: "local" });
  assert.deepEqual(db.one(notes), {
    id: ids.insertThenUpdate,
    title: "Draft",
    body: "after",
    done: true,
  });
});

test("transactions commit update then delete on the same row as deletion", async () => {
  const db = await createDb({
    schema: app._schema,
    appId: "transaction-compat-update-delete-same-row",
  });
  const notes = db.table<Note, Omit<Note, "id">>("notes");

  db.insert(notes, { id: ids.updateThenDelete, title: "Seed", body: "before", done: false });
  const tx = db.beginTransaction();
  tx.update(notes, ids.updateThenDelete, { body: "after", done: true });
  tx.delete(notes, ids.updateThenDelete);

  await tx.commit().wait({ tier: "local" });
  assert.equal(db.one(notes), null);
});

test("mergeable transactions commit restore then update on the same row as restored update", async () => {
  const db = await createDb({
    schema: app._schema,
    appId: "transaction-compat-restore-update-same-row",
  });
  const notes = db.table<Note, Omit<Note, "id">>("notes");

  db.insert(notes, { id: ids.restoreThenUpdate, title: "Seed", body: "deleted", done: false });
  db.delete(notes, ids.restoreThenUpdate);

  const tx = db.beginTransaction();
  tx.restore(notes, ids.restoreThenUpdate, { title: "Restored", body: "before", done: false });
  tx.update(notes, ids.restoreThenUpdate, { body: "after", done: true });

  await tx.commit().wait({ tier: "local" });
  assert.deepEqual(db.one(notes), {
    id: ids.restoreThenUpdate,
    title: "Restored",
    body: "after",
    done: true,
  });
});

test("rollback drops coalesced staged same-row changes", async () => {
  const db = await createDb({
    schema: app._schema,
    appId: "transaction-compat-rollback-coalesced-same-row",
  });
  const notes = db.table<Note, Omit<Note, "id">>("notes");

  db.insert(notes, { id: ids.rollbackCoalesced, title: "Seed", body: "before", done: false });
  db.insert(notes, {
    id: ids.rollbackCoalescedRestore,
    title: "Deleted",
    body: "before",
    done: false,
  });
  db.delete(notes, ids.rollbackCoalescedRestore);

  const tx = db.beginTransaction();
  tx.update(notes, ids.rollbackCoalesced, { body: "updated" });
  tx.delete(notes, ids.rollbackCoalesced);
  tx.restore(notes, ids.rollbackCoalescedRestore, {
    title: "Restored",
    body: "restored",
    done: false,
  });
  tx.update(notes, ids.rollbackCoalescedRestore, { done: true });

  tx.rollback();

  assert.deepEqual(db.all(notes), [
    {
      id: ids.rollbackCoalesced,
      title: "Seed",
      body: "before",
      done: false,
    },
  ]);
});

test("transactions coalesce repeated same-row upserts", async () => {
  const db = await createDb({
    schema: app._schema,
    appId: "transaction-compat-upsert-same-row-sequences",
  });
  const notes = db.table<Note, Omit<Note, "id">>("notes");

  db.insert(notes, {
    id: ids.upsertSequenceExisting,
    title: "Existing",
    body: "before",
    done: false,
  });
  const tx = db.beginTransaction();

  tx.upsert(
    notes,
    { id: ids.upsertSequenceCreated, title: "Created", body: "first", done: false },
    { id: ids.upsertSequenceCreated },
  );
  tx.upsert(
    notes,
    { id: ids.upsertSequenceCreated, title: "Created", body: "second", done: true },
    { id: ids.upsertSequenceCreated },
  );
  tx.upsert(
    notes,
    { id: ids.upsertSequenceExisting, title: "Existing", body: "patched", done: false },
    { id: ids.upsertSequenceExisting },
  );
  tx.upsert(
    notes,
    { id: ids.upsertSequenceExisting, title: "Existing", body: "patched again", done: true },
    { id: ids.upsertSequenceExisting },
  );

  await tx.commit().wait({ tier: "local" });
  assert.deepEqual(
    db.all(notes).sort((left, right) => left.id.localeCompare(right.id)),
    [
      {
        id: ids.upsertSequenceCreated,
        title: "Created",
        body: "second",
        done: true,
      },
      {
        id: ids.upsertSequenceExisting,
        title: "Existing",
        body: "patched again",
        done: true,
      },
    ],
  );
});

test("transaction callback commits returned values and rolls back thrown callbacks", async () => {
  const db = await createDb({
    schema: app._schema,
    appId: "transaction-compat-callback-commit-rollback",
  });
  const notes = db.table<Note, Omit<Note, "id">>("notes");

  const result = db.transaction((tx) => {
    tx.insert(notes, { id: ids.callbackRollback, title: "Committed", body: "yes", done: false });
    return { committed: true };
  });
  assert.deepEqual(result, { committed: true });
  assert.equal(db.one(notes)?.title, "Committed");

  assert.throws(
    () =>
      db.transaction((tx) => {
        tx.update(notes, ids.callbackRollback, { body: "rolled back", done: true });
        throw new Error("stop");
      }),
    /stop/,
  );

  assert.deepEqual(db.one(notes), {
    id: ids.callbackRollback,
    title: "Committed",
    body: "yes",
    done: false,
  });
});

test("async transaction callbacks commit after resolution and roll back rejection", async () => {
  const db = await createDb({ schema: app._schema, appId: "transaction-compat-async-callback" });
  const notes = db.table<Note, Omit<Note, "id">>("notes");

  const result = await db.transaction(async (tx) => {
    tx.insert(notes, {
      id: ids.callbackRollback,
      title: "Async committed",
      body: "yes",
      done: false,
    });
    await Promise.resolve();
    return { committed: true };
  });
  assert.deepEqual(result, { committed: true });
  assert.equal(db.one(notes)?.title, "Async committed");

  await assert.rejects(
    () =>
      db.transaction(async (tx) => {
        tx.update(notes, ids.callbackRollback, { body: "rolled back", done: true });
        await Promise.resolve();
        throw new Error("async stop");
      }),
    /async stop/,
  );

  assert.deepEqual(db.one(notes), {
    id: ids.callbackRollback,
    title: "Async committed",
    body: "yes",
    done: false,
  });
});

test("mergeable transaction reads are unsupported while default remains mergeable", async () => {
  const db = await createDb({
    schema: app._schema,
    appId: "transaction-compat-mergeable-read-error",
  });
  const notes = db.table<Note, Omit<Note, "id">>("notes");
  const tx = db.beginTransaction();

  assert.throws(
    () => tx.all(notes),
    /mergeable transaction reads are not supported|UnsupportedFeature/,
  );
});
