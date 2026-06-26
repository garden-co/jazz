import assert from "node:assert/strict";
import {
  createDb,
  createFileFromBlob,
  defineApp,
  deleteFile,
  isDeleted,
  loadFileAsBlob,
  readFileBytes,
  readFiles,
  schema,
  type BinaryLargeValueRow,
  type Table,
} from "./jazz-tools.js";

const accountAuthor = filledBytes(0xa7);
const accountAuthorId = "a7a7a7a7-a7a7-a7a7-a7a7-a7a7a7a7a7a7";
const node = filledBytes(0x57);
const rowId = "71717171-7171-7171-7171-717171717171";
const ownerRowId = "70707070-7070-7070-7070-707070707070";
const transactionRowId = "74747474-7474-7474-7474-747474747474";
const fileId = "74747474-7474-7474-7474-747474747474";
const otherOwnerId = "73737373-7373-7373-7373-737373737373";

type Todo = {
  id: string;
  title: string;
  done: boolean;
  priority: number;
  owner?: string;
  tags: string[];
  payload?: Uint8Array;
};

type OwnedTodo = {
  id: string;
  title: string;
  done: boolean;
  priority: number;
  owner: string;
};

type Team = {
  id: string;
  name: string;
  parent?: string;
};

type User = {
  id: string;
  name: string;
  team?: string;
  ownedTodos?: OwnedTodo[];
};

type PrivateNote = {
  id: string;
  body: string;
  owner: string;
};

type NoteLink = {
  id: string;
  label: string;
  note?: string;
};

type StoredFile = BinaryLargeValueRow & {
  id: string;
  name?: string;
  mime_type?: string;
};

const app = defineApp({
  teams: schema.table({
    name: schema.string(),
    parent: schema.uuid({ nullable: true, references: "teams" }),
  }),
  users: schema.table({
    name: schema.string(),
    team: schema.uuid({ nullable: true, references: "teams" }),
  }, {
    relations: {
      ownedTodos: { table: "owned_todos", column: "owner" },
    },
  }),
  todos: schema.table({
    title: schema.string(),
    done: schema.boolean(),
    priority: schema.integer(),
    owner: schema.uuid({ nullable: true }),
    tags: schema.array("Text"),
    payload: schema.bytea({ nullable: true }),
  }),
  owned_todos: schema.table({
    title: schema.string(),
    done: schema.boolean(),
    priority: schema.integer(),
    owner: schema.uuid({ references: "users" }),
  }, { readPolicy: "owner" }),
  private_notes: schema.table({
    body: schema.string(),
    owner: schema.uuid(),
  }, { readPolicy: "owner" }),
  note_links: schema.table({
    label: schema.string(),
    note: schema.uuid({ nullable: true, references: "private_notes" }),
  }),
  files: schema.binaryLargeValueTable(),
});

async function main(): Promise<void> {
  const db = await createDb({
    schema: app._schema,
    node,
    accountAuthor,
    accountId: 0x7101,
    server: true,
  });
  const todosTable = app.todos as Table<Todo, Omit<Todo, "id">>;
  const ownedTodosTable = app.owned_todos as Table<OwnedTodo, Omit<OwnedTodo, "id">>;
  const teamsTable = app.teams as Table<Team, Omit<Team, "id">>;
  const usersTable = app.users as Table<User, Omit<User, "id" | "ownedTodos">>;
  const privateNotesTable = app.private_notes as Table<PrivateNote, Omit<PrivateNote, "id">>;
  const noteLinksTable = app.note_links as Table<NoteLink, Omit<NoteLink, "id">>;
  const filesTable = app.files as Table<StoredFile, Omit<StoredFile, "id">>;

  const todoSnapshots: Todo[][] = [];
  const todos = db.subscribe(todosTable, (rows) => {
    todoSnapshots.push(rows);
  });
  await waitForSnapshots(todoSnapshots, 1);
  assert.deepEqual(latest(todoSnapshots), []);

  const created = db.insert(todosTable, {
    title: "Adopt alpha public flow",
    done: false,
    priority: 1,
    tags: ["alpha", "compat"],
    payload: new Uint8Array([1, 2, 3]),
  }, { id: rowId });
  assert.equal(created.value, created);
  assert.equal(await created.wait({ tier: "local" }), created);
  assert.equal(created.title, "Adopt alpha public flow");
  assert.equal(created.done, false);
  db.insert(todosTable, {
    id: "72727272-7272-7272-7272-727272727272",
    title: "Ship query parity",
    done: false,
    priority: 2,
    owner: otherOwnerId,
    tags: ["query", "compat"],
    payload: new Uint8Array([4, 5, 6]),
  });
  db.insert(todosTable, {
    id: ownerRowId,
    title: "Prove identity reads",
    done: false,
    priority: 3,
    owner: accountAuthorId,
    tags: ["alpha", "identity"],
    payload: undefined,
  });
  assert.deepEqual(created.tags, ["alpha", "compat"]);
  assert.deepEqual(created.payload, new Uint8Array([1, 2, 3]));
  db.insert(ownedTodosTable, {
    id: "75757575-7575-7575-7575-757575757575",
    title: "Owner A scoped read",
    done: false,
    priority: 1,
    owner: accountAuthorId,
  });
  db.insert(ownedTodosTable, {
    id: "76767676-7676-7676-7676-767676767676",
    title: "Owner B scoped read",
    done: false,
    priority: 1,
    owner: otherOwnerId,
  });
  db.insert(teamsTable, {
    id: "78787878-7878-7878-7878-787878787878",
    name: "Root alpha team",
    parent: undefined,
  });
  db.insert(teamsTable, {
    id: "79797979-7979-7979-7979-797979797979",
    name: "Child alpha team",
    parent: "78787878-7878-7878-7878-787878787878",
  });
  db.insert(usersTable, {
    id: accountAuthorId,
    name: "Account author",
    team: "79797979-7979-7979-7979-797979797979",
  });
  db.insert(usersTable, {
    id: otherOwnerId,
    name: "Other owner",
    team: undefined,
  });
  db.insert(privateNotesTable, {
    id: "7a7a7a7a-7a7a-7a7a-7a7a-7a7a7a7a7a7a",
    body: "Account author private note",
    owner: accountAuthorId,
  });
  db.insert(noteLinksTable, {
    id: "7b7b7b7b-7b7b-7b7b-7b7b-7b7b7b7b7b7b",
    label: "readable note link",
    note: "7a7a7a7a-7a7a-7a7a-7a7a-7a7a7a7a7a7a",
  });
  db.insert(noteLinksTable, {
    id: "7c7c7c7c-7c7c-7c7c-7c7c-7c7c7c7c7c7c",
    label: "missing note link",
    note: "7d7d7d7d-7d7d-7d7d-7d7d-7d7d7d7d7d7d",
  });
  await waitForLatestSummaries(todoSnapshots, [
    "Adopt alpha public flow:open",
    "Prove identity reads:open",
    "Ship query parity:open",
  ]);
  assert.deepEqual(db.all(todosTable).map(summary).sort(), [
    "Adopt alpha public flow:open",
    "Prove identity reads:open",
    "Ship query parity:open",
  ]);
  assert.equal(db.all(todosTable.limit(1)).length, 1);
  assert.deepEqual(db.all(todosTable.select("title")).map((todo) => Object.keys(todo).sort()), [
    ["id", "title"],
    ["id", "title"],
    ["id", "title"],
  ]);
  assert.deepEqual(db.all(todosTable.select("title").orderBy("title")).map((todo) => todo.title), [
    "Adopt alpha public flow",
    "Prove identity reads",
    "Ship query parity",
  ]);
  assert.deepEqual(db.all(todosTable.orderBy("title", "desc")).map((todo) => todo.title), [
    "Ship query parity",
    "Prove identity reads",
    "Adopt alpha public flow",
  ]);
  assert.deepEqual(db.all(todosTable.orderBy("title").offset(1).limit(1)).map(summary), ["Prove identity reads:open"]);
  assert.deepEqual(db.allForIdentity(ownedTodosTable, accountAuthor).map(summary), ["Owner A scoped read:open"]);
  assert.deepEqual(db.allForIdentity(ownedTodosTable, otherOwnerId).map(summary), ["Owner B scoped read:open"]);
  const usersWithOwnedTodos = db.allForIdentity(usersTable.where("id", "eq", accountAuthorId).include("ownedTodos"), accountAuthor);
  assert.equal(usersWithOwnedTodos.length, 1);
  assert.deepEqual(usersWithOwnedTodos[0].ownedTodos?.map(summary), ["Owner A scoped read:open"]);
  const usersWithObjectIncludedTodos = db.allForIdentity(usersTable.where({ id: accountAuthorId }).include({ ownedTodos: true }), accountAuthor);
  assert.equal(usersWithObjectIncludedTodos.length, 1);
  assert.deepEqual(usersWithObjectIncludedTodos[0].ownedTodos?.map(summary), ["Owner A scoped read:open"]);
  assert.throws(
    () => db.subscribe(usersTable.where({ id: accountAuthorId }).include({ ownedTodos: true }), () => undefined),
    /current jazz-tools\/WasmDb subscribe with relation includes only supports simple forward includes/,
  );
  const usersWithOptionalOwnedTodos = db.allForIdentity(usersTable.orderBy("name").include("ownedTodos"), accountAuthor);
  assert.deepEqual(usersWithOptionalOwnedTodos.map((user) => [user.name, user.ownedTodos?.map(summary)]), [
    ["Account author", ["Owner A scoped read:open"]],
    ["Other owner", []],
  ]);
  const usersWithRequiredOwnedTodos = db.allForIdentity(usersTable.orderBy("name").requireIncludes("ownedTodos"), accountAuthor) as Array<User & { ownedTodos: OwnedTodo[] }>;
  assert.deepEqual(usersWithRequiredOwnedTodos.map((user) => [user.name, user.ownedTodos.map(summary)]), [
    ["Account author", ["Owner A scoped read:open"]],
  ]);
  const usersWithOptionalTeams = db.all(usersTable.orderBy("name").include("team")) as unknown as Array<Omit<User, "team"> & { team: Team | null }>;
  assert.deepEqual(usersWithOptionalTeams.map((user) => [user.name, user.team?.name ?? null]), [
    ["Account author", "Child alpha team"],
    ["Other owner", null],
  ]);
  const usersWithRequiredTeams = db.all(usersTable.orderBy("name").requireIncludes("team")) as unknown as Array<Omit<User, "team"> & { team: Team }>;
  assert.deepEqual(usersWithRequiredTeams.map((user) => [user.name, user.team.name]), [
    ["Account author", "Child alpha team"],
  ]);
  const usersWithNestedSelectedTeams = db.all(usersTable.orderBy("name").include({
    team: {
      required: true,
      select: ["name"],
      include: {
        parent: { select: ["name"] },
      },
    },
  })) as unknown as Array<Omit<User, "team"> & { team: Pick<Team, "id" | "name"> & { parent: Pick<Team, "id" | "name"> | null } }>;
  assert.deepEqual(usersWithNestedSelectedTeams.map((user) => [
    user.name,
    Object.keys(user.team).sort(),
    user.team.name,
    user.team.parent?.name ?? null,
    user.team.parent ? Object.keys(user.team.parent).sort() : null,
  ]), [
    ["Account author", ["id", "name", "parent"], "Child alpha team", "Root alpha team", ["id", "name"]],
  ]);
  const optionalNoteLinksForOther = db.allForIdentity(noteLinksTable.orderBy("label").include("note"), otherOwnerId) as unknown as Array<Omit<NoteLink, "note"> & { note: PrivateNote | null }>;
  assert.deepEqual(optionalNoteLinksForOther.map((link) => [link.label, link.note?.body ?? null]), [
    ["missing note link", null],
    ["readable note link", null],
  ]);
  const requiredNoteLinksForOther = db.allForIdentity(noteLinksTable.orderBy("label").requireIncludes("note"), otherOwnerId);
  assert.deepEqual(requiredNoteLinksForOther, []);
  const requiredNoteLinksForAuthor = db.allForIdentity(noteLinksTable.orderBy("label").requireIncludes("note"), accountAuthor) as unknown as Array<Omit<NoteLink, "note"> & { note: PrivateNote }>;
  assert.deepEqual(requiredNoteLinksForAuthor.map((link) => [link.label, link.note.body]), [
    ["readable note link", "Account author private note"],
  ]);
  const authorTeams = db.all(usersTable.where("id", "eq", accountAuthorId).hop("team"));
  assert.deepEqual(authorTeams.map((team) => team.name), ["Child alpha team"]);
  const gatheredTeams = db.all(teamsTable.where("id", "eq", "79797979-7979-7979-7979-797979797979").gather({
    max_depth: 4,
    step_table: "teams",
    step_current_column: "id",
    step_hops: ["parent"],
  }));
  assert.deepEqual(gatheredTeams.map((team) => team.name).sort(), ["Child alpha team", "Root alpha team"]);
  assert.deepEqual(db.all(todosTable.where({ done: false })).map(summary).sort(), [
    "Adopt alpha public flow:open",
    "Prove identity reads:open",
    "Ship query parity:open",
  ]);
  assert.deepEqual(db.all(todosTable.where({ done: undefined }).where({ title: { contains: "query" } })).map(summary), [
    "Ship query parity:open",
  ]);
  assert.deepEqual(db.all(todosTable.where({ done: { eq: false }, title: { contains: "query" } })).map(summary), [
    "Ship query parity:open",
  ]);
  assert.deepEqual(db.all(todosTable.where({ done: { ne: true }, priority: { in: [2] } })).map(summary), [
    "Ship query parity:open",
  ]);
  assert.deepEqual(db.all(todosTable.where({ priority: { gt: 1, lte: 2 } })).map(summary), ["Ship query parity:open"]);
  assert.deepEqual(db.all(todosTable.where({ title: { gte: "Prove identity reads", lt: "T" } })).map(summary).sort(), [
    "Prove identity reads:open",
    "Ship query parity:open",
  ]);
  assert.deepEqual(db.all(todosTable.where({ owner: { isNull: true } })).map(summary), ["Adopt alpha public flow:open"]);
  assert.deepEqual(db.all(todosTable.where({ owner: { isNull: false } })).map(summary).sort(), [
    "Prove identity reads:open",
    "Ship query parity:open",
  ]);
  assert.deepEqual(db.all(todosTable.where({ tags: { contains: "alpha" } })).map(summary).sort(), [
    "Adopt alpha public flow:open",
    "Prove identity reads:open",
  ]);
  assert.equal(db.one(todosTable.where({ title: "Ship query parity" }))?.title, "Ship query parity");
  assert.equal(db.one(todosTable.where({ title: "Not present" })), null);
  assert.deepEqual(db.all(todosTable.where("done", "eq", false)).map(summary).sort(), [
    "Adopt alpha public flow:open",
    "Prove identity reads:open",
    "Ship query parity:open",
  ]);
  assert.deepEqual(db.all(todosTable.where("done", "in", [true])).map(summary), []);
  assert.deepEqual(db.all(todosTable.where("done", "in", [false])).map(summary).sort(), [
    "Adopt alpha public flow:open",
    "Prove identity reads:open",
    "Ship query parity:open",
  ]);
  assert.deepEqual(db.all(todosTable.where("title", "eq", "Ship query parity")).map(summary), ["Ship query parity:open"]);
  assert.deepEqual(
    db.all(todosTable.where("title", "in", ["Adopt alpha public flow", "Not present"])).map(summary),
    ["Adopt alpha public flow:open"],
  );
  assert.deepEqual(db.all(todosTable.where("priority", "eq", 2)).map(summary), ["Ship query parity:open"]);
  assert.deepEqual(db.all(todosTable.where("priority", "in", [1, 99])).map(summary), ["Adopt alpha public flow:open"]);
  assert.deepEqual(db.all(todosTable.where("priority", "gt", 1)).map(summary).sort(), [
    "Prove identity reads:open",
    "Ship query parity:open",
  ]);
  assert.deepEqual(db.all(todosTable.where("priority", "gte", 1)).map(summary).sort(), [
    "Adopt alpha public flow:open",
    "Prove identity reads:open",
    "Ship query parity:open",
  ]);
  assert.deepEqual(db.all(todosTable.where("priority", "lt", 2)).map(summary), ["Adopt alpha public flow:open"]);
  assert.deepEqual(db.all(todosTable.where("priority", "lte", 2)).map(summary).sort(), [
    "Adopt alpha public flow:open",
    "Ship query parity:open",
  ]);
  assert.deepEqual(db.all(todosTable.where("title", "gt", "Prove identity reads")).map(summary), [
    "Ship query parity:open",
  ]);
  assert.deepEqual(db.all(todosTable.where("title", "gte", "Prove identity reads")).map(summary).sort(), [
    "Prove identity reads:open",
    "Ship query parity:open",
  ]);
  assert.deepEqual(db.all(todosTable.where("title", "lt", "Prove identity reads")).map(summary), [
    "Adopt alpha public flow:open",
  ]);
  assert.deepEqual(db.all(todosTable.where("title", "lte", "Prove identity reads")).map(summary).sort(), [
    "Adopt alpha public flow:open",
    "Prove identity reads:open",
  ]);
  assert.deepEqual(db.all(todosTable.where("title", "ne", "Ship query parity")).map(summary).sort(), [
    "Adopt alpha public flow:open",
    "Prove identity reads:open",
  ]);
  assert.deepEqual(db.all(todosTable.where("owner", "isNull")).map(summary), ["Adopt alpha public flow:open"]);
  assert.deepEqual(db.all(todosTable.where("owner", "eq", null)).map(summary), ["Adopt alpha public flow:open"]);
  assert.deepEqual(db.all(todosTable.where("owner", "ne", null)).map(summary).sort(), [
    "Prove identity reads:open",
    "Ship query parity:open",
  ]);
  assert.deepEqual(db.all(todosTable.where("owner", "eq", accountAuthorId)).map(summary), ["Prove identity reads:open"]);
  assert.deepEqual(db.all(todosTable.where("owner", "in", [null, accountAuthorId])).map(summary).sort(), [
    "Adopt alpha public flow:open",
    "Prove identity reads:open",
  ]);
  assert.deepEqual(db.all(todosTable.where("owner", "isNotNull")).map(summary).sort(), [
    "Prove identity reads:open",
    "Ship query parity:open",
  ]);
  assert.deepEqual(db.all(todosTable.where("title", "contains", "alpha").limit(1)).map(summary), [
    "Adopt alpha public flow:open",
  ]);
  assert.deepEqual(
    db.all(todosTable.where("done", "eq", false).where("title", "contains", "query")).map(summary),
    ["Ship query parity:open"],
  );
  assert.deepEqual(db.all(todosTable.where("tags", "contains", "alpha")).map(summary).sort(), [
    "Adopt alpha public flow:open",
    "Prove identity reads:open",
  ]);
  assert.deepEqual(db.all(todosTable.where("tags", "eq", ["alpha", "compat"])).map(summary), [
    "Adopt alpha public flow:open",
  ]);
  assert.deepEqual(db.all(todosTable.where("tags", "in", [["query", "compat"]])).map(summary), ["Ship query parity:open"]);
  assert.deepEqual(db.all(todosTable.where("payload", "eq", new Uint8Array([1, 2, 3]))).map(summary), [
    "Adopt alpha public flow:open",
  ]);
  assert.deepEqual(db.all(todosTable.where("payload", "in", [new Uint8Array([4, 5, 6])])).map(summary), [
    "Ship query parity:open",
  ]);

  const tx = db.beginTransaction();
  const staged = tx.insert(todosTable, {
    title: "Committed transaction row",
    done: false,
    priority: 4,
    tags: ["alpha", "transaction"],
    payload: new Uint8Array([7, 8]),
  }, { id: transactionRowId });
  assert.equal(staged.id, transactionRowId);
  assert.equal(db.all(todosTable).some((todo) => todo.id === transactionRowId), false);

  tx.update(todosTable, ownerRowId, {
    done: true,
    tags: ["alpha", "identity", "transaction"],
  });
  const committed = tx.commit();
  assert.equal(committed.value, undefined);
  assert.notEqual(committed.handle, null);
  assert.equal(await committed.wait({ tier: "local" }), undefined);
  await waitForLatestSummaries(todoSnapshots, [
    "Adopt alpha public flow:open",
    "Committed transaction row:open",
    "Prove identity reads:done",
    "Ship query parity:open",
  ]);
  assert.deepEqual(db.all(todosTable.where("tags", "contains", "transaction")).map(summary).sort(), [
    "Committed transaction row:open",
    "Prove identity reads:done",
  ]);

  const bytes = new TextEncoder().encode("alpha public row-backed binary large value");
  const createdFile = await createFileFromBlob(db, filesTable, {
    fileId,
    name: "public-alpha-note.txt",
    mimeType: "text/plain",
    blob: new Blob([arrayBufferFromBytes(bytes)], { type: "text/plain" }),
  });
  assert.equal(createdFile.name, "public-alpha-note.txt");
  assert.equal(createdFile.mime_type, "text/plain");
  assert.deepEqual(createdFile.data, bytes);
  assert.deepEqual(readFiles(db, filesTable).map((file) => file.name), ["public-alpha-note.txt"]);
  assert.deepEqual(readFileBytes(db, filesTable, fileId), bytes);
  const loadedFile = await loadFileAsBlob(db, filesTable, fileId);
  assert.equal(loadedFile.type, "text/plain");
  assert.deepEqual(new Uint8Array(await loadedFile.arrayBuffer()), bytes);

  const updated = db.update(todosTable, rowId, { done: true, tags: ["alpha", "done"] });
  assert.equal(updated.done, true);
  assert.deepEqual(updated.tags, ["alpha", "done"]);
  await waitForLatestSummaries(todoSnapshots, [
    "Adopt alpha public flow:done",
    "Committed transaction row:open",
    "Prove identity reads:done",
    "Ship query parity:open",
  ]);
  assert.deepEqual(db.all(todosTable.where("done", "eq", true)).map(summary).sort(), [
    "Adopt alpha public flow:done",
    "Prove identity reads:done",
  ]);
  assert.deepEqual(db.all(todosTable.where("done", "eq", false)).map(summary).sort(), [
    "Committed transaction row:open",
    "Ship query parity:open",
  ]);
  assert.deepEqual(db.all(todosTable.where("tags", "contains", "done")).map(summary), ["Adopt alpha public flow:done"]);

  const upsertCreated = db.upsert(todosTable, {
    title: "Upsert creates",
    done: false,
    priority: 7,
    tags: ["alpha", "upsert"],
    payload: undefined,
  }, { id: "75757575-7575-7575-7575-757575757575", updatedAt: "2026-06-24T00:00:00.000Z" });
  assert.equal(upsertCreated.title, "Upsert creates");
  const upsertPatched = db.upsert(todosTable, {
    title: "Upsert patches",
    done: true,
    priority: 8,
    tags: ["alpha", "upsert", "patched"],
    payload: new Uint8Array([7, 4]),
  }, { id: "75757575-7575-7575-7575-757575757575" });
  assert.equal(upsertPatched.id, "75757575-7575-7575-7575-757575757575");
  assert.equal(upsertPatched.done, true);
  assert.equal((await upsertPatched.wait()).title, "Upsert patches");

  assert.equal(await db.delete(todosTable, rowId, { updatedAt: Date.now() }).wait(), undefined);
  assert.equal(db.all(todosTable).some((row) => row.id === rowId), false);
  const deletedRows = db.all(todosTable, { includeDeleted: true });
  const deletedRow = deletedRows.find((row) => row.id === rowId);
  const liveRow = deletedRows.find((row) => row.id === ownerRowId);
  assert.ok(deletedRow);
  assert.ok(liveRow);
  assert.equal(isDeleted(deletedRow), true);
  assert.equal(isDeleted(liveRow), false);
  assert.equal(Object.keys(deletedRow).includes("deleted"), false);
  const restored = db.restore(todosTable, rowId, {
    title: "Restored alpha public flow",
    done: false,
    priority: 6,
    tags: ["alpha", "restore"],
    payload: new Uint8Array([8, 9]),
  });
  assert.equal(restored.id, rowId);
  assert.equal(restored.title, "Restored alpha public flow");
  assert.deepEqual(restored.tags, ["alpha", "restore"]);
  assert.deepEqual(await restored.wait({ tier: "local" }), restored.value);
  assert.deepEqual(db.all(todosTable).filter((row) => row.id === rowId).map(summary), ["Restored alpha public flow:open"]);
  assert.throws(
    () => db.restore(todosTable, rowId, {
      title: "Restore should not overwrite visible rows",
      done: false,
      priority: 6,
      tags: [],
      payload: undefined,
    }),
    /Restore failed: row not deleted/,
  );
  db.delete(todosTable, rowId);
  db.delete(todosTable, "72727272-7272-7272-7272-727272727272");
  db.delete(todosTable, ownerRowId);
  db.delete(todosTable, transactionRowId);
  db.delete(todosTable, "75757575-7575-7575-7575-757575757575");
  db.delete(ownedTodosTable, "75757575-7575-7575-7575-757575757575");
  db.delete(ownedTodosTable, "76767676-7676-7676-7676-767676767676");
  db.delete(noteLinksTable, "7b7b7b7b-7b7b-7b7b-7b7b-7b7b7b7b7b7b");
  db.delete(noteLinksTable, "7c7c7c7c-7c7c-7c7c-7c7c-7c7c7c7c7c7c");
  db.delete(privateNotesTable, "7a7a7a7a-7a7a-7a7a-7a7a-7a7a7a7a7a7a");
  db.delete(usersTable, accountAuthorId);
  db.delete(usersTable, otherOwnerId);
  db.delete(teamsTable, "79797979-7979-7979-7979-797979797979");
  db.delete(teamsTable, "78787878-7878-7878-7878-787878787878");
  deleteFile(db, filesTable, fileId);
  await waitForLatestSummaries(todoSnapshots, []);
  todos.unsubscribe();
  assert.deepEqual(db.all(todosTable), []);
  assert.deepEqual(readFiles(db, filesTable), []);

  console.log("alpha public flow gate: ok");
}

function summary(todo: { title: string; done: boolean }): string {
  return `${todo.title}:${todo.done ? "done" : "open"}`;
}

function latest<Row>(snapshots: Row[][]): Row[] {
  const snapshot = snapshots[snapshots.length - 1];
  assert.ok(snapshot);
  return snapshot;
}

async function waitForSnapshots<Row>(snapshots: Row[][], count: number): Promise<void> {
  for (let attempt = 0; attempt < 20; attempt += 1) {
    if (snapshots.length >= count) return;
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  assert.fail(`expected at least ${count} subscription snapshots, got ${snapshots.length}`);
}

async function waitForLatestSummaries<Row extends { title: string; done: boolean }>(
  snapshots: Row[][],
  expected: string[],
): Promise<void> {
  const sorted = [...expected].sort();
  for (let attempt = 0; attempt < 20; attempt += 1) {
    const snapshot = snapshots[snapshots.length - 1];
    if (snapshot && arraysEqual(snapshot.map(summary).sort(), sorted)) return;
    await new Promise((resolve) => setTimeout(resolve, 0));
  }
  assert.deepEqual(latest(snapshots).map(summary).sort(), sorted);
}

function arraysEqual(left: string[], right: string[]): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}

function filledBytes(byte: number): Uint8Array {
  return new Uint8Array(16).fill(byte);
}

function arrayBufferFromBytes(bytes: Uint8Array): ArrayBuffer {
  const out = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(out).set(bytes);
  return out;
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
