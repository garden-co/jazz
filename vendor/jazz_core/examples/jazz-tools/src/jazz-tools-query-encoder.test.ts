import assert from "node:assert/strict";
import test from "node:test";
import {
  assertSubscribeQuerySupportedForTest,
  createDb,
  defineApp,
  encodeBuiltQueryForTest,
  schema,
} from "./jazz-tools.js";
import { queryFromTable } from "./direct-codec.js";

const app = defineApp({
  users: schema.table(
    {
      name: schema.text(),
    },
    { relations: { ownedTodos: { table: "todos", column: "owner_id" } } },
  ),
  todos: schema.table({
    title: schema.text(),
    owner_id: schema.uuid({ references: "users" }),
  }),
});

test("encodes simple forward includes into Rust query bytes", () => {
  const query = app.todos.include("owner");

  assert.deepEqual(
    [...encodeBuiltQueryForTest(query._build(), app._schema)],
    [
      5, 116, 111, 100, 111, 115, 0, 0, 0, 1, 8, 111, 119, 110, 101, 114, 95, 105, 100, 0, 0, 0, 0,
      0, 0, 0,
    ],
  );
});

test("keeps table-root fallback for plain table-shaped builders", () => {
  const query = app.todos.where({});

  assert.deepEqual(encodeBuiltQueryForTest(query._build(), app._schema), queryFromTable("todos"));
});

test("subscribe guard accepts simple forward include builders", () => {
  const query = app.todos.include("owner");

  assert.doesNotThrow(() => assertSubscribeQuerySupportedForTest(query));
});

test("query encoder accepts reverse includes supported by alpha include expansion", () => {
  const query = app.users.include("ownedTodos");

  assert.doesNotThrow(() => encodeBuiltQueryForTest(query._build(), app._schema));
});

test("query encoder accepts selected include projections supported by alpha include expansion", () => {
  const query = app.todos.include({ owner: { select: ["name"] } });

  assert.doesNotThrow(() => encodeBuiltQueryForTest(query._build(), app._schema));
});

test("subscribe guard accepts reverse includes supported by alpha include expansion", () => {
  const query = app.users.include("ownedTodos");

  assert.doesNotThrow(() => assertSubscribeQuerySupportedForTest(query));
});

test("subscribe guard accepts selected include projections supported by alpha include expansion", () => {
  const query = app.todos.include({ owner: { select: ["name"] } });

  assert.doesNotThrow(() => assertSubscribeQuerySupportedForTest(query));
});

test("subscribe simple forward include callbacks include rows", async () => {
  const db = await createDb({ schema: app._schema, appId: "subscribe-forward-include" });
  const owner = db.insert(app.users, { name: "Ada" });
  db.insert(app.todos, { title: "Ship relation watches", owner_id: owner.id });

  let rows: Array<Record<string, unknown>> = [];
  const subscription = db.subscribe(app.todos.include("owner"), (nextRows) => {
    rows = nextRows as Array<Record<string, unknown>>;
  });
  try {
    assert.equal(rows.length, 1);
    assert.equal(rows[0].title, "Ship relation watches");
    assert.deepEqual(rows[0].owner, { id: owner.id, name: "Ada" });
  } finally {
    subscription.unsubscribe();
    await (db as { close?: () => Promise<void> }).close?.();
  }
});

test("subscribe simple forward include callbacks rows inserted after subscribe", async () => {
  const db = await createDb({ schema: app._schema, appId: "subscribe-forward-include-live" });
  let rows: Array<Record<string, unknown>> = [];
  const subscription = db.subscribe(app.todos.include("owner"), (nextRows) => {
    rows = nextRows as Array<Record<string, unknown>>;
  });
  try {
    const first = db.insert(app.users, { name: "Ada" });
    const second = db.insert(app.users, { name: "Grace" });
    db.insert(app.todos, { title: "First live relation", owner_id: first.id });
    db.insert(app.todos, { title: "Second live relation", owner_id: second.id });

    rows.sort((left, right) => String(left.title).localeCompare(String(right.title)));
    assert.deepEqual(
      rows.map((row) => [row.title, (row.owner as { name?: string } | null)?.name]),
      [
        ["First live relation", "Ada"],
        ["Second live relation", "Grace"],
      ],
    );
  } finally {
    subscription.unsubscribe();
    await (db as { close?: () => Promise<void> }).close?.();
  }
});
