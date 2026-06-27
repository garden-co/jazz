import assert from "node:assert/strict";
import test from "node:test";
import { createJazzContext } from "./backend.js";
import { defineApp, schema } from "./jazz-tools.js";

type Todo = {
  id: string;
  title: string;
  done: boolean;
};

test("createJazzContext exposes db and asBackend over the same memory DB", async () => {
  const app = defineApp({
    todos: schema.table({
      title: schema.text(),
      done: schema.boolean(),
    }),
  });

  const context = await createJazzContext({
    appId: "backend-test",
    app,
    driver: "memory",
    nextRowId: 0x1200,
  });

  try {
    const todos = context.db().table<Todo, Omit<Todo, "id">>("todos");
    const created = context.db().insert(todos, { title: "Ship backend slice", done: false });

    assert.equal(created.title, "Ship backend slice");
    assert.deepEqual(
      context
        .db()
        .all(todos)
        .map((todo) => todo.title),
      ["Ship backend slice"],
    );

    const backendRows = context.asBackend().db.all(todos);
    assert.deepEqual(
      backendRows.map((todo) => todo.done),
      [false],
    );
  } finally {
    await context.shutdown();
  }
});

test("createJazzContext rejects persistent drivers until core WasmDb persistence is exposed", async () => {
  await assert.rejects(
    createJazzContext({
      appId: "backend-persistent-test",
      schema: {},
      driver: "persistent",
    }),
    /persistent backend storage is not exposed honestly yet/,
  );
});

test("createJazzContext shutdown is idempotent and closes the core facade", async () => {
  CloseTrackingWasmDb.openCount = 0;

  const context = await createJazzContext({
    appId: "backend-close-test",
    schema: {},
    driver: "memory",
    Runtime: CloseTrackingWasmDb,
  });

  await context.shutdown();
  await context.shutdown();

  assert.equal(CloseTrackingWasmDb.openCount, 1);
  assert.throws(() => context.db().table("anything"), /db is closed/);
});

class CloseTrackingWasmDb {
  static openCount = 0;

  static openMemory(): CloseTrackingWasmDb {
    CloseTrackingWasmDb.openCount += 1;
    return new CloseTrackingWasmDb();
  }
}
