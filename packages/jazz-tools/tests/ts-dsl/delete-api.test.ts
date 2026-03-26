import { createDb, type Db } from "../../src/runtime/db.js";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { app } from "./fixtures/basic/app";
import { insertUser, uniqueDbName } from "./factories";

describe("TS Delete API", () => {
  let db: Db;

  beforeEach(async () => {
    db = await createDb({
      appId: "test-app",
      driver: { type: "persistent", dbName: uniqueDbName("insert-row-shape") },
    });
  });

  afterEach(async () => {
    await db.shutdown();
  });

  it("deletes rows synchronously without returning a promise", async () => {
    const project = db.insert(app.projects, { name: "Test Project" });
    const owner = insertUser(db);
    const todo = db.insert(app.todos, {
      title: "Test Todo",
      done: false,
      tags: ["tag1", "tag2"],
      projectId: project.id,
      ownerId: owner.id,
      assigneesIds: [],
    });

    const result = db.delete(app.todos, todo.id);
    expect(result).toBeUndefined();

    const rows = await db.all(app.todos.where({ id: { eq: todo.id } }));
    expect(rows).toEqual([]);
  });

  it("can wait for deletes to be persisted up to a specific durability tier", async () => {
    const project = await db.insertDurable(
      app.projects,
      { name: "Test Project" },
      { tier: "worker" },
    );
    const owner = insertUser(db);
    const todo = await db.insertDurable(
      app.todos,
      {
        title: "Test Todo",
        done: false,
        tags: ["tag1", "tag2"],
        projectId: project.id,
        ownerId: owner.id,
        assigneesIds: [],
      },
      { tier: "worker" },
    );

    const pending = db.deleteDurable(app.todos, todo.id, { tier: "worker" });
    expect(pending).toBeInstanceOf(Promise);

    await pending;

    const rows = await db.all(app.todos.where({ id: { eq: todo.id } }), { tier: "worker" });
    expect(rows).toEqual([]);
  });
});
