import { createDb, type Db } from "../../src/runtime/db.js";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { app } from "./fixtures/basic/schema";
import { insertProject, insertUser, uniqueDbName } from "./factories";

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

  it("deletes rows synchronously and returns a write handle", async () => {
    const { value: project } = db.insert(app.projects, { name: "Test Project" });
    const owner = insertUser(db);
    const { value: todo } = db.insert(app.todos, {
      title: "Test Todo",
      done: false,
      tags: ["tag1", "tag2"],
      projectId: project.id,
      ownerId: owner.id,
      assigneesIds: [],
    });

    const result = db.delete(app.todos, todo.id);
    expect(result).toMatchObject({
      wait: expect.any(Function),
    });

    const rows = await db.all(app.todos.where({ id: { eq: todo.id } }));
    expect(rows).toEqual([]);
  });

  it("can wait for deletes to be persisted up to a specific durability tier", async () => {
    const project = await db.insert(app.projects, { name: "Test Project" }).wait({ tier: "local" });

    const owner = insertUser(db);
    const todo = await db
      .insert(app.todos, {
        title: "Test Todo",
        done: false,
        tags: ["tag1", "tag2"],
        projectId: project.id,
        ownerId: owner.id,
        assigneesIds: [],
      })
      .wait({ tier: "local" });

    const pending = db.delete(app.todos, todo.id);
    await pending.wait({ tier: "local" });

    const rows = await db.all(app.todos.where({ id: { eq: todo.id } }), { tier: "local" });
    expect(rows).toEqual([]);
  });

  it("can use caller-supplied updatedAt on delete", async () => {
    const updatedAt = 1_704_067_200_123_000;
    const project = insertProject(db);

    db.delete(app.projects, project.id, { updatedAt });

    const deleted = await db.one(
      app.projects
        .select("name", "$updatedAt")
        .includeDeleted()
        .where({ id: { eq: project.id } }),
    );
    expect(deleted).toEqual({
      id: project.id,
      name: project.name,
      $updatedAt: new Date(Math.trunc(updatedAt / 1_000)),
    });
  });

  it("trying to delete an already-deleted row fails", async () => {
    const project = insertProject(db);
    db.delete(app.projects, project.id);

    expect(() => db.delete(app.projects, project.id)).toThrow(
      `Delete failed: WriteError("row already deleted: ${project.id}")`,
    );
  });
});
