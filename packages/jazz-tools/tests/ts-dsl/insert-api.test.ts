import { createDb, type Db } from "../../src/runtime/db.js";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { app } from "./fixtures/basic/app";

function uniqueDbName(label: string): string {
  return `test-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function insertOwner(db: Db, name = "Test User") {
  return db.insert(app.users, { name, friends: [] });
}

describe("TS Write API", () => {
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

  it("returns the inserted row", async () => {
    const project = db.insert(app.projects, { name: "Test Project" });
    const owner = insertOwner(db);

    expect(project).toEqual({
      id: expect.any(String),
      name: "Test Project",
    });

    const todo = db.insert(app.todos, {
      title: "Test Todo",
      done: true,
      tags: ["tag1", "tag2"],
      projectId: project.id,
      ownerId: owner.id,
      assigneesIds: [],
    });

    expect(todo).toEqual({
      id: expect.any(String),
      title: "Test Todo",
      done: true,
      tags: ["tag1", "tag2"],
      projectId: project.id,
      ownerId: owner.id,
      assigneesIds: [],
    });
  });

  it("can wait for row to be persisted up to a specific durability tier", async () => {
    const project = await db.insertDurable(
      app.projects,
      { name: "Test Project" },
      { tier: "worker" },
    );

    expect(project).toEqual({
      id: expect.any(String),
      name: "Test Project",
    });
    const owner = insertOwner(db);

    const todo = await db.insertDurable(
      app.todos,
      {
        title: "Test Todo",
        done: true,
        tags: ["tag1", "tag2"],
        projectId: project.id,
        ownerId: owner.id,
        assigneesIds: [],
      },
      { tier: "worker" },
    );

    expect(todo).toEqual({
      id: expect.any(String),
      title: "Test Todo",
      done: true,
      tags: ["tag1", "tag2"],
      projectId: project.id,
      ownerId: owner.id,
      assigneesIds: [],
    });
  });

  it("updates rows synchronously without returning a promise", async () => {
    const project = db.insert(app.projects, { name: "Test Project" });
    const owner = insertOwner(db);
    const todo = db.insert(app.todos, {
      title: "Test Todo",
      done: false,
      tags: ["tag1", "tag2"],
      projectId: project.id,
      ownerId: owner.id,
      assigneesIds: [],
    });

    const result = db.update(app.todos, todo.id, { done: true });
    expect(result).toBeUndefined();

    const [updated] = await db.all(app.todos.where({ id: { eq: todo.id } }));
    expect(updated.done).toBe(true);
  });

  it("can wait for updates to be persisted up to a specific durability tier", async () => {
    const project = db.insert(app.projects, { name: "Test Project" });
    const owner = insertOwner(db);
    const todo = db.insert(app.todos, {
      title: "Test Todo",
      done: false,
      tags: ["tag1", "tag2"],
      projectId: project.id,
      ownerId: owner.id,
      assigneesIds: [],
    });

    const pending = db.updateDurable(app.todos, todo.id, { done: true }, { tier: "worker" });
    expect(pending).toBeInstanceOf(Promise);

    await pending;

    const [updated] = await db.all(app.todos.where({ id: { eq: todo.id } }), { tier: "worker" });
    expect(updated.done).toBe(true);
  });

  it("deletes rows synchronously without returning a promise", async () => {
    const project = db.insert(app.projects, { name: "Test Project" });
    const owner = insertOwner(db);
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
    const owner = insertOwner(db);
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
