import { createDb } from "../../src/runtime/default-create-db.js";
import type { Db } from "../../src/runtime/db.js";
import { afterEach, assert, beforeEach, describe, expect, it } from "vitest";
import { app } from "./fixtures/basic/schema";
import { insertProject, insertTodo, insertUser, uniqueDbName } from "./factories";

describe("TS Upsert API", () => {
  let db: Db;

  beforeEach(async () => {
    db = await createDb({
      appId: "test-app",
      driver: { type: "persistent", dbName: uniqueDbName("upsert-api") },
    });
  });

  afterEach(async () => {
    await db.shutdown();
  });

  it("creates a row with a caller-supplied id", async () => {
    const id = "00000000-0000-0000-0000-000000000000";
    db.upsert(app.projects, id, { name: "Test Project" });

    const project = await db.one(app.projects.where({ id: { eq: id } }));
    expect(project).toEqual({
      id,
      name: "Test Project",
    });
  });

  it("can wait for upserts to be persisted up to a specific durability tier", async () => {
    const id = "00000000-0000-0000-0000-000000000000";
    const result = db.upsert(app.projects, id, { name: "Test Project" });
    expect(result).toMatchObject({ value: undefined, wait: expect.any(Function) });
    await result.wait({ tier: "local" });

    const project = await db.one(app.projects.where({ id: { eq: id } }), { tier: "local" });
    expect(project).toEqual({
      id,
      name: "Test Project",
    });
  });

  it("updates an existing row with the same id", async () => {
    const project = insertProject(db, "Test Project");

    db.upsert(app.projects, project.id, { name: "Updated Project" });

    const updatedProject = await db.one(app.projects.where({ id: { eq: project.id } }));
    expect(updatedProject?.name).toBe("Updated Project");
  });

  it("upserts don't modify the original row", async () => {
    const project = insertProject(db, "Test Project");

    db.upsert(app.projects, project.id, { name: "Updated Project" });

    expect(project.name).toBe("Test Project");
  });

  it("fields that are not present in an existing-row upsert are not modified", async () => {
    const owner = insertUser(db);
    const assignee = insertUser(db, "Assignee");
    const todo = insertTodo(db, {
      title: "Test Todo",
      done: false,
      tags: ["tag1", "tag2"],
      ownerId: owner.id,
      assigneesIds: [assignee.id],
    });

    db.upsert(app.todos, todo.id, {
      title: todo.title,
      done: true,
      projectId: todo.projectId,
    });

    const updatedTodo = await db.one(app.todos.where({ id: { eq: todo.id } }));
    expect(updatedTodo).toEqual({
      ...todo,
      done: true,
    });
  });

  it("fields that are explicitly set to undefined are not modified", async () => {
    const owner = insertUser(db);
    const todo = insertTodo(db, { ownerId: owner.id });

    db.upsert(app.todos, todo.id, {
      title: todo.title,
      projectId: todo.projectId,
      ownerId: undefined,
    });

    const updatedTodo = await db.one(app.todos.where({ id: { eq: todo.id } }));
    assert(updatedTodo);
    expect(updatedTodo.ownerId).toBe(owner.id);
  });

  it("nullable fields can be unset by setting them to null", async () => {
    const owner = insertUser(db);
    const todo = insertTodo(db, { ownerId: owner.id });

    db.upsert(app.todos, todo.id, {
      title: todo.title,
      projectId: todo.projectId,
      ownerId: null,
    });

    const updatedTodo = await db.one(app.todos.where({ id: { eq: todo.id } }));
    assert(updatedTodo);
    expect(updatedTodo.ownerId).toBeNull();
  });

  it("required fields cannot be unset", async () => {
    const todo = insertTodo(db, { title: "Test Todo" });

    expect(() =>
      // @ts-expect-error - null is not a valid value for a required field
      db.upsert(app.todos, todo.id, { title: null, projectId: todo.projectId }),
    ).toThrow("Cannot set required field 'title' to null");
  });

  it("fails when trying to insert a row with missing required fields", async () => {
    const id = "00000000-0000-0000-0000-000000000000";
    expect(() => db.upsert(app.todos, id, { done: true })).toThrow(
      'Upsert failed: WriteError("encoding error: missing required field `title` on table `todos`")',
    );
  });

  it("uses default values missing from upsert data when creating a row", async () => {
    const id = "00000000-0000-0000-0000-000000000000";
    const project = insertProject(db);
    const owner = insertUser(db);

    db.upsert(app.todos, id, {
      title: "Test Todo",
      projectId: project.id,
      ownerId: owner.id,
    });

    const todo = await db.one(app.todos.where({ id: { eq: id } }));
    expect(todo).toEqual({
      id,
      title: "Test Todo",
      projectId: project.id,
      ownerId: owner.id,
      done: false,
      tags: [],
      assigneesIds: [],
    });
  });

  it("reports deleted-row reservation through the write handle", async () => {
    const project = insertProject(db);
    db.delete(app.projects, project.id);

    await expect(
      db.upsert(app.projects, project.id, { name: "Restored Project" }).wait({ tier: "local" }),
    ).rejects.toMatchObject({
      name: "PersistedWriteRejectedError",
      code: "write_rejected",
      reason: `row already deleted: ${project.id}`,
    });
  });

  it("can use caller-supplied updatedAt on new-row upsert", async () => {
    const id = "00000000-0000-0000-0000-000000000000";
    const updatedAt = 1_704_067_200_123;
    db.upsert(app.projects, id, { name: "Backfilled Project" }, { updatedAt });

    const project = await db.one(app.projects.select("name", "$updatedAt").where({ id }));

    expect(project).toEqual({
      id: project?.id,
      name: "Backfilled Project",
      $updatedAt: new Date(updatedAt),
    });
  });

  it("can use caller-supplied updatedAt on existing-row upsert", async () => {
    const updatedAt = 1_704_067_200_123;
    const originalProject = insertProject(db, "Test Project");

    db.upsert(app.projects, originalProject.id, { name: "Backfilled Project" }, { updatedAt });

    const project = await db.one(
      app.projects.select("name", "$updatedAt").where({ id: { eq: originalProject.id } }),
    );

    expect(project).toEqual({
      id: originalProject.id,
      name: "Backfilled Project",
      $updatedAt: new Date(updatedAt),
    });
  });
});
