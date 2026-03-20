import { createDb, type Db } from "../../src/runtime/db.js";
import { afterEach, assert, beforeEach, describe, expect, it } from "vitest";
import { app } from "./fixtures/basic/app";
import { insertProject, insertTodo, insertUser } from "./factories";

describe("TS Update API", () => {
  let db: Db;

  beforeEach(async () => {
    db = await createDb({
      appId: "test-app",
      driver: { type: "persistent" },
    });
  });

  afterEach(async () => {
    await db.shutdown();
  });

  it("can update a row", async () => {
    const project = insertProject(db, "Test Project");

    db.update(app.projects, project.id, { name: "Updated Project" });

    const updatedProject = await db.one(app.projects.where({ id: { eq: project.id } }));

    assert(updatedProject);
    expect(updatedProject.name).toBe("Updated Project");
  });

  it("updates don't modify the original row", async () => {
    const project = insertProject(db, "Test Project");

    db.update(app.projects, project.id, { name: "Updated Project" });

    expect(project.name).toBe("Test Project");
  });

  it("fields that are not present in the update are not modified", async () => {
    const project = insertProject(db, "Test Project");

    db.update(app.projects, project.id, {});

    const updatedProject = await db.one(app.projects.where({ id: { eq: project.id } }));
    assert(updatedProject);
    expect(updatedProject.name).toBe("Test Project");
  });

  it("fields that are explicitly set to undefined are not modified", async () => {
    const project = insertProject(db, "Test Project");

    db.update(app.projects, project.id, { name: undefined });

    const updatedProject = await db.one(app.projects.where({ id: { eq: project.id } }));
    assert(updatedProject);
    expect(updatedProject.name).toBe("Test Project");
  });

  it("nullable fields can be unset by setting them to null", async () => {
    const owner = insertUser(db);
    const todo = insertTodo(db, { ownerId: owner.id });

    db.update(app.todos, todo.id, { ownerId: null });

    const updatedTodo = await db.one(app.todos.where({ id: { eq: todo.id } }));
    assert(updatedTodo);
    expect(updatedTodo.ownerId).toBeUndefined();
  });

  it("required fields cannot be unset", async () => {
    const todo = insertTodo(db, { title: "Test Todo" });

    // @ts-expect-error - null is not a valid value for a required field
    expect(() => db.update(app.todos, todo.id, { title: null })).toThrow(
      "Cannot set required field 'title' to null",
    );
  });
});
