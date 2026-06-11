import { createDb, type Db } from "../../src/runtime/db.js";
import { afterEach, assert, beforeEach, describe, expect, it } from "vitest";
import { app } from "./fixtures/basic/schema";
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
    expect(updatedTodo.ownerId).toBeNull();
  });

  it("required fields cannot be unset", async () => {
    const todo = insertTodo(db, { title: "Test Todo" });

    // @ts-expect-error - null is not a valid value for a required field
    expect(() => db.update(app.todos, todo.id, { title: null })).toThrow(
      "Cannot set required field 'title' to null",
    );
  });

  it("updates rows synchronously and returns a write handle", async () => {
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

    const result = db.update(app.todos, todo.id, { done: true });
    expect(result).toMatchObject({
      wait: expect.any(Function),
    });

    const [updated] = await db.all(app.todos.where({ id: { eq: todo.id } }));
    expect(updated!.done).toBe(true);
  });

  it("can wait for updates to be persisted up to a specific durability tier", async () => {
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

    const pending = db.update(app.todos, todo.id, { done: true });
    await pending.wait({ tier: "local" });

    const [updated] = await db.all(app.todos.where({ id: { eq: todo.id } }), { tier: "local" });
    expect(updated!.done).toBe(true);
  });

  it("can use caller-supplied updatedAt on update", async () => {
    const updatedAt = 1_704_067_200_123_000;
    const project = insertProject(db, "Test Project");

    db.update(app.projects, project.id, { name: "Backfilled Project" }, { updatedAt });

    const projected = await db.one(
      app.projects.select("name", "$updatedAt").where({ id: { eq: project.id } }),
    );

    expect(projected).toEqual({
      id: project.id,
      name: "Backfilled Project",
      $updatedAt: new Date(Math.trunc(updatedAt / 1_000)),
    });
  });

  it("trying to update an already-deleted row fails", async () => {
    const project = insertProject(db);
    db.delete(app.projects, project.id);

    expect(() => db.update(app.projects, project.id, { name: "Restored Project" })).toThrow(
      `Update failed: WriteError("row already deleted: ${project.id}")`,
    );
  });

  it("enforces constraints on JSON schemas", async () => {
    const { value } = db.insert(app.table_with_constraints, {
      data: {},
    });

    expect(() => db.update(app.table_with_constraints, value.id, { data: { string: "" } })).toThrow(
      'Update failed: WriteError("encoding error: JSON schema validation failed for column `data`: \\"\\" is shorter than 1 character")',
    );

    expect(() =>
      db.update(app.table_with_constraints, value.id, {
        data: { string: "01234567890" },
      }),
    ).toThrow(
      'Update failed: WriteError("encoding error: JSON schema validation failed for column `data`: \\"01234567890\\" is longer than 10 characters")',
    );

    expect(() =>
      db.update(app.table_with_constraints, value.id, {
        data: { integer: -1 },
      }),
    ).toThrow(
      'Update failed: WriteError("encoding error: JSON schema validation failed for column `data`: -1 is less than the minimum of 0")',
    );

    expect(() =>
      db.update(app.table_with_constraints, value.id, {
        data: { integer: 11 },
      }),
    ).toThrow(
      'Update failed: WriteError("encoding error: JSON schema validation failed for column `data`: 11 is greater than the maximum of 10")',
    );

    expect(() =>
      db.update(app.table_with_constraints, value.id, {
        data: { datetime: "2020-01-01T06:15:00" },
      }),
    ).toThrow(
      'Update failed: WriteError("encoding error: JSON schema validation failed for column `data`: \\"2020-01-01T06:15:00\\" does not match \\"^(?:(?:[0-9][0-9][2468][048]|[0-9][0-9][13579][26]|[0-9][0-9]0[48]|[02468][048]00|[13579][26]00)-02-29|[0-9]{4}-(?:(?:0[13578]|1[02])-(?:0[1-9]|[12][0-9]|3[01])|(?:0[469]|11)-(?:0[1-9]|[12][0-9]|30)|(?:02)-(?:0[1-9]|1[0-9]|2[0-8])))T(?:(?:[01][0-9]|2[0-3]):[0-5][0-9](?::[0-5][0-9](?:\\\\.[0-9]+)?)?(?:Z))$\\"")',
    );
  });
});
