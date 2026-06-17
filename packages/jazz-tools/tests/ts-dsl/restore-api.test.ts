import { createDb, type Db } from "../../src/runtime/db.js";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { app } from "./fixtures/basic/schema";
import { insertProject, insertUser, uniqueDbName } from "./factories";

describe("TS Restore API", () => {
  let db: Db;

  beforeEach(async () => {
    db = await createDb({
      appId: "test-app",
      driver: { type: "persistent", dbName: uniqueDbName("restore-api") },
    });
  });

  afterEach(async () => {
    await db.shutdown();
  });

  it("restores a deleted row and returns the restored row", async () => {
    const project = insertProject(db, "Test Project");
    db.delete(app.projects, project.id);

    const { value: restored } = db.restore(app.projects, project.id, {
      name: "Restored Project",
    });

    expect(restored).toEqual({
      id: project.id,
      name: "Restored Project",
    });

    const queried = await db.one(app.projects.where({ id: { eq: project.id } }));
    expect(queried).toEqual(restored);
  });

  it("can wait for restores to be persisted up to a specific durability tier", async () => {
    const project = await db.insert(app.projects, { name: "Test Project" }).wait({ tier: "local" });
    await db.delete(app.projects, project.id).wait({ tier: "local" });

    const restored = await db
      .restore(app.projects, project.id, { name: "Restored Project" })
      .wait({ tier: "local" });

    expect(restored).toEqual({
      id: project.id,
      name: "Restored Project",
    });

    const queried = await db.one(app.projects.where({ id: { eq: project.id } }), {
      tier: "local",
    });
    expect(queried).toEqual(restored);
  });

  it("uses default values missing from restore data", async () => {
    const id = "00000000-0000-0000-0000-000000000000";
    const project = insertProject(db);
    const owner = insertUser(db);
    const todo = await db
      .insert(
        app.todos,
        {
          title: "Test Todo",
          projectId: project.id,
          ownerId: owner.id,
        },
        { id },
      )
      .wait({ tier: "local" });
    db.delete(app.todos, todo.id);

    const { value: restored } = db.restore(app.todos, todo.id, {
      title: "Restored Todo",
      projectId: project.id,
      ownerId: owner.id,
    });

    expect(restored).toEqual({
      id,
      title: "Restored Todo",
      projectId: project.id,
      ownerId: owner.id,
      done: false,
      tags: [],
      assigneesIds: [],
    });
  });

  it("fails when the row is not deleted", async () => {
    const project = insertProject(db);

    expect(() => db.restore(app.projects, project.id, { name: "Restored Project" })).toThrow(
      `Restore failed: WriteError("row not deleted: ${project.id}")`,
    );
  });
});
