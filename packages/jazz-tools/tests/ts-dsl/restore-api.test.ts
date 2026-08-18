import { createDb, type Db } from "../../src/runtime/db.js";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { app } from "./fixtures/basic/schema";
import { insertProject, insertUser } from "./factories";

describe("TS Restore API", () => {
  let db: Db;

  beforeEach(async () => {
    db = await createDb({
      appId: "test-app",
      driver: { type: "memory" },
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

  it("can wait for restores to become immediately visible", async () => {
    const project = await db.insert(app.projects, { name: "Test Project" }).wait({ tier: "none" });
    await db.delete(app.projects, project.id).wait({ tier: "none" });

    const restored = await db
      .restore(app.projects, project.id, { name: "Restored Project" })
      .wait({ tier: "none" });

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
      .wait({ tier: "none" });
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

  it("restores an all-default row from empty data", async () => {
    const { value: inserted } = db.insert(app.table_with_defaults, {});
    await db.delete(app.table_with_defaults, inserted.id).wait({ tier: "none" });

    const { value: restored } = db.restore(app.table_with_defaults, inserted.id, {});

    expect(restored).toEqual(inserted);
    await expect(
      db.one(app.table_with_defaults.where({ id: { eq: inserted.id } }), { tier: "local" }),
    ).resolves.toEqual(inserted);
  });

  it("fails when the row is not deleted", async () => {
    const project = insertProject(db);

    expect(() => db.restore(app.projects, project.id, { name: "Restored Project" })).toThrow(
      `Restore failed: WriteError("row not deleted: ${project.id}")`,
    );
  });
});
