import { createDb, type Db } from "../../src/runtime/db.js";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { app } from "./fixtures/basic/app";
import { insertProject, insertUser, uniqueDbName } from "./factories";

describe("TS Insert API", () => {
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
    const owner = insertUser(db);

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

  it("uses default values missing from the insert data", async () => {
    const project = insertProject(db);
    const owner = insertUser(db);
    const todo = db.insert(app.todos, {
      title: "Test Todo",
      projectId: project.id,
      ownerId: owner.id,
    });

    expect(todo).toEqual({
      id: todo.id,
      title: "Test Todo",
      projectId: project.id,
      ownerId: owner.id,
      done: false,
      tags: [],
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
    const owner = insertUser(db);

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
});
