import { createDb, type Db } from "../../src/runtime/db.js";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { app } from "./fixtures/basic/app";

function uniqueDbName(label: string): string {
  return `test-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

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

    expect(project).toEqual({
      id: expect.any(String),
      name: "Test Project",
    });

    const todo = db.insert(app.todos, {
      title: "Test Todo",
      done: true,
      tags: ["tag1", "tag2"],
      project: project.id,
    });

    expect(todo).toEqual({
      id: expect.any(String),
      title: "Test Todo",
      done: true,
      tags: ["tag1", "tag2"],
      project: project.id,
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

    const todo = await db.insertDurable(
      app.todos,
      {
        title: "Test Todo",
        done: true,
        tags: ["tag1", "tag2"],
        project: project.id,
      },
      { tier: "worker" },
    );

    expect(todo).toEqual({
      id: expect.any(String),
      title: "Test Todo",
      done: true,
      tags: ["tag1", "tag2"],
      project: project.id,
    });
  });
});
