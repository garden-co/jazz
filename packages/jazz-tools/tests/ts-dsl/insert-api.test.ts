import { createDb, type Db } from "../../src/runtime/db.js";
import { afterEach, describe, expect, it } from "vitest";
import { app } from "./fixtures/basic/app";

function uniqueDbName(label: string): string {
  return `test-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

describe("TS Insert API", () => {
  const dbs: Db[] = [];

  function track(db: Db): Db {
    dbs.push(db);
    return db;
  }

  afterEach(async () => {
    for (const db of dbs) {
      try {
        await db.shutdown();
      } catch {
        // Best effort
      }
    }
    dbs.length = 0;
  });

  it("returns the inserted row", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("insert-row-shape") },
      }),
    );

    const project = await db.insert(app.projects, { name: "Test Project" });

    expect(project).toEqual({
      id: expect.any(String),
      name: "Test Project",
    });

    const todo = await db.insert(app.todos, {
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
});
