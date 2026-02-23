import { createDb, type Db } from "../../src/runtime/db.js";
import { afterEach, describe, it, expect } from "vitest";
import { app } from "./fixtures/basic/app";

function uniqueDbName(label: string): string {
  return `test-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

describe("TS Query API", () => {
  const dbs: Db[] = [];

  /** Track dbs for cleanup. */
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

  it("queries by id", async () => {
    const db = track(await createDb({ appId: "test-app", dbName: uniqueDbName("query-by-id") }));

    const id = db.insert(app.projects, { name: "Project A" });

    const results = await db.all(app.projects.where({ id: { eq: id } }));
    expect(results.length).toBe(1);

    expect(results[0].id).toBe(id);
    expect(results[0].name).toBe("Project A");
  });

  it("text is not corrupted when using include", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        dbName: uniqueDbName("include-corruption"),
      }),
    );

    const projectId = db.insert(app.projects, { name: "Announcements" });
    const todoId = db.insert(app.todos, {
      title: "Hello world",
      done: false,
      tags: ["general"],
      project: projectId,
    });

    const baseline = await db.all(app.todos.where({ id: { eq: todoId } }));
    expect(baseline[0].title).toBe("Hello world");

    const withInclude = await db.all(
      app.todos
        .where({ id: { eq: todoId } })
        .include({ project: true }),
    );

    expect(withInclude.length).toBe(1);
    expect(withInclude[0].title).toBe("Hello world");
  });

  describe("query by array column", () => {
    it("using eq", async () => {
      const db = track(
        await createDb({
          appId: "test-app",
          dbName: uniqueDbName("query-by-array-column-equality"),
        }),
      );
      const projectId = db.insert(app.projects, { name: "Project A" });
      const id1 = db.insert(app.todos, {
        title: "Todo 1",
        done: false,
        tags: ["tag1"],
        project: projectId,
      });
      const _id2 = db.insert(app.todos, {
        title: "Todo 2",
        done: false,
        tags: ["tag2"],
        project: projectId,
      });
      const _id3 = db.insert(app.todos, {
        title: "Todo 3",
        done: false,
        tags: ["tag1", "tag2"],
        project: projectId,
      });

      const todosWithTags = await db.all(app.todos.where({ tags: { eq: ["tag1"] } }));
      expect(todosWithTags.length).toBe(1);
      expect(todosWithTags[0].id).toEqual(id1);
    });

    it("using contains", async () => {
      const db = track(
        await createDb({
          appId: "test-app",
          dbName: uniqueDbName("query-by-array-column-contains"),
        }),
      );
      const projectId = db.insert(app.projects, { name: "Project A" });
      const id1 = db.insert(app.todos, {
        title: "Todo 1",
        done: false,
        tags: ["tag1"],
        project: projectId,
      });
      const _id2 = db.insert(app.todos, {
        title: "Todo 2",
        done: false,
        tags: ["tag2"],
        project: projectId,
      });
      const id3 = db.insert(app.todos, {
        title: "Todo 3",
        done: false,
        tags: ["tag1", "tag2"],
        project: projectId,
      });

      const todosWithTags = await db.all(app.todos.where({ tags: { contains: "tag1" } }));
      expect(todosWithTags.length).toBe(2);
      expect(todosWithTags).toContainEqual(expect.objectContaining({ id: id1 }));
      expect(todosWithTags).toContainEqual(expect.objectContaining({ id: id3 }));
    });
  });
});
