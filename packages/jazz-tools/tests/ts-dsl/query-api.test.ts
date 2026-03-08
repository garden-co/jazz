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
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("query-by-id") },
      }),
    );

    const { id } = await db.insert(app.projects, { name: "Project A" });

    const results = await db.all(app.projects.where({ id: { eq: id } }));
    expect(results.length).toBe(1);

    expect(results[0].id).toBe(id);
    expect(results[0].name).toBe("Project A");
  });

  it("text is not corrupted when using include", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("include-corruption") },
      }),
    );

    const { id: projectId } = await db.insert(app.projects, { name: "Announcements" });
    const { id: todoId } = await db.insert(app.todos, {
      title: "Hello world",
      done: false,
      tags: ["general"],
      project: projectId,
    });

    const baseline = await db.all(app.todos.where({ id: { eq: todoId } }));
    expect(baseline[0].title).toBe("Hello world");

    const withInclude = await db.all(
      app.todos.where({ id: { eq: todoId } }).include({ project: true }),
    );

    expect(withInclude.length).toBe(1);
    expect(withInclude[0].title).toBe("Hello world");
  });

  it("include returns the related entity", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("include-returns-entity") },
      }),
    );

    const { id: projectId } = await db.insert(app.projects, { name: "Announcements" });
    const { id: todoId } = await db.insert(app.todos, {
      title: "Write tests",
      done: false,
      tags: ["dev"],
      project: projectId,
    });

    const results = await db.all(
      app.todos.where({ id: { eq: todoId } }).include({ project: true }),
    );

    expect(results.length).toBe(1);
    const todo = results[0];
    expect(todo.title).toBe("Write tests");
    expect(todo.project).toBeDefined();
    expect(todo.project?.name).toBe("Announcements");
  });

  it("select narrows root columns while preserving id and includes", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("select-root-columns") },
      }),
    );

    const { id: projectId } = await db.insert(app.projects, { name: "Announcements" });
    const { id: todoId } = await db.insert(app.todos, {
      title: "Write tests",
      done: false,
      tags: ["dev"],
      project: projectId,
    });

    const results = await db.all(
      app.todos
        .select("title")
        .where({ id: { eq: todoId } })
        .include({ project: true }),
    );

    expect(results).toEqual([
      {
        id: todoId,
        title: "Write tests",
        project: {
          id: projectId,
          name: "Announcements",
        },
      },
    ]);
    expect("done" in results[0]).toBe(false);
    expect("tags" in results[0]).toBe(false);
  });

  it('select("*") resets to all root columns', async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("select-all-columns") },
      }),
    );

    const { id: projectId } = await db.insert(app.projects, { name: "Announcements" });
    const { id: todoId } = await db.insert(app.todos, {
      title: "Write tests",
      done: false,
      tags: ["dev"],
      project: projectId,
    });

    const results = await db.all(app.todos.select("*").where({ id: { eq: todoId } }));

    expect(results).toEqual([
      {
        id: todoId,
        title: "Write tests",
        done: false,
        tags: ["dev"],
        project: projectId,
      },
    ]);
  });

  it("include builders can project nested relation columns", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("select-nested-columns") },
      }),
    );

    const { id: projectId } = await db.insert(app.projects, { name: "Announcements" });
    const { id: todoId } = await db.insert(app.todos, {
      title: "Write tests",
      done: false,
      tags: ["dev"],
      project: projectId,
    });

    const results = await db.all(
      app.projects
        .where({ id: { eq: projectId } })
        .include({ todosViaProject: app.todos.select("title") }),
    );

    expect(results).toEqual([
      {
        id: projectId,
        name: "Announcements",
        todosViaProject: [
          {
            id: todoId,
            title: "Write tests",
          },
        ],
      },
    ]);
    expect("done" in results[0].todosViaProject![0]).toBe(false);
    expect("tags" in results[0].todosViaProject![0]).toBe(false);
    expect("project" in results[0].todosViaProject![0]).toBe(false);
  });

  it("subscribeAll preserves projected root columns with includes", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("subscribe-select-root-columns") },
      }),
    );

    const { id: projectId } = await db.insert(app.projects, { name: "Announcements" });

    type SubscribedTodo = {
      id: string;
      title: string;
      project: {
        id: string;
        name: string;
      };
    };

    let unsubscribe = () => {};
    let timeout: ReturnType<typeof setTimeout> | undefined;
    const deltaPromise = new Promise<{ all: SubscribedTodo[] }>((resolve, reject) => {
      timeout = setTimeout(() => {
        unsubscribe();
        reject(new Error("Timed out waiting for subscribeAll projection update"));
      }, 10_000);

      unsubscribe = db.subscribeAll(
        app.todos.select("title").include({ project: true }),
        (delta) => {
          if (delta.all.length !== 1) {
            return;
          }

          resolve(delta as { all: SubscribedTodo[] });
        },
      );
    });

    await new Promise((resolve) => setTimeout(resolve, 0));

    const { id: todoId } = await db.insert(app.todos, {
      title: "Watch subscription",
      done: false,
      tags: ["dev"],
      project: projectId,
    });

    const delta = await deltaPromise;
    if (timeout) {
      clearTimeout(timeout);
    }
    unsubscribe();

    expect(delta.all).toEqual([
      {
        id: todoId,
        title: "Watch subscription",
        project: {
          id: projectId,
          name: "Announcements",
        },
      },
    ]);
    expect("done" in delta.all[0]).toBe(false);
    expect("tags" in delta.all[0]).toBe(false);
  });

  describe("query by array column", () => {
    it("using eq", async () => {
      const db = track(
        await createDb({
          appId: "test-app",
          driver: { type: "persistent", dbName: uniqueDbName("query-by-array-column-equality") },
        }),
      );
      const { id: projectId } = await db.insert(app.projects, { name: "Project A" });
      const { id: id1 } = await db.insert(app.todos, {
        title: "Todo 1",
        done: false,
        tags: ["tag1"],
        project: projectId,
      });
      await db.insert(app.todos, {
        title: "Todo 2",
        done: false,
        tags: ["tag2"],
        project: projectId,
      });
      await db.insert(app.todos, {
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
          driver: { type: "persistent", dbName: uniqueDbName("query-by-array-column-contains") },
        }),
      );
      const { id: projectId } = await db.insert(app.projects, { name: "Project A" });
      const { id: id1 } = await db.insert(app.todos, {
        title: "Todo 1",
        done: false,
        tags: ["tag1"],
        project: projectId,
      });
      await db.insert(app.todos, {
        title: "Todo 2",
        done: false,
        tags: ["tag2"],
        project: projectId,
      });
      const { id: id3 } = await db.insert(app.todos, {
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
