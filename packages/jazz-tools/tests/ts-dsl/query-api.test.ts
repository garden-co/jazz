import { createDb, type Db } from "../../src/runtime/db.js";
import { afterEach, describe, it, expect, assert, expectTypeOf } from "vitest";
import { app, Project, Todo, User } from "./fixtures/basic/app";

function uniqueDbName(label: string): string {
  return `test-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function insertOwner(db: Db, name = "Test User") {
  return db.insert(app.users, { name });
}

function insertProject(db: Db, name = "Test Project") {
  return db.insert(app.projects, { name });
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

    const { id } = insertProject(db, "Project A");

    const results = await db.all(app.projects.where({ id: { eq: id } }));
    expect(results.length).toBe(1);

    expectTypeOf(results[0]).branded.toEqualTypeOf<Project>();
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

    const { id: projectId } = insertProject(db);
    const { id: ownerId } = insertOwner(db);
    const { id: todoId } = db.insert(app.todos, {
      title: "Hello world",
      done: false,
      tags: ["general"],
      project: projectId,
      owner: ownerId,
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

    const { id: projectId } = insertProject(db, "Announcements");
    const { id: ownerId } = insertOwner(db);
    const { id: todoId } = db.insert(app.todos, {
      title: "Write tests",
      done: false,
      tags: ["dev"],
      project: projectId,
      owner: ownerId,
    });

    const results = await db.all(
      app.todos.where({ id: { eq: todoId } }).include({ project: true }),
    );

    expect(results.length).toBe(1);
    const todo = results[0];
    expect(todo.title).toBe("Write tests");
    expectTypeOf(todo.owner).toEqualTypeOf<string | undefined>();
    expect(todo.owner).toBe(ownerId);
    expectTypeOf(todo.project).toEqualTypeOf<Project>();
    expect(todo.project.name).toBe("Announcements");
  });

  it("select narrows root columns while preserving id and includes", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("select-root-columns") },
      }),
    );

    const { id: projectId } = insertProject(db, "Announcements");
    const { id: ownerId } = insertOwner(db);
    const { id: todoId } = db.insert(app.todos, {
      title: "Write tests",
      done: false,
      tags: ["dev"],
      project: projectId,
      owner: ownerId,
    });

    const result = await db.one(
      app.todos
        .select("title")
        .where({ id: { eq: todoId } })
        .include({ project: true }),
    );

    assert(result, "Result is not defined");
    expectTypeOf(result.id).toEqualTypeOf<string>();
    expectTypeOf(result.title).toEqualTypeOf<string>();
    expectTypeOf(result.project).toEqualTypeOf<Project>();
    expect(result).toEqual({
      id: todoId,
      title: "Write tests",
      project: {
        id: projectId,
        name: "Announcements",
      },
    });
    expect("done" in result).toBe(false);
    expect("tags" in result).toBe(false);
  });

  it("include only resolves the provided columns, not all references", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("select-root-columns") },
      }),
    );

    const { id: projectId } = insertProject(db, "Announcements");
    const { id: ownerId } = insertOwner(db);
    const { id: todoId } = db.insert(app.todos, {
      title: "Write tests",
      done: false,
      tags: ["dev"],
      project: projectId,
      owner: ownerId,
    });

    const result = await db.one(
      app.todos
        .select("owner")
        .where({ id: { eq: todoId } })
        .include({ project: true }),
    );

    assert(result, "Result is not defined");
    expectTypeOf(result.owner).toEqualTypeOf<string | undefined>();
    expect(result.owner).toBe(ownerId);
    expectTypeOf(result.project).toEqualTypeOf<Project>();
    expect(result.project.name).toBe("Announcements");
  });

  it("include returns 'undefined' for null foreign key columns", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("select-root-columns") },
      }),
    );

    const { id: projectId } = insertProject(db, "Announcements");
    const { id: todoId } = db.insert(app.todos, {
      title: "Write tests",
      done: false,
      tags: ["dev"],
      project: projectId,
      owner: undefined,
    });

    const result = await db.one(app.todos.where({ id: { eq: todoId } }).include({ owner: true }));

    assert(result, "Result is not defined");
    expectTypeOf(result.owner).toEqualTypeOf<User | undefined>();
    expect(result.owner).toBeUndefined();
  });

  it('select("*") resets to all root columns', async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("select-all-columns") },
      }),
    );

    const { id: projectId } = insertProject(db);
    const { id: ownerId } = insertOwner(db);
    const { id: todoId } = db.insert(app.todos, {
      title: "Write tests",
      done: false,
      tags: ["dev"],
      project: projectId,
      owner: ownerId,
    });

    const result = await db.one(app.todos.select("*").where({ id: { eq: todoId } }));

    assert(result, "Result is not defined");
    expectTypeOf(result).branded.toEqualTypeOf<Todo>();
    expect(result).toEqual({
      id: todoId,
      title: "Write tests",
      done: false,
      tags: ["dev"],
      project: projectId,
      owner: ownerId,
    });
  });

  it("include builders can project nested relation columns", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("select-nested-columns") },
      }),
    );

    const { id: projectId } = insertProject(db, "Announcements");
    const { id: ownerId } = insertOwner(db);
    const { id: todoId } = db.insert(app.todos, {
      title: "Write tests",
      done: false,
      tags: ["dev"],
      project: projectId,
      owner: ownerId,
    });

    const result = await db.one(
      app.projects
        .where({ id: { eq: projectId } })
        .include({ todosViaProject: app.todos.select("title") }),
    );

    assert(result, "Result is not defined");
    expect(result).toEqual({
      id: projectId,
      name: "Announcements",
      todosViaProject: [
        {
          id: todoId,
          title: "Write tests",
        },
      ],
    });
    expectTypeOf(result.name).toEqualTypeOf<string>();
    expectTypeOf(result.todosViaProject).branded.toEqualTypeOf<{ id: string; title: string }[]>();
    expect("done" in result.todosViaProject[0]).toBe(false);
    expect("tags" in result.todosViaProject[0]).toBe(false);
    expect("project" in result.todosViaProject[0]).toBe(false);
  });

  it("subscribeAll preserves projected root columns with includes", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("subscribe-select-root-columns") },
      }),
    );

    const { id: projectId } = insertProject(db, "Announcements");
    const { id: ownerId } = insertOwner(db);

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

    const { id: todoId } = db.insert(app.todos, {
      title: "Watch subscription",
      done: false,
      tags: ["dev"],
      project: projectId,
      owner: ownerId,
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
      const { id: projectId } = insertProject(db);
      const { id: ownerId } = insertOwner(db);
      const { id: id1 } = db.insert(app.todos, {
        title: "Todo 1",
        done: false,
        tags: ["tag1"],
        project: projectId,
        owner: ownerId,
      });
      db.insert(app.todos, {
        title: "Todo 2",
        done: false,
        tags: ["tag2"],
        project: projectId,
        owner: ownerId,
      });
      db.insert(app.todos, {
        title: "Todo 3",
        done: false,
        tags: ["tag1", "tag2"],
        project: projectId,
        owner: ownerId,
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
      const { id: projectId } = insertProject(db);
      const { id: ownerId } = insertOwner(db);
      const { id: id1 } = db.insert(app.todos, {
        title: "Todo 1",
        done: false,
        tags: ["tag1"],
        project: projectId,
        owner: ownerId,
      });
      db.insert(app.todos, {
        title: "Todo 2",
        done: false,
        tags: ["tag2"],
        project: projectId,
        owner: ownerId,
      });
      const { id: id3 } = db.insert(app.todos, {
        title: "Todo 3",
        done: false,
        tags: ["tag1", "tag2"],
        project: projectId,
        owner: ownerId,
      });

      const todosWithTags = await db.all(app.todos.where({ tags: { contains: "tag1" } }));
      expect(todosWithTags.length).toBe(2);
      expect(todosWithTags).toContainEqual(expect.objectContaining({ id: id1 }));
      expect(todosWithTags).toContainEqual(expect.objectContaining({ id: id3 }));
    });
  });
});
