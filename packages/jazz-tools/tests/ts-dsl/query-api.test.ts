import { createDb, type Db } from "../../src/runtime/db.js";
import { afterEach, describe, it, expect, assert, expectTypeOf } from "vitest";
import { app, Project, Todo, User, UserSelected } from "./fixtures/basic/app";

function uniqueDbName(label: string): string {
  return `test-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function insertUser(db: Db, name = "Test User") {
  return db.insert(app.users, { name });
}

function insertProject(db: Db, name = "Test Project") {
  return db.insert(app.projects, { name });
}

function insertTodo(db: Db, data: Partial<Todo>) {
  return db.insert(app.todos, {
    title: data.title ?? "Test Todo",
    done: data.done ?? false,
    tags: data.tags ?? [],
    project: data.project ?? insertProject(db).id,
    owner: data.owner ?? undefined,
    assignees: data.assignees ?? [],
  });
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
    const { id: ownerId } = insertUser(db);
    const { id: todoId } = insertTodo(db, {
      title: "Hello world",
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
    const { id: ownerId } = insertUser(db);
    const { id: todoId } = insertTodo(db, {
      title: "Write tests",
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
    expectTypeOf(todo.project).toEqualTypeOf<Project | undefined>();
    expect(todo.project?.name).toBe("Announcements");
  });

  it("include returns 'undefined' for missing scalar referenced entities", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("include-returns-entity") },
      }),
    );

    const project = insertProject(db);
    const todo = insertTodo(db, {
      project: project.id,
    });

    await db.delete(app.projects, project.id);

    const result = await db.one(
      app.todos.where({ id: { eq: todo.id } }).include({ project: true }),
    );
    assert(result, "Result is not defined");
    expectTypeOf(result.project).toEqualTypeOf<Project | undefined>();
    expect(result.project).toBeUndefined();
  });

  it("requireIncludes filters out rows with missing scalar referenced entities", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("require-includes-scalar-missing") },
      }),
    );

    const project = insertProject(db);
    const todo = insertTodo(db, {
      project: project.id,
    });

    await db.delete(app.projects, project.id);

    const result = await db.one(
      app.todos
        .where({ id: { eq: todo.id } })
        .include({ project: true })
        .requireIncludes(),
    );

    expect(result).toBeNull();
  });

  it("requireIncludes does not filter out rows with null scalar references", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("require-includes-scalar-missing") },
      }),
    );

    const todo = insertTodo(db, {
      owner: undefined,
    });

    const result = await db.one(
      app.todos
        .where({ id: { eq: todo.id } })
        .include({ owner: true })
        .requireIncludes(),
    );

    assert(result, "Result is not defined");
    expect(result.id).toBe(todo.id);
    expect(result.owner).toBeUndefined();
  });

  it("include skips missing referenced entities in forward array relations", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("include-returns-entity") },
      }),
    );

    const assignee1 = insertUser(db);
    const assignee2 = insertUser(db);
    const todo = insertTodo(db, {
      assignees: [assignee1.id, assignee2.id],
    });

    await db.delete(app.users, assignee1.id);

    const result = await db.one(
      app.todos.where({ id: { eq: todo.id } }).include({ assignees: app.users.select("id") }),
    );
    assert(result, "Result is not defined");
    expectTypeOf(result.assignees).branded.toEqualTypeOf<{ id: string }[]>();
    expect(result.assignees).toEqual([{ id: assignee2.id }]);
  });

  it("requireIncludes filters out rows with missing entities in forward array relations", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("require-includes-array-missing") },
      }),
    );

    const assignee1 = insertUser(db);
    const assignee2 = insertUser(db);
    const todo = insertTodo(db, {
      assignees: [assignee1.id, assignee2.id],
    });

    await db.delete(app.users, assignee1.id);

    const result = await db.one(
      app.todos
        .where({ id: { eq: todo.id } })
        .include({ assignees: app.users.select("id") })
        .requireIncludes(),
    );

    expect(result).toBeNull();
  });

  it("include skips missing referenced entities in reverse relations", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("include-returns-entity") },
      }),
    );

    const owner = insertUser(db);
    const { id: todoId } = insertTodo(db, {
      owner: owner.id,
    });
    const { id: todoId2 } = insertTodo(db, {
      owner: owner.id,
    });

    await db.delete(app.todos, todoId);

    const result = await db.one(
      app.users.where({ id: { eq: owner.id } }).include({ todosViaOwner: app.todos.select("id") }),
    );
    assert(result, "Result is not defined");
    expectTypeOf(result.todosViaOwner).branded.toEqualTypeOf<{ id: string }[]>();
    expect(result.todosViaOwner).toEqual([{ id: todoId2 }]);
  });

  it("requireIncludes does not filter rows for reverse relations", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("require-includes-reverse") },
      }),
    );

    const owner = insertUser(db);
    const { id: todoId } = insertTodo(db, {
      owner: owner.id,
    });
    const { id: todoId2 } = insertTodo(db, {
      owner: owner.id,
    });

    await db.delete(app.todos, todoId);

    const result = await db.one(
      app.users
        .where({ id: { eq: owner.id } })
        .include({ todosViaOwner: app.todos.select("id") })
        .requireIncludes(),
    );
    assert(result, "Result is not defined");
    expect(result.todosViaOwner).toEqual([{ id: todoId2 }]);
  });

  it("select narrows root columns while preserving id and includes", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("select-root-columns") },
      }),
    );

    const { id: projectId } = insertProject(db, "Announcements");
    const { id: todoId } = insertTodo(db, {
      title: "Write tests",
      done: false,
      tags: ["dev"],
      project: projectId,
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
    expectTypeOf(result.project).toEqualTypeOf<Project | undefined>();
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
    const { id: ownerId } = insertUser(db);
    const { id: todoId } = insertTodo(db, {
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
    expectTypeOf(result.project).toEqualTypeOf<Project | undefined>();
    assert(result.project, "Project include is not defined");
    expect(result.project.name).toBe("Announcements");
  });

  it("include returns 'undefined' for null foreign key columns", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("select-root-columns") },
      }),
    );

    const { id: todoId } = insertTodo(db, {
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
    const { id: ownerId } = insertUser(db);
    const { id: todoId } = insertTodo(db, {
      title: "Write tests",
      done: false,
      tags: ["dev"],
      project: projectId,
      owner: ownerId,
      assignees: [],
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
      assignees: [],
    });
  });

  it("selects and filters permission magic columns end to end", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("select-magic-columns") },
        localAuthMode: "anonymous",
        localAuthToken: "magic-columns-user",
      }),
    );

    const { id: projectId } = insertProject(db, "Announcements");
    const { id: editableId } = insertTodo(db, {
      title: "Draft docs",
      done: false,
      tags: ["dev"],
      project: projectId,
      assignees: [],
    });
    const { id: lockedId } = insertTodo(db, {
      title: "Shipped docs",
      done: true,
      tags: ["docs"],
      project: projectId,
      assignees: [],
    });

    const projected = await db.all(
      app.todos.select("title", "$canRead", "$canEdit", "$canDelete").orderBy("title", "asc"),
    );

    expect(projected).toEqual([
      {
        id: editableId,
        title: "Draft docs",
        $canRead: true,
        $canEdit: true,
        $canDelete: true,
      },
      {
        id: lockedId,
        title: "Shipped docs",
        $canRead: true,
        $canEdit: false,
        $canDelete: false,
      },
    ]);

    const editableOnly = await db.all(
      app.todos.where({ $canEdit: true }).select("title", "$canEdit").orderBy("title", "asc"),
    );

    expect(editableOnly).toEqual([
      {
        id: editableId,
        title: "Draft docs",
        $canEdit: true,
      },
    ]);

    const readableOnly = await db.all(
      app.todos.where({ $canRead: true }).select("title", "$canRead").orderBy("title", "asc"),
    );

    expect(readableOnly).toEqual([
      {
        id: editableId,
        title: "Draft docs",
        $canRead: true,
      },
      {
        id: lockedId,
        title: "Shipped docs",
        $canRead: true,
      },
    ]);

    const deletableOnly = await db.all(
      app.todos.where({ $canDelete: true }).select("title", "$canDelete").orderBy("title", "asc"),
    );

    expect(deletableOnly).toEqual([
      {
        id: editableId,
        title: "Draft docs",
        $canDelete: true,
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

    const { id: projectId } = insertProject(db, "Announcements");
    const { id: ownerId } = insertUser(db);
    const { id: todoId } = insertTodo(db, {
      title: "Write tests",
      done: false,
      tags: ["dev"],
      project: projectId,
      owner: ownerId,
      assignees: [],
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
    const { id: ownerId } = insertUser(db);

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

    const { id: todoId } = insertTodo(db, {
      title: "Watch subscription",
      done: false,
      tags: ["dev"],
      project: projectId,
      owner: ownerId,
      assignees: [],
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
      const { id: id1 } = insertTodo(db, {
        title: "Todo 1",
        tags: ["tag1"],
      });
      insertTodo(db, {
        title: "Todo 2",
        tags: ["tag2"],
      });
      insertTodo(db, {
        title: "Todo 3",
        tags: ["tag1", "tag2"],
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
      const { id: id1 } = insertTodo(db, {
        title: "Todo 1",
        tags: ["tag1"],
      });
      insertTodo(db, {
        title: "Todo 2",
        tags: ["tag2"],
      });
      const { id: id3 } = insertTodo(db, {
        title: "Todo 3",
        tags: ["tag1", "tag2"],
      });

      const todosWithTags = await db.all(app.todos.where({ tags: { contains: "tag1" } }));
      expect(todosWithTags.length).toBe(2);
      expect(todosWithTags).toContainEqual(expect.objectContaining({ id: id1 }));
      expect(todosWithTags).toContainEqual(expect.objectContaining({ id: id3 }));
    });
  });
});
