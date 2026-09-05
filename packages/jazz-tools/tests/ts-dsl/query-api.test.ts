import { createDb } from "../../src/runtime/default-create-db.js";
import type { Db, QueryBuilder, QueryOptions } from "../../src/runtime/db.js";
import { afterEach, beforeEach, describe, it, expect, assert, expectTypeOf } from "vitest";
import { app, type Project, type Todo, type User } from "./fixtures/basic/schema";
import { insertProject, insertTodo, insertUser } from "./factories";

function makeFriends(db: Db, user1: User, user2: User) {
  const user1Friends = [...user1.friendsIds, user2.id];
  const user2Friends = [...user2.friendsIds, user1.id];

  db.update(app.users, user1.id, { friendsIds: user1Friends });
  db.update(app.users, user2.id, { friendsIds: user2Friends });

  // Keep the in-memory fixtures aligned with the DB row so later updates append correctly.
  user1.friendsIds = user1Friends;
  user2.friendsIds = user2Friends;
}

const readModes = ["direct", "mergeable-tx", "exclusive-tx"] as const;
type ReadMode = (typeof readModes)[number];

const orderedIds = {
  low: "00000000-0000-4000-8000-000000000101",
  middle: "00000000-0000-4000-8000-000000000102",
  high: "00000000-0000-4000-8000-000000000103",
} as const;

describe.each(readModes)("TS Query API (%s reads)", (readMode: ReadMode) => {
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

  async function readAll<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T[]> {
    if (readMode === "direct") {
      return db.all(query, options);
    }

    const tx = readMode === "mergeable-tx" ? db.beginTransaction() : db.beginExclusiveTransaction();
    try {
      return await tx.all(query, options);
    } finally {
      tx.rollback();
    }
  }

  async function readOne<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T | null> {
    if (readMode === "direct") {
      return db.one(query, options);
    }

    const tx = readMode === "mergeable-tx" ? db.beginTransaction() : db.beginExclusiveTransaction();
    try {
      return await tx.one(query, options);
    } finally {
      tx.rollback();
    }
  }

  it.skipIf(readMode === "direct")(
    "includes observe staged roots, relation membership, and children",
    async () => {
      const project = insertProject(db, "Committed project");
      const tx =
        readMode === "mergeable-tx" ? db.beginTransaction() : db.beginExclusiveTransaction();
      try {
        const user = tx.insert(app.users, {
          name: "Staged assignee",
          friendsIds: [],
        });
        const todo = tx.insert(app.todos, {
          title: "Staged todo",
          done: false,
          tags: [],
          projectId: project.id,
          assigneesIds: [user.id],
        });
        const read = await tx.one(
          app.todos
            .where({ id: { eq: todo.id } })
            .include({ assignees: app.users.select("id", "name") }),
        );
        assert(read, "staged todo is not defined");
        expect(read.assignees).toEqual([{ id: user.id, name: "Staged assignee" }]);
      } finally {
        tx.rollback();
      }
    },
  );

  it.skipIf(readMode !== "exclusive-tx")(
    "relation reads participate in exclusive transaction conflict detection",
    async () => {
      const user = insertUser(db, "Original assignee");
      const todo = insertTodo(db, { assigneesIds: [user.id] });
      const tx = db.beginExclusiveTransaction();

      const read = await tx.one(
        app.todos
          .where({ id: { eq: todo.id } })
          .include({ assignees: app.users.select("id", "name") }),
      );
      assert(read, "todo is not defined");
      expect(read.assignees).toEqual([{ id: user.id, name: "Original assignee" }]);

      db.update(app.users, user.id, { name: "Concurrent assignee" });
      tx.update(app.todos, todo.id, { title: "Staged title" });

      await expect(tx.commit().wait()).rejects.toThrow("transaction_conflict");
    },
  );

  it.skipIf(readMode === "direct")(
    "relation reads include staged deletions and staged restores",
    async () => {
      const user = insertUser(db, "Assignee");
      const todo = insertTodo(db, { assigneesIds: [user.id] });
      const begin = () =>
        readMode === "mergeable-tx" ? db.beginTransaction() : db.beginExclusiveTransaction();

      const deleting = begin();
      try {
        deleting.delete(app.todos, todo.id);
        const deleted = await deleting.one(
          app.todos
            .includeDeleted()
            .where({ id: { eq: todo.id } })
            .include({ assignees: app.users.select("id", "name") }),
        );
        assert(deleted, "staged deleted todo is not defined");
        expect(deleted.assignees).toEqual([{ id: user.id, name: "Assignee" }]);
      } finally {
        deleting.rollback();
      }

      db.delete(app.todos, todo.id);
      const restoring = begin();
      try {
        restoring.restore(app.todos, todo.id, {
          title: "Restored todo",
          done: false,
          tags: [],
          projectId: todo.projectId,
          assigneesIds: [user.id],
        });
        const restored = await restoring.one(
          app.todos
            .where({ id: { eq: todo.id } })
            .include({ assignees: app.users.select("id", "name") }),
        );
        assert(restored, "staged restored todo is not defined");
        expect(restored.title).toBe("Restored todo");
        expect(restored.assignees).toEqual([{ id: user.id, name: "Assignee" }]);
      } finally {
        restoring.rollback();
      }
    },
  );

  it.skipIf(readMode !== "exclusive-tx")(
    "includeDeleted relation reads retain exclusive transaction conflict witnesses",
    async () => {
      const user = insertUser(db, "Assignee");
      const todo = insertTodo(db, { assigneesIds: [user.id] });
      const tx = db.beginExclusiveTransaction();

      const read = await tx.one(
        app.todos
          .includeDeleted()
          .where({ id: { eq: todo.id } })
          .include({ assignees: app.users.select("id", "name") }),
      );
      assert(read, "todo is not defined");
      expect(read.assignees).toEqual([{ id: user.id, name: "Assignee" }]);

      db.delete(app.users, user.id);
      tx.update(app.todos, todo.id, { title: "Staged title" });

      await expect(tx.commit().wait()).rejects.toThrow("transaction_conflict");
    },
  );

  describe("default ordering", () => {
    it("orders one-shot roots and pagination by canonical row id, not insertion order", async () => {
      db.upsert(app.projects, orderedIds.high, { name: "High" });
      db.upsert(app.projects, orderedIds.low, { name: "Low" });
      db.upsert(app.projects, orderedIds.middle, { name: "Middle" });

      const all = await readAll(app.projects);
      expect(all.map((project) => project.id)).toEqual([
        orderedIds.low,
        orderedIds.middle,
        orderedIds.high,
      ]);

      const window = await readAll(app.projects.offset(1).limit(1));
      expect(window.map((project) => project.id)).toEqual([orderedIds.middle]);
    });

    it("uses canonical row id as the implicit tie-break for explicit ordering", async () => {
      db.upsert(app.projects, orderedIds.high, { name: "Same" });
      db.upsert(app.projects, orderedIds.low, { name: "Same" });
      db.upsert(app.projects, orderedIds.middle, { name: "Same" });

      const results = await readAll(app.projects.orderBy("name", "asc"));
      expect(results.map((project) => project.id)).toEqual([
        orderedIds.low,
        orderedIds.middle,
        orderedIds.high,
      ]);
    });

    it("orders forward-array and reverse-relation payloads by child row id", async () => {
      db.upsert(app.users, orderedIds.high, { name: "High", friendsIds: [] });
      db.upsert(app.users, orderedIds.low, { name: "Low", friendsIds: [] });
      db.upsert(app.users, orderedIds.middle, { name: "Middle", friendsIds: [] });
      const project = insertProject(db, "Relations");
      const todoIds = {
        low: "00000000-0000-4000-8000-000000000201",
        high: "00000000-0000-4000-8000-000000000203",
      } as const;
      db.upsert(app.todos, todoIds.high, {
        title: "High",
        done: false,
        tags: [],
        projectId: project.id,
        ownerId: null,
        assigneesIds: [orderedIds.high, orderedIds.low, orderedIds.middle],
      });
      db.upsert(app.todos, todoIds.low, {
        title: "Low",
        done: false,
        tags: [],
        projectId: project.id,
        ownerId: null,
        assigneesIds: [],
      });

      const todo = await readOne(
        app.todos
          .where({ id: { eq: todoIds.high } })
          .include({ assignees: app.users.select("id") }),
      );
      assert(todo, "Todo is not defined");
      expect(todo.assignees.map((user) => user.id)).toEqual([
        orderedIds.low,
        orderedIds.middle,
        orderedIds.high,
      ]);

      const parent = await readOne(
        app.projects
          .where({ id: { eq: project.id } })
          .include({ todosViaProject: app.todos.select("id") }),
      );
      assert(parent, "Project is not defined");
      expect(parent.todosViaProject.map((child) => child.id)).toEqual([todoIds.low, todoIds.high]);
    });
  });

  describe("filtering", () => {
    it("queries by id", async () => {
      const { id } = insertProject(db, "Project A");

      const results = await readAll(app.projects.where({ id: { eq: id } }));
      expect(results.length).toBe(1);

      expectTypeOf(results[0]!).branded.toEqualTypeOf<Project>();
      expect(results[0]!.id).toBe(id);
      expect(results[0]!.name).toBe("Project A");
    });

    it("can read deleted rows with includeDeleted", async () => {
      const project = insertProject(db, "Deleted Project");
      db.delete(app.projects, project.id);

      const defaultResult = await readOne(app.projects.where({ id: { eq: project.id } }));
      expect(defaultResult).toBeNull();

      const deletedResult = await readOne(
        app.projects.includeDeleted().where({ id: { eq: project.id } }),
      );

      assert(deletedResult, "Deleted row is not defined");
      expectTypeOf(deletedResult).branded.toEqualTypeOf<Project>();
      expect(deletedResult).toEqual(project);
    });

    it("filters nullable columns with isNull:true", async () => {
      const todoWithoutOwner = insertTodo(db, {
        title: "Todo without owner",
        ownerId: null,
      });
      const _todoWithOwner = insertTodo(db, {
        title: "Todo with owner",
        ownerId: insertUser(db).id,
      });

      const results = await readAll(app.todos.where({ ownerId: { isNull: true } }));

      expect(results.map((todo) => todo.id)).toEqual([todoWithoutOwner.id]);
    });

    it("filters non-nullable columns with isNull:false", async () => {
      const _todoWithoutOwner = insertTodo(db, {
        title: "Todo without owner",
        ownerId: null,
      });
      const todoWithOwner = insertTodo(db, {
        title: "Todo with owner",
        ownerId: insertUser(db).id,
      });

      const results = await readAll(app.todos.where({ ownerId: { isNull: false } }));

      expect(results.map((todo) => todo.id)).toEqual([todoWithOwner.id]);
    });

    // Note: this is a difference with respect to SQL, when =null checks always return false.
    it("filters with explicit null values work as isNull:true", async () => {
      const todoWithoutOwner = insertTodo(db, {
        title: "Todo without owner",
        ownerId: null,
      });
      const _todoWithOwner = insertTodo(db, {
        title: "Todo with owner",
        ownerId: insertUser(db).id,
      });

      const resultsNullMatch = await readAll(app.todos.where({ ownerId: null }));
      const resultsEqNull = await readAll(app.todos.where({ ownerId: { eq: null } }));
      expect(resultsNullMatch.map((todo) => todo.id)).toEqual([todoWithoutOwner.id]);
      expect(resultsEqNull.map((todo) => todo.id)).toEqual([todoWithoutOwner.id]);
    });

    it("filters with explicit undefined values are no-ops", async () => {
      const todoWithoutOwner = insertTodo(db, {
        title: "Todo without owner",
        ownerId: null,
      });
      const todoWithOwner = insertTodo(db, {
        title: "Todo with owner",
        ownerId: insertUser(db).id,
      });

      const results = await readAll(app.todos.where({ ownerId: undefined }));
      expect(results.map((todo) => todo.id)).toEqual(
        [todoWithoutOwner.id, todoWithOwner.id].sort(),
      );
    });

    describe("in operator", () => {
      it("queries by id with in", async () => {
        const projectA = insertProject(db, "Project A");
        const projectB = insertProject(db, "Project B");
        const _projectC = insertProject(db, "Project C");

        const results = await readAll(
          app.projects.where({ id: { in: [projectA.id, projectB.id] } }),
        );

        expect(results.map((project) => project.id).sort()).toEqual(
          [projectA.id, projectB.id].sort(),
        );
      });

      it("returns no rows for an empty in list", async () => {
        insertProject(db, "Project A");

        const results = await readAll(app.projects.where({ id: { in: [] } }));

        expect(results).toEqual([]);
      });

      it("filters enum columns", async () => {
        const { value: rowA } = db.insert(app.table_with_defaults, { enum: "a" });
        const { value: rowB } = db.insert(app.table_with_defaults, { enum: "b" });
        db.insert(app.table_with_defaults, { enum: "c" });

        const results = await readAll(app.table_with_defaults.where({ enum: { in: ["a", "b"] } }));

        expect(results.map((row) => row.id).sort()).toEqual([rowA.id, rowB.id].sort());
      });

      it("filters reference columns", async () => {
        const projectA = insertProject(db, "Project A");
        const projectB = insertProject(db, "Project B");
        const projectC = insertProject(db, "Project C");
        const todoA = insertTodo(db, { title: "A", projectId: projectA.id });
        const todoB = insertTodo(db, { title: "B", projectId: projectB.id });
        const _todoC = insertTodo(db, { title: "C", projectId: projectC.id });

        const results = await readAll(
          app.todos.where({ projectId: { in: [projectA.id, projectB.id] } }),
        );

        expect(results.map((todo) => todo.id).sort()).toEqual([todoA.id, todoB.id].sort());
      });

      it("filters nullable reference columns", async () => {
        const owner = insertUser(db, "Owner");
        const todoWithOwner = insertTodo(db, { title: "Owned", ownerId: owner.id });
        const _todoWithoutOwner = insertTodo(db, { title: "Unowned", ownerId: null });

        const results = await readAll(app.todos.where({ ownerId: { in: [owner.id] } }));

        expect(results.map((todo) => todo.id)).toEqual([todoWithOwner.id]);
      });

      it("filters string columns", async () => {
        const todoA = insertTodo(db, { title: "Buy milk" });
        const todoB = insertTodo(db, { title: "Walk dog" });
        const _todoC = insertTodo(db, { title: "Write code" });

        const results = await readAll(app.todos.where({ title: { in: ["Buy milk", "Walk dog"] } }));

        expect(results.map((todo) => todo.id).sort()).toEqual([todoA.id, todoB.id].sort());
      });

      it("filters boolean columns", async () => {
        const { value: rowA } = db.insert(app.table_with_defaults, { boolean: true });
        db.insert(app.table_with_defaults, { boolean: false });

        const results = await readAll(app.table_with_defaults.where({ boolean: { in: [true] } }));

        expect(results.map((row) => row.id)).toEqual([rowA.id]);
      });

      it("filters numeric columns", async () => {
        const { value: rowA } = db.insert(app.table_with_defaults, { integer: 5, float: 1.5 });
        const { value: rowB } = db.insert(app.table_with_defaults, { integer: 10, float: 2.5 });
        db.insert(app.table_with_defaults, { integer: 15, float: 3.5 });

        const results = await readAll(
          app.table_with_defaults.where({ integer: { in: [5, 10] }, float: { in: [1.5, 2.5] } }),
        );

        expect(results.map((row) => row.id).sort()).toEqual([rowA.id, rowB.id].sort());
      });

      it("filters timestamp columns", async () => {
        const first = new Date("2026-01-01T00:00:00.000Z");
        const second = new Date("2026-01-02T00:00:00.000Z");
        const third = new Date("2026-01-03T00:00:00.000Z");
        const { value: rowA } = db.insert(app.table_with_defaults, { timestampDate: first });
        const { value: rowB } = db.insert(app.table_with_defaults, { timestampDate: second });
        db.insert(app.table_with_defaults, { timestampDate: third });

        const results = await readAll(
          app.table_with_defaults.where({ timestampDate: { in: [first, second] } }),
        );

        expect(results.map((row) => row.id).sort()).toEqual([rowA.id, rowB.id].sort());
      });

      it("filters byte-array columns", async () => {
        const { value: rowA } = db.insert(app.table_with_defaults, {
          bytes: new Uint8Array([1, 2, 3]),
        });
        db.insert(app.table_with_defaults, { bytes: new Uint8Array([4, 5, 6]) });

        const results = await readAll(
          app.table_with_defaults.where({ bytes: { in: [new Uint8Array([1, 2, 3])] } }),
        );

        expect(results.map((row) => row.id)).toEqual([rowA.id]);
      });

      it("filters array columns as whole-array equality", async () => {
        const { value: rowA } = db.insert(app.table_with_defaults, { array: ["a", "b"] });
        db.insert(app.table_with_defaults, { array: ["a"] });
        db.insert(app.table_with_defaults, { array: ["b", "a"] });

        const results = await readAll(
          app.table_with_defaults.where({ array: { in: [["a", "b"]] } }),
        );

        expect(results.map((row) => row.id)).toEqual([rowA.id]);
      });
    });

    describe("notIn operator", () => {
      it("excludes listed values, retains nullable values under two-valued ne semantics, and treats an empty list as true", async () => {
        const owner = insertUser(db, "Included");
        const excluded = insertTodo(db, { title: "Excluded", ownerId: owner.id });
        const retained = insertTodo(db, { title: "Retained", ownerId: null });

        const results = await readAll(app.todos.where({ ownerId: { notIn: [owner.id] } }));
        expect(results.map((todo) => todo.id)).toContain(retained.id);
        expect(results.map((todo) => todo.id)).not.toContain(excluded.id);

        const all = await readAll(app.todos.where({ id: { notIn: [] } }));
        expect(all.map((todo) => todo.id)).toEqual([excluded.id, retained.id].sort());
      });
    });

    it("filters int columns with multiple range operators on the same column", async () => {
      db.insert(app.table_with_defaults, { integer: 5 });
      const { value: aliceTask } = db.insert(app.table_with_defaults, { integer: 10 });
      const { value: bobTask } = db.insert(app.table_with_defaults, { integer: 15 });
      db.insert(app.table_with_defaults, { integer: 20 });

      const results = await readAll(
        app.table_with_defaults
          .where({ integer: { gt: 5, lt: 20 } })
          .select("integer")
          .orderBy("integer", "asc"),
      );

      expect(results).toEqual([
        { id: aliceTask.id, integer: 10 },
        { id: bobTask.id, integer: 15 },
      ]);
    });

    it("filters nullable int columns with range and not-null predicates", async () => {
      db.insert(app.table_with_defaults, { nullableInteger: null });
      const { value: aliceTask } = db.insert(app.table_with_defaults, { nullableInteger: 5 });
      db.insert(app.table_with_defaults, { nullableInteger: 10 });
      db.insert(app.table_with_defaults, { nullableInteger: 15 });

      const results = await readAll(
        app.table_with_defaults
          .where({ nullableInteger: { lt: 10, ne: null } })
          .select("nullableInteger")
          .orderBy("nullableInteger", "asc"),
      );

      expect(results).toEqual([{ id: aliceTask.id, nullableInteger: 5 }]);
    });

    it("filters nullable int columns with range and null equality predicates", async () => {
      db.insert(app.table_with_defaults, { nullableInteger: null });
      db.insert(app.table_with_defaults, { nullableInteger: 5 });
      db.insert(app.table_with_defaults, { nullableInteger: 10 });
      db.insert(app.table_with_defaults, { nullableInteger: 15 });

      const results = await readAll(
        app.table_with_defaults
          .where({ nullableInteger: { lt: 10, eq: null } })
          .select("nullableInteger")
          .orderBy("nullableInteger", "asc"),
      );

      expect(results).toEqual([]);
    });

    it("filters float columns with multiple range operators on the same column", async () => {
      db.insert(app.table_with_defaults, { float: 1.5 });
      const { value: aliceTask } = db.insert(app.table_with_defaults, { float: 2.5 });
      const { value: bobTask } = db.insert(app.table_with_defaults, { float: 3.5 });
      db.insert(app.table_with_defaults, { float: 4.5 });

      const results = await readAll(
        app.table_with_defaults
          .where({ float: { gt: 1.5, lt: 4.5 } })
          .select("float")
          .orderBy("float", "asc"),
      );

      expect(results).toEqual([
        { id: aliceTask.id, float: 2.5 },
        { id: bobTask.id, float: 3.5 },
      ]);
    });

    it("filters timestamp columns with multiple range operators on the same column", async () => {
      const lowerBound = new Date("2026-02-01T00:00:00.000Z");
      const aliceDueAt = new Date("2026-02-02T00:00:00.000Z");
      const bobDueAt = new Date("2026-02-03T00:00:00.000Z");
      const upperBound = new Date("2026-02-04T00:00:00.000Z");

      db.insert(app.table_with_defaults, { timestampDate: lowerBound });
      const { value: aliceTask } = db.insert(app.table_with_defaults, {
        timestampDate: aliceDueAt,
      });
      const { value: bobTask } = db.insert(app.table_with_defaults, { timestampDate: bobDueAt });
      db.insert(app.table_with_defaults, { timestampDate: upperBound });

      const results = await readAll(
        app.table_with_defaults
          .where({ timestampDate: { gt: lowerBound, lt: upperBound } })
          .select("timestampDate")
          .orderBy("timestampDate", "asc"),
      );

      expect(results).toEqual([
        { id: aliceTask.id, timestampDate: aliceDueAt },
        { id: bobTask.id, timestampDate: bobDueAt },
      ]);
    });

    it("filters same-column numeric bounds by the full predicate after index scanning", async () => {
      const { value: aliceTask } = db.insert(app.table_with_defaults, { integer: 5 });
      const { value: daveTask } = db.insert(app.table_with_defaults, { integer: 10 });
      const { value: bobTask } = db.insert(app.table_with_defaults, { integer: 15 });
      const { value: carolTask } = db.insert(app.table_with_defaults, { integer: 20 });

      const duplicateLowerBoundResults = await readAll(
        app.table_with_defaults
          .where({ integer: { gt: 10, gte: 5 } })
          .select("integer")
          .orderBy("integer", "asc"),
      );

      expect(duplicateLowerBoundResults).toEqual([
        { id: bobTask.id, integer: 15 },
        { id: carolTask.id, integer: 20 },
      ]);

      const duplicateUpperBoundResults = await readAll(
        app.table_with_defaults
          .where({ integer: { lt: 20, lte: 15 } })
          .select("integer")
          .orderBy("integer", "asc"),
      );

      expect(duplicateUpperBoundResults).toEqual([
        { id: aliceTask.id, integer: 5 },
        { id: daveTask.id, integer: 10 },
        { id: bobTask.id, integer: 15 },
      ]);

      const eqInsideRangeResults = await readAll(
        app.table_with_defaults
          .where({ integer: { eq: 15, gte: 5, lt: 20 } })
          .select("integer")
          .orderBy("integer", "asc"),
      );

      expect(eqInsideRangeResults).toEqual([{ id: bobTask.id, integer: 15 }]);

      const impossibleRangeResults = await readAll(
        app.table_with_defaults
          .where({ integer: { eq: 10, lt: 10 } })
          .select("integer")
          .orderBy("integer", "asc"),
      );

      expect(impossibleRangeResults).toEqual([]);
    });

    describe("query by array column", () => {
      it("using eq", async () => {
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

        const todosWithTags = await readAll(app.todos.where({ tags: { eq: ["tag1"] } }));
        expect(todosWithTags.length).toBe(1);
        expect(todosWithTags[0]!.id).toEqual(id1);
      });

      it("using contains", async () => {
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

        const todosWithTags = await readAll(app.todos.where({ tags: { contains: "tag1" } }));
        expect(todosWithTags.length).toBe(2);
        expect(todosWithTags).toContainEqual(expect.objectContaining({ id: id1 }));
        expect(todosWithTags).toContainEqual(expect.objectContaining({ id: id3 }));
      });
    });
  });

  describe("include", () => {
    it("filters forward and reverse included relations with in without filtering the parent", async () => {
      const matchingProject = insertProject(db, "Matching");
      const otherProject = insertProject(db, "Other");
      const todo = insertTodo(db, { projectId: otherProject.id, title: "Task" });
      const matchingTodo = insertTodo(db, {
        projectId: matchingProject.id,
        title: "Matching task",
      });
      insertTodo(db, { projectId: matchingProject.id, title: "Other task" });

      const forward = await readOne(
        app.todos
          .where({ id: { eq: todo.id } })
          .include({ project: app.projects.where({ id: { in: [matchingProject.id] } }) }),
      );
      assert(forward, "Todo is not defined");
      expect(forward.project).toBeNull();

      const emptyForward = await readOne(
        app.todos
          .where({ id: { eq: todo.id } })
          .include({ project: app.projects.where({ id: { in: [] } }) }),
      );
      assert(emptyForward, "Todo is not defined");
      expect(emptyForward.project).toBeNull();

      const reverse = await readOne(
        app.projects
          .where({ id: { eq: matchingProject.id } })
          .include({ todosViaProject: app.todos.where({ id: { in: [matchingTodo.id] } }) }),
      );
      assert(reverse, "Project is not defined");
      expect(reverse.todosViaProject.map((child) => child.id)).toEqual([matchingTodo.id]);

      const emptyReverse = await readOne(
        app.projects
          .where({ id: { eq: matchingProject.id } })
          .include({ todosViaProject: app.todos.where({ id: { in: [] } }) }),
      );
      assert(emptyReverse, "Project is not defined");
      expect(emptyReverse.todosViaProject).toEqual([]);
    });

    it("keeps a parent when its included relation is empty unless requireIncludes is explicit", async () => {
      const includedProject = insertProject(db, "Included");
      const excludedProject = insertProject(db, "Excluded");
      const todo = insertTodo(db, { projectId: excludedProject.id, title: "Task" });

      const optional = await readOne(
        app.todos
          .where({ id: { eq: todo.id } })
          .include({ project: app.projects.where({ id: { in: [includedProject.id] } }) }),
      );
      expect(optional?.id).toBe(todo.id);
      expect(optional?.project).toBeNull();

      const required = await readOne(
        app.todos
          .where({ id: { eq: todo.id } })
          .include({ project: app.projects.where({ id: { in: [includedProject.id] } }) })
          .requireIncludes(),
      );
      expect(required).toBeNull();
    });

    it("updates a live included relation as an in filter starts matching", async () => {
      const project = insertProject(db, "Announcements");
      const todo = insertTodo(db, { projectId: project.id, title: "Draft" });
      let unsubscribe = () => {};
      let timeout: ReturnType<typeof setTimeout> | undefined;
      const matching = new Promise<void>((resolve, reject) => {
        timeout = setTimeout(() => {
          unsubscribe();
          reject(new Error("Timed out waiting for included in-filter update"));
        }, 10_000);
        unsubscribe = db.subscribe(
          app.projects
            .where({ id: { eq: project.id } })
            .select("id")
            .include({
              todosViaProject: app.todos.where({ title: { in: ["Published"] } }).select("id"),
            }),
          (rows) => {
            if (rows[0]?.todosViaProject.map((child) => child.id).includes(todo.id)) resolve();
          },
        );
      });

      await new Promise((resolve) => setTimeout(resolve, 0));
      db.update(app.todos, todo.id, { title: "Published" });
      await matching;
      if (timeout) clearTimeout(timeout);
      unsubscribe();
    });

    it("updates a live reverse include as a notIn filter changes from excluded to included", async () => {
      const project = insertProject(db, "Announcements");
      const todo = insertTodo(db, { projectId: project.id, title: "Blocked" });
      let unsubscribe = () => {};
      let timeout: ReturnType<typeof setTimeout> | undefined;
      let resolveInitial: (() => void) | undefined;
      let rejectInitial: ((error: Error) => void) | undefined;
      const initial = new Promise<void>((resolve, reject) => {
        resolveInitial = resolve;
        rejectInitial = reject;
      });
      const included = new Promise<void>((resolve, reject) => {
        timeout = setTimeout(() => {
          unsubscribe();
          const error = new Error("Timed out waiting for included notIn-filter update");
          rejectInitial?.(error);
          reject(error);
        }, 10_000);
        unsubscribe = db.subscribe(
          app.projects
            .where({ id: { eq: project.id } })
            .select("id")
            .include({
              todosViaProject: app.todos.where({ title: { notIn: ["Blocked"] } }).select("id"),
            }),
          (rows) => {
            if (rows[0]?.todosViaProject.length === 0) resolveInitial?.();
            if (rows[0]?.todosViaProject.map((child) => child.id).includes(todo.id)) resolve();
          },
        );
      });

      await initial;
      db.update(app.todos, todo.id, { title: "Published" });
      await included;
      if (timeout) clearTimeout(timeout);
      unsubscribe();
    });

    it("include returns the related entity", async () => {
      const { id: projectId } = insertProject(db, "Announcements");
      const { id: ownerId } = insertUser(db);
      const { id: todoId } = insertTodo(db, {
        title: "Write tests",
        projectId: projectId,
        ownerId: ownerId,
      });

      const results = await readAll(
        app.todos.where({ id: { eq: todoId } }).include({ project: true }),
      );

      expect(results.length).toBe(1);
      const todo = results[0]!;
      expect(todo.title).toBe("Write tests");
      expectTypeOf(todo.ownerId).toEqualTypeOf<string | null>();
      expect(todo.ownerId).toBe(ownerId);
      expectTypeOf(todo.project).toEqualTypeOf<Project | null>();
      expect(todo.project?.name).toBe("Announcements");
    });

    it("include only resolves the provided columns, not all references", async () => {
      const { id: projectId } = insertProject(db, "Announcements");
      const { id: ownerId } = insertUser(db);
      const { id: todoId } = insertTodo(db, {
        projectId: projectId,
        ownerId: ownerId,
      });

      const result = await readOne(
        app.todos
          .select("ownerId")
          .where({ id: { eq: todoId } })
          .include({ project: true }),
      );

      assert(result, "Result is not defined");
      expectTypeOf(result.ownerId).toEqualTypeOf<string | null>();
      expect(result.ownerId).toBe(ownerId);
      expectTypeOf(result.project).toEqualTypeOf<Project | null>();
      assert(result.project, "Project include is not defined");
      expect(result.project.name).toBe("Announcements");
    });

    it("include returns null for null foreign key columns", async () => {
      const { id: todoId } = insertTodo(db, {
        ownerId: undefined,
      });

      const result = await readOne(
        app.todos.where({ id: { eq: todoId } }).include({ owner: true }),
      );

      assert(result, "Result is not defined");
      expectTypeOf(result.owner).toEqualTypeOf<User | null>();
      expect(result.owner).toBeNull();
    });

    it("text is not corrupted when using include", async () => {
      const { id: projectId } = insertProject(db);
      const { id: ownerId } = insertUser(db);
      const { id: todoId } = insertTodo(db, {
        title: "Hello world",
        tags: ["general"],
        projectId: projectId,
        ownerId: ownerId,
      });

      const baseline = await readAll(app.todos.where({ id: { eq: todoId } }));
      expect(baseline[0]!.title).toBe("Hello world");

      const withInclude = await readAll(
        app.todos.where({ id: { eq: todoId } }).include({ project: true }),
      );

      expect(withInclude.length).toBe(1);
      expect(withInclude[0]!.title).toBe("Hello world");
    });
  });

  describe("missing reference handling", () => {
    it("include returns null for missing scalar referenced entities", async () => {
      const project = insertProject(db);
      const todo = insertTodo(db, {
        projectId: project.id,
      });

      await db.delete(app.projects, project.id);

      const result = await readOne(
        app.todos.where({ id: { eq: todo.id } }).include({ project: true }),
      );
      assert(result, "Result is not defined");
      expectTypeOf(result.project).toEqualTypeOf<Project | null>();
      expect(result.project).toBeNull();
    });

    it("include skips missing referenced entities in forward array relations", async () => {
      const assignee1 = insertUser(db);
      const assignee2 = insertUser(db);
      const todo = insertTodo(db, {
        assigneesIds: [assignee1.id, assignee2.id],
      });

      await db.delete(app.users, assignee1.id);

      const result = await readOne(
        app.todos.where({ id: { eq: todo.id } }).include({ assignees: app.users.select("id") }),
      );
      assert(result, "Result is not defined");
      expectTypeOf(result.assignees).branded.toEqualTypeOf<{ id: string }[]>();
      expect(result.assignees).toEqual([{ id: assignee2.id }]);
    });

    it("include skips missing referenced entities in reverse relations", async () => {
      const owner = insertUser(db);
      const { id: todoId } = insertTodo(db, {
        ownerId: owner.id,
      });
      const { id: todoId2 } = insertTodo(db, {
        ownerId: owner.id,
      });

      await db.delete(app.todos, todoId);

      const result = await readOne(
        app.users
          .where({ id: { eq: owner.id } })
          .include({ todosViaOwner: app.todos.select("id") }),
      );
      assert(result, "Result is not defined");
      expectTypeOf(result.todosViaOwner).branded.toEqualTypeOf<{ id: string }[]>();
      expect(result.todosViaOwner).toEqual([{ id: todoId2 }]);
    });

    describe("requireIncludes", () => {
      it("requireIncludes filters out rows with missing scalar referenced entities", async () => {
        const project = insertProject(db);
        const todo = insertTodo(db, {
          projectId: project.id,
        });

        await db.delete(app.projects, project.id);

        const result = await readOne(
          app.todos
            .where({ id: { eq: todo.id } })
            .include({ project: true })
            .requireIncludes(),
        );

        expect(result).toBeNull();
        if (result) {
          expectTypeOf(result.project).toEqualTypeOf<Project>();
        }
      });

      it("requireIncludes does not filter out rows with null scalar references", async () => {
        const todo = insertTodo(db, {
          ownerId: undefined,
        });

        const result = await readOne(
          app.todos
            .where({ id: { eq: todo.id } })
            .include({ owner: true })
            .requireIncludes(),
        );

        assert(result, "Result is not defined");
        expect(result.id).toBe(todo.id);
        expectTypeOf(result.owner).toEqualTypeOf<User | null>();
        expect(result.owner).toBeNull();
      });

      it("requireIncludes filters out rows with missing entities in forward array relations", async () => {
        const assignee1 = insertUser(db);
        const assignee2 = insertUser(db);
        const todo = insertTodo(db, {
          assigneesIds: [assignee1.id, assignee2.id],
        });

        await db.delete(app.users, assignee1.id);

        const result = await readOne(
          app.todos
            .where({ id: { eq: todo.id } })
            .include({ assignees: app.users.select("id") })
            .requireIncludes(),
        );

        expect(result).toBeNull();
      });

      it("requireIncludes does not filter rows for reverse relations", async () => {
        const owner = insertUser(db);
        const { id: todoId } = insertTodo(db, {
          ownerId: owner.id,
        });
        const { id: todoId2 } = insertTodo(db, {
          ownerId: owner.id,
        });

        await db.delete(app.todos, todoId);

        const result = await readOne(
          app.users
            .where({ id: { eq: owner.id } })
            .include({ todosViaOwner: app.todos.select("id") })
            .requireIncludes(),
        );
        assert(result, "Result is not defined");
        expect(result.todosViaOwner).toEqual([{ id: todoId2 }]);
      });

      it("can use requireIncludes in nested includes", async () => {
        const alice = insertUser(db);
        const bob = insertUser(db);
        const deletedUser = insertUser(db);

        makeFriends(db, alice, bob);
        makeFriends(db, bob, deletedUser);

        db.delete(app.users, deletedUser.id);

        const result = await readOne(
          app.users.where({ id: { eq: alice.id } }).include({
            friends: app.users.include({ friends: true }).requireIncludes(),
          }),
        );

        assert(result, "Result is not defined");
        // Bob is not loaded because he's friends with a deleted user
        // But Alice can still be loaded, because we didn't use requireIncludes on the top-level include
        expect(result.friends).toHaveLength(0);
      });

      it("top-level requireIncludes does not affect inner includes", async () => {
        const alice = insertUser(db);
        const bob = insertUser(db);
        const deletedUser = insertUser(db);

        makeFriends(db, alice, bob);
        makeFriends(db, bob, deletedUser);

        db.delete(app.users, deletedUser.id);

        const result = await readOne(
          app.users
            .where({ id: { eq: alice.id } })
            .include({ friends: { friends: true } })
            .requireIncludes(),
        );

        assert(result, "Result is not defined");
        expect(result.friends.map((f) => f.id)).toEqual([bob.id]);
        const aliceFriend = result.friends[0];
        assert(aliceFriend, "Alice's friend is not defined");
        // requireIncludes only affects Alice. Bob's remaining friends still load.
        expect(aliceFriend.friends.map((f) => f.id)).toEqual([alice.id]);
      });

      it("rows skipped by requireIncludes affect limit-offset pagination", async () => {
        const alice = insertUser(db);
        const bob = insertUser(db);
        const deletedUser = insertUser(db);

        makeFriends(db, alice, bob);
        makeFriends(db, bob, deletedUser);

        const results = await readAll(
          app.users.include({ friends: true }).requireIncludes().limit(1).offset(1),
        );
        expect(results.map((u) => u.id)).toEqual([[alice.id, bob.id, deletedUser.id].sort()[1]]);

        await db.delete(app.users, deletedUser.id);

        const results2 = await readAll(
          app.users.include({ friends: true }).requireIncludes().limit(1).offset(1),
        );
        expect(results2).toHaveLength(0);
      });
    });
  });

  describe("select", () => {
    it("select narrows root columns while preserving id and includes", async () => {
      const { id: projectId } = insertProject(db, "Announcements");
      const { id: todoId } = insertTodo(db, {
        title: "Write tests",
        done: false,
        tags: ["dev"],
        projectId: projectId,
      });

      const result = await readOne(
        app.todos
          .select("title")
          .where({ id: { eq: todoId } })
          .include({ project: true }),
      );

      assert(result, "Result is not defined");
      expectTypeOf(result.id).toEqualTypeOf<string>();
      expectTypeOf(result.title).toEqualTypeOf<string>();
      expectTypeOf(result.project).toEqualTypeOf<Project | null>();
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

    it('select("*") resets to all root columns', async () => {
      const { id: projectId } = insertProject(db);
      const { id: ownerId } = insertUser(db);
      const { id: todoId } = insertTodo(db, {
        title: "Write tests",
        done: false,
        tags: ["dev"],
        projectId: projectId,
        ownerId: ownerId,
        assigneesIds: [],
      });

      const result = await readOne(app.todos.select("*").where({ id: { eq: todoId } }));

      assert(result, "Result is not defined");
      expectTypeOf(result).branded.toEqualTypeOf<Todo>();
      expect(result).toEqual({
        id: todoId,
        title: "Write tests",
        done: false,
        tags: ["dev"],
        projectId,
        ownerId,
        assigneesIds: [],
      });
    });

    it("selects and filters provenance magic timestamp columns as JS dates", async () => {
      const startedAt = Date.now();
      const { id: projectId } = insertProject(db, "Announcements");
      const { id: todoId } = insertTodo(db, {
        title: "Draft docs",
        done: false,
        tags: ["dev"],
        projectId,
        assigneesIds: [],
      });

      const projected = await readOne(
        app.todos.select("title", "$createdAt", "$updatedAt").where({ id: { eq: todoId } }),
      );

      assert(projected, "Result is not defined");
      expectTypeOf(projected.$createdAt).toEqualTypeOf<Date>();
      expectTypeOf(projected.$updatedAt).toEqualTypeOf<Date>();
      expect(projected.title).toBe("Draft docs");
      expect(projected.$createdAt).toBeInstanceOf(Date);
      expect(projected.$updatedAt).toBeInstanceOf(Date);
      expect(projected.$createdAt.getTime()).toBeGreaterThanOrEqual(startedAt - 60_000);
      expect(projected.$createdAt.getTime()).toBeLessThanOrEqual(Date.now() + 60_000);
      expect(projected.$updatedAt.getTime()).toBeGreaterThanOrEqual(startedAt - 60_000);
      expect(projected.$updatedAt.getTime()).toBeLessThanOrEqual(Date.now() + 60_000);

      const upperBound = new Date(Date.now() + 60_000);
      const withinUpperBound = await readAll(
        app.todos
          .where({ $updatedAt: { lte: upperBound } })
          .select("title", "$updatedAt")
          .orderBy("title", "asc"),
      );

      expect(withinUpperBound).toContainEqual(
        expect.objectContaining({
          id: todoId,
          title: "Draft docs",
          $updatedAt: projected.$updatedAt,
        }),
      );
    });

    it("include builders can project nested relation columns", async () => {
      const { id: projectId } = insertProject(db, "Announcements");
      const { id: ownerId } = insertUser(db);
      const { id: todoId } = insertTodo(db, {
        title: "Write tests",
        done: false,
        tags: ["dev"],
        projectId,
        ownerId,
        assigneesIds: [],
      });

      const result = await readOne(
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
      expect("done" in result.todosViaProject[0]!).toBe(false);
      expect("tags" in result.todosViaProject[0]!).toBe(false);
      expect("project" in result.todosViaProject[0]!).toBe(false);
    });

    it("include builders can project reverse relation magic timestamp columns", async () => {
      const startedAt = Date.now();
      const { id: projectId } = insertProject(db, "Announcements");
      const { id: todoId } = insertTodo(db, {
        title: "Write tests",
        done: false,
        tags: ["dev"],
        projectId,
        assigneesIds: [],
      });

      const result = await readOne(
        app.projects
          .where({ id: { eq: projectId } })
          .include({
            todosViaProject: app.todos.limit(1).select("*", "$createdAt", "$updatedAt"),
          })
          .requireIncludes(),
      );

      assert(result, "Result is not defined");
      expect(result.todosViaProject).toHaveLength(1);
      const todo = result.todosViaProject[0];
      assert(todo, "Included todo is not defined");
      expect(todo.id).toBe(todoId);
      expect(todo.title).toBe("Write tests");
      expect(todo.$createdAt).toBeInstanceOf(Date);
      expect(todo.$updatedAt).toBeInstanceOf(Date);
      expect(todo.$createdAt.getTime()).toBeGreaterThanOrEqual(startedAt - 60_000);
      expect(todo.$updatedAt.getTime()).toBeGreaterThanOrEqual(startedAt - 60_000);
    });

    it("include builders can project magic timestamp columns on nested array relations", async () => {
      const startedAt = Date.now();
      const { id: projectId } = insertProject(db, "Announcements");
      const { id: assigneeId } = insertUser(db, "Alice");
      const { id: todoId } = insertTodo(db, {
        title: "Write tests",
        done: false,
        tags: ["dev"],
        projectId,
        assigneesIds: [assigneeId],
      });

      const result = await readOne(
        app.projects
          .where({ id: { eq: projectId } })
          .include({
            todosViaProject: app.todos.select("title").include({
              assignees: app.users.select("name", "$createdAt", "$updatedAt"),
            }),
          })
          .requireIncludes(),
      );

      assert(result, "Result is not defined");
      expect(result.todosViaProject).toHaveLength(1);
      const todo = result.todosViaProject[0];
      assert(todo, "Included todo is not defined");
      expect(todo.id).toBe(todoId);
      expect(todo.assignees).toHaveLength(1);
      const assignee = todo.assignees[0];
      assert(assignee, "Nested assignee is not defined");
      expect(assignee.id).toBe(assigneeId);
      expect(assignee.name).toBe("Alice");
      expect(assignee.$createdAt).toBeInstanceOf(Date);
      expect(assignee.$updatedAt).toBeInstanceOf(Date);
      expect(assignee.$createdAt.getTime()).toBeGreaterThanOrEqual(startedAt - 60_000);
      expect(assignee.$updatedAt.getTime()).toBeGreaterThanOrEqual(startedAt - 60_000);
    });

    it("subscribe preserves projected root columns with includes", async () => {
      const { id: projectId } = insertProject(db, "Announcements");
      const { id: ownerId } = insertUser(db);

      type SubscribedTodo = {
        id: string;
        title: string;
        project: {
          id: string;
          name: string;
        } | null;
      };

      let unsubscribe = () => {};
      let timeout: ReturnType<typeof setTimeout> | undefined;
      const deltaPromise = new Promise<SubscribedTodo[]>((resolve, reject) => {
        timeout = setTimeout(() => {
          unsubscribe();
          reject(new Error("Timed out waiting for subscribe projection update"));
        }, 10_000);

        unsubscribe = db.subscribe(app.todos.select("title").include({ project: true }), (rows) => {
          if (rows.length !== 1) {
            return;
          }

          resolve(rows);
        });
      });

      await new Promise((resolve) => setTimeout(resolve, 0));

      const { id: todoId } = insertTodo(db, {
        title: "Watch subscription",
        done: false,
        tags: ["dev"],
        projectId,
        ownerId,
        assigneesIds: [],
      });

      const all = await deltaPromise;
      if (timeout) {
        clearTimeout(timeout);
      }
      unsubscribe();

      expect(all).toEqual([
        {
          id: todoId,
          title: "Watch subscription",
          project: {
            id: projectId,
            name: "Announcements",
          },
        },
      ]);
      assert(all[0]);
      expect("done" in all[0]).toBe(false);
      expect("tags" in all[0]).toBe(false);
    });

    it("subscribe returns null for selected nullable columns while omitting unselected columns", async () => {
      const { id: projectId } = insertProject(db, "Announcements");

      type SubscribedTodo = {
        id: string;
        title: string;
        ownerId: string | null;
      };

      let unsubscribe = () => {};
      let timeout: ReturnType<typeof setTimeout> | undefined;
      const deltaPromise = new Promise<SubscribedTodo[]>((resolve, reject) => {
        timeout = setTimeout(() => {
          unsubscribe();
          reject(new Error("Timed out waiting for subscribe nullable update"));
        }, 10_000);

        unsubscribe = db.subscribe(app.todos.select("title", "ownerId"), (rows) => {
          if (rows.length !== 1) {
            return;
          }
          resolve(rows);
        });
      });

      await new Promise((resolve) => setTimeout(resolve, 0));

      const { id: todoId } = insertTodo(db, {
        title: "Watch nullable subscription",
        done: false,
        tags: ["dev"],
        projectId,
        ownerId: null,
        assigneesIds: [],
      });

      const all = await deltaPromise;
      if (timeout) {
        clearTimeout(timeout);
      }
      unsubscribe();

      expect(all).toEqual([
        {
          id: todoId,
          title: "Watch nullable subscription",
          ownerId: null,
        },
      ]);
      assert(all[0]);
      expect("done" in all[0]).toBe(false);
      expect("tags" in all[0]).toBe(false);
    });
  });
});
