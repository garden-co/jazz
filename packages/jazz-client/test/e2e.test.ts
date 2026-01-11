/**
 * End-to-end tests for @jazz/client
 *
 * These tests use generated types from test/generated/ which are
 * generated from test/fixtures/app.sql before tests run.
 *
 * Run: pnpm test
 */

import { beforeAll, describe, expect, it } from "vitest";
// @ts-ignore - vite handles ?raw imports
import schema from "./fixtures/app.sql?raw";
import { type Database, createDatabase } from "./generated/client.js";

// Helper to subscribe and get first result
function subscribeOnce<T>(
  subscribe: (callback: (rows: T) => void) => () => void,
): Promise<T> {
  return new Promise((resolve) => {
    let unsubscribe: (() => void) | undefined;
    unsubscribe = subscribe((rows) => {
      // Unsubscribe async to avoid issues during callback
      setTimeout(() => {
        try {
          unsubscribe?.();
        } catch {
          // Ignore cleanup errors (database may already be freed)
        }
      }, 0);
      resolve(rows);
    });
  });
}

let db: Database;

beforeAll(async () => {
  // Dynamic import of the WASM module
  const wasm = await import("groove-wasm");
  await wasm.default();

  const wasmDb = new wasm.WasmDatabase();
  wasmDb.init_schema(schema);
  db = createDatabase(wasmDb);
});

// === CRUD Operations ===

describe("CRUD Operations", () => {
  describe("create", () => {
    it("creates a user with all required fields", async () => {
      const id = db.users.create({
        name: "Alice",
        email: "alice@test.com",
        age: BigInt(30),
        score: 95.5,
        isAdmin: true,
      });

      expect(typeof id).toBe("string");
      expect(id.length).toBeGreaterThan(0);

      const users = await subscribeOnce((cb) => db.users.subscribeAll(cb));
      const alice = users.find((u) => u.id === id);
      expect(alice).toBeDefined();
      expect(alice!.name).toBe("Alice");
      expect(alice!.email).toBe("alice@test.com");
      expect(alice!.age).toBe(BigInt(30));
      expect(alice!.score).toBe(95.5);
      expect(alice!.isAdmin).toBe(true);
      expect(alice!.avatar).toBeNull();
    });

    it("creates a user with optional nullable field", async () => {
      const id = db.users.create({
        name: "Bob",
        email: "bob@test.com",
        avatar: "https://example.com/bob.png",
        age: BigInt(25),
        score: 80.5, // Use non-integer to ensure F64 type
        isAdmin: false,
      });

      const users = await subscribeOnce((cb) => db.users.subscribeAll(cb));
      const bob = users.find((u) => u.id === id);
      expect(bob).toBeDefined();
      expect(bob!.avatar).toBe("https://example.com/bob.png");
    });

    it("creates a project with forward reference", async () => {
      const userId = db.users.create({
        name: "Owner",
        email: "owner@test.com",
        age: BigInt(40),
        score: 100.5, // Use non-integer to ensure F64 type
        isAdmin: true,
      });

      const projectId = db.projects.create({
        name: "Test Project",
        owner: userId,
        color: "#ff0000",
      });

      const projects = await subscribeOnce((cb) =>
        db.projects.subscribeAll(cb),
      );
      const project = projects.find((p) => p.id === projectId);
      expect(project).toBeDefined();
      expect(project!.name).toBe("Test Project");
      expect(project!.owner).toBe(userId); // FK is string without include
    });
  });

  describe("update", () => {
    it("updates a single field", async () => {
      const id = db.users.create({
        name: "UpdateMe",
        email: "update@test.com",
        age: BigInt(20),
        score: 50.5, // Use non-integer to ensure F64 type
        isAdmin: false,
      });

      db.users.update(id, { name: "Updated" });

      const users = await subscribeOnce((cb) => db.users.subscribeAll(cb));
      const user = users.find((u) => u.id === id);
      expect(user!.name).toBe("Updated");
      expect(user!.email).toBe("update@test.com"); // unchanged
    });

    it("updates multiple fields", async () => {
      const id = db.users.create({
        name: "MultiUpdate",
        email: "multi@test.com",
        age: BigInt(20),
        score: 50.5, // Use non-integer to ensure F64 type
        isAdmin: false,
      });

      db.users.update(id, {
        name: "MultiUpdated",
        score: 99.9,
        isAdmin: true,
      });

      const users = await subscribeOnce((cb) => db.users.subscribeAll(cb));
      const user = users.find((u) => u.id === id);
      expect(user!.name).toBe("MultiUpdated");
      expect(user!.score).toBe(99.9);
      expect(user!.isAdmin).toBe(true);
    });

    it("updates nullable field to value", async () => {
      const id = db.users.create({
        name: "NoAvatar",
        email: "noavatar@test.com",
        age: BigInt(20),
        score: 50.5, // Use non-integer to ensure F64 type
        isAdmin: false,
      });

      db.users.update(id, { avatar: "https://example.com/avatar.png" });

      const users = await subscribeOnce((cb) => db.users.subscribeAll(cb));
      const user = users.find((u) => u.id === id);
      expect(user!.avatar).toBe("https://example.com/avatar.png");
    });
  });

  describe("delete", () => {
    it("deletes a row", async () => {
      const id = db.users.create({
        name: "DeleteMe",
        email: "delete@test.com",
        age: BigInt(20),
        score: 50.5, // Use non-integer to ensure F64 type
        isAdmin: false,
      });

      // Verify it exists
      let users = await subscribeOnce((cb) => db.users.subscribeAll(cb));
      expect(users.some((u) => u.id === id)).toBe(true);

      // Delete it
      db.users.delete(id);

      // Verify it's gone
      users = await subscribeOnce((cb) => db.users.subscribeAll(cb));
      expect(users.some((u) => u.id === id)).toBe(false);
    });
  });
});

// === Subscribe Operations ===

describe("Subscribe Operations", () => {
  let testUserId: string;
  let testProjectId: string;
  let testTaskId: string;
  let testTagId: string;

  beforeAll(() => {
    // Create test data for subscribe tests
    testUserId = db.users.create({
      name: "SubscribeTestUser",
      email: "subscribe@test.com",
      age: BigInt(35),
      score: 88.8,
      isAdmin: false,
    });

    testProjectId = db.projects.create({
      name: "SubscribeTestProject",
      description: "A project for testing",
      owner: testUserId,
      color: "#00ff00",
    });

    testTaskId = db.tasks.create({
      title: "SubscribeTestTask",
      description: "A task for testing",
      status: "open",
      priority: "high",
      project: testProjectId,
      assignee: testUserId,
      createdAt: BigInt(Date.now()),
      updatedAt: BigInt(Date.now()),
      isCompleted: false,
    });

    testTagId = db.tags.create({
      name: "SubscribeTestTag",
      color: "#0000ff",
    });

    db.tasktags.create({
      task: testTaskId,
      tag: testTagId,
    });
  });

  describe("subscribeAll without includes", () => {
    it("returns plain rows with FK as strings", async () => {
      const tasks = await subscribeOnce((cb) => db.tasks.subscribeAll(cb));
      const task = tasks.find((t) => t.id === testTaskId);
      expect(task).toBeDefined();
      expect(task!.title).toBe("SubscribeTestTask");
      expect(typeof task!.project).toBe("string"); // FK as string
      expect(task!.project).toBe(testProjectId);
    });
  });

  describe("subscribeAll with forward ref include", () => {
    it("resolves single forward ref", async () => {
      const tasks = await subscribeOnce((cb) =>
        db.tasks.with({ project: true }).subscribeAll(cb),
      );
      const task = tasks.find((t) => t.id === testTaskId);
      expect(task).toBeDefined();
      expect(typeof task!.project).toBe("object");
      expect(task!.project.name).toBe("SubscribeTestProject");
      expect(task!.project.color).toBe("#00ff00");
    });

    it("resolves nullable forward ref", async () => {
      const tasks = await subscribeOnce((cb) =>
        db.tasks.with({ assignee: true }).subscribeAll(cb),
      );
      const task = tasks.find((t) => t.id === testTaskId);
      expect(task).toBeDefined();
      expect(typeof task!.assignee).toBe("object");
      expect(task!.assignee!.name).toBe("SubscribeTestUser");
    });

    // Skip: Groove doesn't support multiple JOINed tables in binary output yet
    it.skip("resolves multiple forward refs", async () => {
      const tasks = await subscribeOnce((cb) =>
        db.tasks.with({ project: true, assignee: true }).subscribeAll(cb),
      );
      const task = tasks.find((t) => t.id === testTaskId);
      expect(task).toBeDefined();
      expect(task!.project.name).toBe("SubscribeTestProject");
      expect(task!.assignee!.name).toBe("SubscribeTestUser");
    });
  });

  describe("subscribeAll with reverse ref include", () => {
    it("resolves reverse ref as array", async () => {
      const projects = await subscribeOnce((cb) =>
        db.projects.with({ Tasks: true }).subscribeAll(cb),
      );
      const project = projects.find((p) => p.id === testProjectId);
      expect(project).toBeDefined();
      expect(Array.isArray(project!.Tasks)).toBe(true);
      expect(project!.Tasks.length).toBeGreaterThanOrEqual(1);
      const task = project!.Tasks.find((t: any) => t.id === testTaskId);
      expect(task).toBeDefined();
      expect(task.title).toBe("SubscribeTestTask");
    });

    it("resolves user's tasks via reverse ref", async () => {
      const users = await subscribeOnce((cb) =>
        db.users.with({ Projects: true }).subscribeAll(cb),
      );
      const user = users.find((u) => u.id === testUserId);
      expect(user).toBeDefined();
      expect(Array.isArray(user!.Projects)).toBe(true);
      const project = user!.Projects.find((p: any) => p.id === testProjectId);
      expect(project).toBeDefined();
      expect(project.name).toBe("SubscribeTestProject");
    });
  });

  describe("subscribeAll with junction table include", () => {
    it("resolves junction table entries", async () => {
      const tasks = await subscribeOnce((cb) =>
        db.tasks.with({ TaskTags: true }).subscribeAll(cb),
      );
      const task = tasks.find((t) => t.id === testTaskId);
      expect(task).toBeDefined();
      expect(Array.isArray(task!.TaskTags)).toBe(true);
      expect(task!.TaskTags.length).toBeGreaterThanOrEqual(1);
    });

    it("resolves nested refs within junction table", async () => {
      const tasks = await subscribeOnce((cb) =>
        db.tasks.with({ TaskTags: { tag: true } }).subscribeAll(cb),
      );
      const task = tasks.find((t) => t.id === testTaskId);
      expect(task).toBeDefined();
      expect(Array.isArray(task!.TaskTags)).toBe(true);
      const taskTag = task!.TaskTags[0];
      expect(typeof taskTag.tag).toBe("object");
      expect(taskTag.tag.name).toBe("SubscribeTestTag");
      expect(taskTag.tag.color).toBe("#0000ff");
    });
  });

  describe("subscribeAll with mixed includes", () => {
    it("resolves forward refs and reverse refs together", async () => {
      const tasks = await subscribeOnce((cb) =>
        db.tasks
          .with({
            project: true,
            assignee: true,
            TaskTags: { tag: true },
          })
          .subscribeAll(cb),
      );
      const task = tasks.find((t) => t.id === testTaskId);
      expect(task).toBeDefined();
      expect(task!.project.name).toBe("SubscribeTestProject");
      expect(task!.assignee!.name).toBe("SubscribeTestUser");
      expect(task!.TaskTags[0].tag.name).toBe("SubscribeTestTag");
    });
  });
});

// === Filter Operations ===

describe("Filter Operations", () => {
  beforeAll(() => {
    // Create test data for filter tests
    db.users.create({
      name: "FilterUser1",
      email: "filter1@test.com",
      age: BigInt(25),
      score: 75.5, // Use non-integer to ensure F64 type
      isAdmin: false,
    });
    db.users.create({
      name: "FilterUser2",
      email: "filter2@test.com",
      age: BigInt(30),
      score: 85.5, // Use non-integer to ensure F64 type
      isAdmin: true,
    });
    db.users.create({
      name: "FilterUser3",
      email: "filter3@test.com",
      age: BigInt(35),
      score: 95.5, // Use non-integer to ensure F64 type
      isAdmin: false,
    });
  });

  describe("equality filters", () => {
    it("filters by exact string value", async () => {
      const users = await subscribeOnce((cb) =>
        db.users.where({ name: "FilterUser1" }).subscribeAll(cb),
      );
      expect(users.length).toBe(1);
      expect(users[0].name).toBe("FilterUser1");
    });

    it("filters by exact boolean value", async () => {
      const users = await subscribeOnce((cb) =>
        db.users.where({ isAdmin: true }).subscribeAll(cb),
      );
      expect(users.length).toBeGreaterThanOrEqual(1);
      expect(users.every((u) => u.isAdmin === true)).toBe(true);
    });

    it("filters by bigint value", async () => {
      const users = await subscribeOnce((cb) =>
        db.users.where({ age: BigInt(30) }).subscribeAll(cb),
      );
      expect(users.length).toBeGreaterThanOrEqual(1);
      expect(users.every((u) => u.age === BigInt(30))).toBe(true);
    });
  });

  // Skip: Groove parser doesn't support LIKE operator yet
  describe.skip("string filters", () => {
    it("filters by contains", async () => {
      const users = await subscribeOnce((cb) =>
        db.users.where({ name: { contains: "FilterUser" } }).subscribeAll(cb),
      );
      expect(users.length).toBeGreaterThanOrEqual(3);
      expect(users.every((u) => u.name.includes("FilterUser"))).toBe(true);
    });

    it("filters by contains (startsWith not reliable in Groove)", async () => {
      const users = await subscribeOnce((cb) =>
        db.users.where({ name: { contains: "FilterUser" } }).subscribeAll(cb),
      );
      expect(users.length).toBeGreaterThanOrEqual(3);
      expect(users.every((u) => u.name.includes("FilterUser"))).toBe(true);
    });

    it("filters by endsWith", async () => {
      const users = await subscribeOnce((cb) =>
        db.users.where({ email: { endsWith: "@test.com" } }).subscribeAll(cb),
      );
      expect(users.length).toBeGreaterThanOrEqual(1);
      expect(users.every((u) => u.email.endsWith("@test.com"))).toBe(true);
    });
  });

  // Skip: Groove parser doesn't support comparison operators (>, >=, <, <=) yet
  describe.skip("comparison filters", () => {
    it("filters by gt (greater than)", async () => {
      const users = await subscribeOnce((cb) =>
        db.users.where({ score: { gt: 80.0 } }).subscribeAll(cb),
      );
      expect(users.length).toBeGreaterThanOrEqual(2);
      expect(users.every((u) => u.score > 80.0)).toBe(true);
    });

    it("filters by gte (greater than or equal)", async () => {
      const users = await subscribeOnce((cb) =>
        db.users.where({ score: { gte: 85.0 } }).subscribeAll(cb),
      );
      expect(users.length).toBeGreaterThanOrEqual(2);
      expect(users.every((u) => u.score >= 85.0)).toBe(true);
    });

    it("filters by lt (less than)", async () => {
      const users = await subscribeOnce((cb) =>
        db.users.where({ score: { lt: 80.0 } }).subscribeAll(cb),
      );
      expect(users.length).toBeGreaterThanOrEqual(1);
      expect(users.every((u) => u.score < 80.0)).toBe(true);
    });

    it("filters by lte (less than or equal)", async () => {
      const users = await subscribeOnce((cb) =>
        db.users.where({ score: { lte: 75.0 } }).subscribeAll(cb),
      );
      expect(users.length).toBeGreaterThanOrEqual(1);
      expect(users.every((u) => u.score <= 75.0)).toBe(true);
    });

    it("filters by range (gte + lt)", async () => {
      const users = await subscribeOnce((cb) =>
        db.users.where({ score: { gte: 75.0, lt: 90.0 } }).subscribeAll(cb),
      );
      expect(users.length).toBeGreaterThanOrEqual(2);
      expect(users.every((u) => u.score >= 75.0 && u.score < 90.0)).toBe(true);
    });
  });

  // Skip: Groove parser doesn't support IN operator yet
  describe.skip("IN filters", () => {
    it("filters by in array", async () => {
      const users = await subscribeOnce((cb) =>
        db.users
          .where({ name: { in: ["FilterUser1", "FilterUser3"] } })
          .subscribeAll(cb),
      );
      expect(users.length).toBe(2);
      expect(users.map((u) => u.name).sort()).toEqual([
        "FilterUser1",
        "FilterUser3",
      ]);
    });

    it("filters by notIn array", async () => {
      const usersAll = await subscribeOnce((cb) =>
        db.users.where({ name: { contains: "FilterUser" } }).subscribeAll(cb),
      );
      const usersFiltered = await subscribeOnce((cb) =>
        db.users
          .where({
            name: { contains: "FilterUser" },
            AND: [{ name: { notIn: ["FilterUser1"] } }],
          })
          .subscribeAll(cb),
      );
      expect(usersFiltered.every((u) => u.name !== "FilterUser1")).toBe(true);
    });
  });

  // Skip: Groove parser doesn't support != operator yet
  describe.skip("NOT filter", () => {
    it("negates condition with not", async () => {
      const users = await subscribeOnce((cb) =>
        db.users.where({ name: { not: "FilterUser1" } }).subscribeAll(cb),
      );
      expect(users.every((u) => u.name !== "FilterUser1")).toBe(true);
    });
  });

  // Skip: Groove parser doesn't support LIKE, >=, OR, NOT operators yet
  describe.skip("combinators", () => {
    it("combines with AND", async () => {
      const users = await subscribeOnce((cb) =>
        db.users
          .where({
            AND: [
              { name: { contains: "FilterUser" } },
              { score: { gte: 80.0 } },
            ],
          })
          .subscribeAll(cb),
      );
      expect(users.length).toBeGreaterThanOrEqual(2);
      expect(
        users.every((u) => u.name.includes("FilterUser") && u.score >= 80.0),
      ).toBe(true);
    });

    it("combines with OR", async () => {
      const users = await subscribeOnce((cb) =>
        db.users
          .where({
            OR: [{ name: "FilterUser1" }, { name: "FilterUser3" }],
          })
          .subscribeAll(cb),
      );
      expect(users.length).toBe(2);
    });

    it("combines with NOT", async () => {
      const users = await subscribeOnce((cb) =>
        db.users
          .where({
            name: { contains: "FilterUser" },
            NOT: { isAdmin: true },
          })
          .subscribeAll(cb),
      );
      expect(users.every((u) => !u.isAdmin)).toBe(true);
    });
  });

  describe("filter with includes", () => {
    it("combines where and with", async () => {
      // First create a project for the filter test
      const ownerForFilter = db.users.create({
        name: "FilterOwner",
        email: "filterowner@test.com",
        age: BigInt(40),
        score: 100.5, // Use non-integer to ensure F64 type
        isAdmin: true,
      });

      db.projects.create({
        name: "FilterProject",
        owner: ownerForFilter,
        color: "#123456",
      });

      const projects = await subscribeOnce((cb) =>
        db.projects
          .where({ name: "FilterProject" })
          .with({ owner: true })
          .subscribeAll(cb),
      );
      expect(projects.length).toBe(1);
      expect(projects[0].name).toBe("FilterProject");
      expect(projects[0].owner.name).toBe("FilterOwner");
    });
  });
});

// === Self-referential Tables ===
// NOTE: Self-referential tables with nullable parent refs have known issues
// with ID decoding. Skipping these tests until the issue is fixed in Groove.

describe("Self-referential Tables", () => {
  it("creates categories without parent references", async () => {
    // Test basic creation works
    const rootId = db.categories.create({ name: "RootOnly" });

    const categories = await subscribeOnce((cb) =>
      db.categories.subscribeAll(cb),
    );
    const root = categories.find((c) => c.id === rootId);

    expect(root).toBeDefined();
    expect(root!.name).toBe("RootOnly");
    expect(root!.parent).toBeNull();
  });

  // Skip: Parent refs in self-referential tables have decoding issues
  it.skip("creates categories with parent references", async () => {
    const rootId = db.categories.create({ name: "Root" });
    const childId = db.categories.create({ name: "Child", parent: rootId });
    const grandchildId = db.categories.create({
      name: "Grandchild",
      parent: childId,
    });

    const categories = await subscribeOnce((cb) =>
      db.categories.subscribeAll(cb),
    );
    const root = categories.find((c) => c.id === rootId);
    const child = categories.find((c) => c.id === childId);
    const grandchild = categories.find((c) => c.id === grandchildId);

    expect(root).toBeDefined();
    expect(root!.parent).toBeNull();
    expect(child).toBeDefined();
    expect(child!.parent).toBe(rootId);
    expect(grandchild).toBeDefined();
    expect(grandchild!.parent).toBe(childId);
  });

  // Skip: Parent include in self-referential tables has issues
  it.skip("includes parent category", async () => {
    const rootId = db.categories.create({ name: "IncludeRoot" });
    const childId = db.categories.create({
      name: "IncludeChild",
      parent: rootId,
    });

    const categories = await subscribeOnce((cb) =>
      db.categories
        .where({ name: "IncludeChild" })
        .with({ parent: true })
        .subscribeAll(cb),
    );
    expect(categories.length).toBe(1);
    expect(categories[0].name).toBe("IncludeChild");
    expect(typeof categories[0].parent).toBe("object");
    expect(categories[0].parent!.name).toBe("IncludeRoot");
  });

  // Skip: Reverse refs in self-referential tables have issues
  it.skip("includes child categories via reverse ref", async () => {
    const parentId = db.categories.create({ name: "ReverseParent" });
    db.categories.create({ name: "ReverseChild1", parent: parentId });
    db.categories.create({ name: "ReverseChild2", parent: parentId });

    const categories = await subscribeOnce((cb) =>
      db.categories
        .where({ name: "ReverseParent" })
        .with({ Categories: true })
        .subscribeAll(cb),
    );
    expect(categories.length).toBe(1);
    expect(categories[0].name).toBe("ReverseParent");
    expect(Array.isArray(categories[0].Categories)).toBe(true);
    expect(categories[0].Categories.length).toBe(2);
  });
});

// === Comments with Multiple Nullable Refs ===

describe("Comments with Multiple Nullable Refs", () => {
  let commentUserId: string;
  let commentTaskId: string;
  let commentProjectId: string;

  beforeAll(() => {
    commentUserId = db.users.create({
      name: "CommentUser",
      email: "comment@test.com",
      age: BigInt(30),
      score: 90.5, // Use non-integer to ensure F64 type
      isAdmin: false,
    });

    commentProjectId = db.projects.create({
      name: "CommentProject",
      owner: commentUserId,
      color: "#aabbcc",
    });

    commentTaskId = db.tasks.create({
      title: "CommentTask",
      status: "open",
      priority: "medium",
      project: commentProjectId,
      createdAt: BigInt(Date.now()),
      updatedAt: BigInt(Date.now()),
      isCompleted: false,
    });
  });

  it("creates comment with required author and optional task", async () => {
    const commentId = db.comments.create({
      content: "Test comment",
      author: commentUserId,
      task: commentTaskId,
      createdAt: BigInt(Date.now()),
    });

    const comments = await subscribeOnce((cb) => db.comments.subscribeAll(cb));
    const comment = comments.find((c) => c.id === commentId);
    expect(comment).toBeDefined();
    expect(comment!.content).toBe("Test comment");
    expect(comment!.author).toBe(commentUserId);
    expect(comment!.task).toBe(commentTaskId);
    expect(comment!.parentComment).toBeNull();
  });

  // Skip: When a nullable FK is NULL, Groove doesn't include the joined columns in the binary output.
  // The decoder expects joined columns to always be present for included refs.
  // Fixing this requires changes to either Groove (always include NULL join columns) or
  // making the decoder layout dynamic based on FK values.
  it.skip("creates nested comments with parentComment ref", async () => {
    const parentCommentId = db.comments.create({
      content: "Parent comment",
      author: commentUserId,
      createdAt: BigInt(Date.now()),
    });

    const childCommentId = db.comments.create({
      content: "Child comment",
      author: commentUserId,
      parentComment: parentCommentId,
      createdAt: BigInt(Date.now()),
    });

    const comments = await subscribeOnce((cb) =>
      db.comments.with({ parentComment: true }).subscribeAll(cb),
    );
    const childComment = comments.find((c) => c.id === childCommentId);
    expect(childComment).toBeDefined();
    expect(typeof childComment!.parentComment).toBe("object");
    expect(childComment!.parentComment!.content).toBe("Parent comment");
  });

  // Skip: Groove doesn't support multiple JOINed tables in binary output yet
  it.skip("includes all refs on comment", async () => {
    const taskCommentId = db.comments.create({
      content: "Full refs comment",
      author: commentUserId,
      task: commentTaskId,
      createdAt: BigInt(Date.now()),
    });

    const comments = await subscribeOnce((cb) =>
      db.comments
        .where({ id: taskCommentId })
        .with({ author: true, task: true })
        .subscribeAll(cb),
    );
    expect(comments.length).toBe(1);
    expect(comments[0].author.name).toBe("CommentUser");
    expect(comments[0].task!.title).toBe("CommentTask");
  });
});

// === Junction Table Filters (Relation Filters) ===
// These tests mirror the demo app's complex filtered queries with junction tables

describe("Junction Table Filters", () => {
  let filterTestUser1: string;
  let filterTestUser2: string;
  let filterTestProject: string;
  let filterTestTag1: string;
  let filterTestTag2: string;
  let filterTestTag3: string;
  let filterTestTask1: string; // Has tag1, assigned to user1
  let filterTestTask2: string; // Has tag1 and tag2, assigned to user2
  let filterTestTask3: string; // Has tag2 and tag3, no assignee
  let filterTestTask4: string; // No tags, assigned to user1

  beforeAll(() => {
    // Create test users
    filterTestUser1 = db.users.create({
      name: "JunctionFilterUser1",
      email: "jfilter1@test.com",
      age: BigInt(30),
      score: 80.5,
      isAdmin: false,
    });

    filterTestUser2 = db.users.create({
      name: "JunctionFilterUser2",
      email: "jfilter2@test.com",
      age: BigInt(35),
      score: 85.5,
      isAdmin: true,
    });

    // Create test project
    filterTestProject = db.projects.create({
      name: "JunctionFilterProject",
      owner: filterTestUser1,
      color: "#123456",
    });

    // Create test tags
    filterTestTag1 = db.tags.create({
      name: "JunctionTag1",
      color: "#ff0000",
    });

    filterTestTag2 = db.tags.create({
      name: "JunctionTag2",
      color: "#00ff00",
    });

    filterTestTag3 = db.tags.create({
      name: "JunctionTag3",
      color: "#0000ff",
    });

    // Create tasks with various tag/assignee combinations
    const now = BigInt(Date.now());

    filterTestTask1 = db.tasks.create({
      title: "JunctionTask1",
      status: "open",
      priority: "high",
      project: filterTestProject,
      assignee: filterTestUser1,
      createdAt: now,
      updatedAt: now,
      isCompleted: false,
    });

    filterTestTask2 = db.tasks.create({
      title: "JunctionTask2",
      status: "in_progress",
      priority: "medium",
      project: filterTestProject,
      assignee: filterTestUser2,
      createdAt: now,
      updatedAt: now,
      isCompleted: false,
    });

    filterTestTask3 = db.tasks.create({
      title: "JunctionTask3",
      status: "done",
      priority: "low",
      project: filterTestProject,
      createdAt: now,
      updatedAt: now,
      isCompleted: true,
    });

    filterTestTask4 = db.tasks.create({
      title: "JunctionTask4",
      status: "open",
      priority: "high",
      project: filterTestProject,
      assignee: filterTestUser1,
      createdAt: now,
      updatedAt: now,
      isCompleted: false,
    });

    // Create task-tag associations
    // Task1: tag1
    db.tasktags.create({ task: filterTestTask1, tag: filterTestTag1 });

    // Task2: tag1, tag2
    db.tasktags.create({ task: filterTestTask2, tag: filterTestTag1 });
    db.tasktags.create({ task: filterTestTask2, tag: filterTestTag2 });

    // Task3: tag2, tag3
    db.tasktags.create({ task: filterTestTask3, tag: filterTestTag2 });
    db.tasktags.create({ task: filterTestTask3, tag: filterTestTag3 });

    // Task4: no tags
  });

  describe("some filter on junction table", () => {
    it("finds tasks that have a specific tag", async () => {
      const tasks = await subscribeOnce((cb) =>
        db.tasks
          .where({
            project: filterTestProject,
            TaskTags: { some: { tag: filterTestTag1 } },
          })
          .subscribeAll(cb),
      );

      // Should find Task1 and Task2 (both have tag1)
      expect(tasks.length).toBe(2);
      const titles = tasks.map((t) => t.title).sort();
      expect(titles).toEqual(["JunctionTask1", "JunctionTask2"]);
    });

    it("finds tasks that have any of multiple tags", async () => {
      const tasks = await subscribeOnce((cb) =>
        db.tasks
          .where({
            project: filterTestProject,
            TaskTags: { some: { tag: filterTestTag3 } },
          })
          .subscribeAll(cb),
      );

      // Should only find Task3 (only one with tag3)
      expect(tasks.length).toBe(1);
      expect(tasks[0].title).toBe("JunctionTask3");
    });

    it("combines status filter with junction table filter", async () => {
      const tasks = await subscribeOnce((cb) =>
        db.tasks
          .where({
            project: filterTestProject,
            status: "open",
            TaskTags: { some: { tag: filterTestTag1 } },
          })
          .subscribeAll(cb),
      );

      // Should only find Task1 (open with tag1)
      expect(tasks.length).toBe(1);
      expect(tasks[0].title).toBe("JunctionTask1");
    });

    it("combines priority filter with junction table filter", async () => {
      const tasks = await subscribeOnce((cb) =>
        db.tasks
          .where({
            project: filterTestProject,
            priority: "low",
            TaskTags: { some: { tag: filterTestTag2 } },
          })
          .subscribeAll(cb),
      );

      // Should only find Task3 (low priority with tag2)
      expect(tasks.length).toBe(1);
      expect(tasks[0].title).toBe("JunctionTask3");
    });
  });

  describe("junction table filter with includes", () => {
    it("filters by tag and includes project", async () => {
      const tasks = await subscribeOnce((cb) =>
        db.tasks
          .where({
            project: filterTestProject,
            TaskTags: { some: { tag: filterTestTag1 } },
          })
          .with({ project: true })
          .subscribeAll(cb),
      );

      expect(tasks.length).toBe(2);
      // All tasks should have project included as object
      expect(tasks.every((t) => typeof t.project === "object")).toBe(true);
      expect(
        tasks.every((t) => t.project.name === "JunctionFilterProject"),
      ).toBe(true);
    });

    // TODO: This test reveals an issue with the dynamic decoder when only reverse ref includes
    // are used (without forward ref). The binary format parsing is incorrect in this case.
    // The "filters and includes both forward ref and junction table" test works because
    // it has project: true (forward ref) in addition to TaskTags.
    it.skip("filters by tag and includes junction table with nested tag", async () => {
      const tasks = await subscribeOnce((cb) =>
        db.tasks
          .where({
            project: filterTestProject,
            TaskTags: { some: { tag: filterTestTag1 } },
          })
          .with({ TaskTags: { tag: true } })
          .subscribeAll(cb),
      );

      expect(tasks.length).toBe(2);

      // Each task should have TaskTags array
      tasks.forEach((task) => {
        expect(Array.isArray(task.TaskTags)).toBe(true);
        // Each TaskTag should have tag as object
        task.TaskTags.forEach((tt: any) => {
          expect(typeof tt.tag).toBe("object");
          expect(tt.tag.name).toBeDefined();
          expect(tt.tag.color).toBeDefined();
        });
      });

      // Verify specific tags are included
      const task2 = tasks.find((t) => t.title === "JunctionTask2");
      expect(task2!.TaskTags.length).toBe(2);
      const tagNames = task2!.TaskTags.map((tt: any) => tt.tag.name).sort();
      expect(tagNames).toEqual(["JunctionTag1", "JunctionTag2"]);
    });

    it("filters and includes both forward ref and junction table", async () => {
      const tasks = await subscribeOnce((cb) =>
        db.tasks
          .where({
            project: filterTestProject,
            TaskTags: { some: { tag: filterTestTag2 } },
          })
          .with({
            project: true,
            TaskTags: { tag: true },
          })
          .subscribeAll(cb),
      );

      // Should find Task2 and Task3 (both have tag2)
      expect(tasks.length).toBe(2);
      const titles = tasks.map((t) => t.title).sort();
      expect(titles).toEqual(["JunctionTask2", "JunctionTask3"]);

      // Verify includes
      tasks.forEach((task) => {
        expect(typeof task.project).toBe("object");
        expect(task.project.name).toBe("JunctionFilterProject");
        expect(Array.isArray(task.TaskTags)).toBe(true);
        expect(
          task.TaskTags.every((tt: any) => typeof tt.tag === "object"),
        ).toBe(true);
      });
    });

    it("complex query mirroring demo app pattern", async () => {
      // This mirrors the demo app's useAll query:
      // .where({ project, status, priority, IssueAssignees: { some: { user } }, IssueLabels: { some: { label } } })
      // .with({ project: true, IssueLabels: { label: true }, IssueAssignees: { user: true } })

      const tasks = await subscribeOnce((cb) =>
        db.tasks
          .where({
            project: filterTestProject,
            status: "in_progress",
            TaskTags: { some: { tag: filterTestTag1 } },
          })
          .with({
            project: true,
            assignee: true,
            TaskTags: { tag: true },
          })
          .subscribeAll(cb),
      );

      // Should only find Task2 (in_progress with tag1)
      expect(tasks.length).toBe(1);
      const task = tasks[0];
      expect(task.title).toBe("JunctionTask2");

      // Verify all includes
      expect(typeof task.project).toBe("object");
      expect(task.project.name).toBe("JunctionFilterProject");

      expect(typeof task.assignee).toBe("object");
      expect(task.assignee!.name).toBe("JunctionFilterUser2");

      expect(Array.isArray(task.TaskTags)).toBe(true);
      expect(task.TaskTags.length).toBe(2);
      const tagNames = task.TaskTags.map((tt: any) => tt.tag.name).sort();
      expect(tagNames).toEqual(["JunctionTag1", "JunctionTag2"]);
    });
  });

  describe("filter returning no results", () => {
    it("returns empty array when no tasks match junction filter", async () => {
      // Create a new tag that no task has
      const unusedTag = db.tags.create({
        name: "UnusedTag",
        color: "#999999",
      });

      const tasks = await subscribeOnce((cb) =>
        db.tasks
          .where({
            project: filterTestProject,
            TaskTags: { some: { tag: unusedTag } },
          })
          .subscribeAll(cb),
      );

      expect(tasks.length).toBe(0);
    });

    it("returns empty array when combined filters eliminate all results", async () => {
      const tasks = await subscribeOnce((cb) =>
        db.tasks
          .where({
            project: filterTestProject,
            status: "done", // Only Task3 is done
            TaskTags: { some: { tag: filterTestTag1 } }, // Only Task1 and Task2 have tag1
          })
          .subscribeAll(cb),
      );

      // No task is both "done" AND has tag1
      expect(tasks.length).toBe(0);
    });
  });
});

// === Subscription Reactivity ===

describe("Subscription Reactivity", () => {
  it("receives updates when data changes", async () => {
    const updates: any[][] = [];
    let updateCount = 0;

    // Use exact name match instead of startsWith to avoid Groove parser issues
    const unsubscribe = db.users
      .where({ email: "reactive-test@test.com" })
      .subscribeAll((rows) => {
        updates.push([...rows]);
        updateCount++;
      });

    // Wait for initial empty result
    await new Promise((r) => setTimeout(r, 50));
    expect(updates.length).toBeGreaterThanOrEqual(1);
    const initialCount = updateCount;

    // Create a user (use non-integer score to ensure F64 type)
    const userId = db.users.create({
      name: "ReactiveUser",
      email: "reactive-test@test.com",
      age: BigInt(20),
      score: 50.5,
      isAdmin: false,
    });

    // Wait for update
    await new Promise((r) => setTimeout(r, 100));
    expect(updateCount).toBeGreaterThan(initialCount);
    const afterCreate = updates[updates.length - 1];
    expect(afterCreate.some((u) => u.name === "ReactiveUser")).toBe(true);

    // Update the user
    db.users.update(userId, { name: "ReactiveUserUpdated" });

    // Wait for update
    await new Promise((r) => setTimeout(r, 100));
    const afterUpdate = updates[updates.length - 1];
    expect(afterUpdate.some((u) => u.name === "ReactiveUserUpdated")).toBe(
      true,
    );

    // Delete the user
    db.users.delete(userId);

    // Wait for update
    await new Promise((r) => setTimeout(r, 100));
    const afterDelete = updates[updates.length - 1];
    expect(afterDelete.some((u) => u.name === "ReactiveUserUpdated")).toBe(
      false,
    );

    unsubscribe();
  });

  it("subscribe to single row by id", async () => {
    const userId = db.users.create({
      name: "SingleSubscribe",
      email: "single@test.com",
      age: BigInt(25),
      score: 60.5, // Use non-integer to ensure F64 type
      isAdmin: false,
    });

    const row = await subscribeOnce((cb) => db.users.subscribe(userId, cb));
    expect(row).not.toBeNull();
    expect(row!.name).toBe("SingleSubscribe");
  });

  it("subscribe to single row with includes", async () => {
    const ownerId = db.users.create({
      name: "SingleWithInclude",
      email: "singleinclude@test.com",
      age: BigInt(30),
      score: 70.5, // Use non-integer to ensure F64 type
      isAdmin: true,
    });

    const projectId = db.projects.create({
      name: "SingleIncludeProject",
      owner: ownerId,
      color: "#fedcba",
    });

    const project = await subscribeOnce((cb) =>
      db.projects.with({ owner: true }).subscribe(projectId, cb),
    );
    expect(project).not.toBeNull();
    expect(project!.name).toBe("SingleIncludeProject");
    expect(typeof project!.owner).toBe("object");
    expect(project!.owner.name).toBe("SingleWithInclude");
  });
});
