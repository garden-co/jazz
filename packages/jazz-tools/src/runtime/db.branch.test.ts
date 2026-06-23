import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { createDb, type Db } from "./db.js";

const app = s.defineApp({
  projects: s.table({ name: s.string(), ownerId: s.string() }),
  branches: s.table({ projectId: s.ref("projects"), ownerId: s.string() }),
  todos: s.table({
    projectId: s.ref("projects"),
    title: s.string(),
    ownerId: s.string(),
  }),
});

const permissions = s.definePermissions(app, ({ policy, session }) => {
  policy.projects.allowRead.where({ ownerId: session.user_id });
  policy.projects.allowInsert.where({ ownerId: session.user_id });
  policy.branches.allowRead.where({ ownerId: session.user_id });
  policy.branches.allowInsert.where({ ownerId: session.user_id });
  policy.forBranch(policy.branches, ({ $branch, branchPolicy }) => {
    branchPolicy.todos.allowRead.where({ projectId: $branch.projectId });
    branchPolicy.todos.allowInsert.where({
      projectId: $branch.projectId,
      ownerId: session.user_id,
    });
  });
});

const dbs: Db[] = [];

afterEach(async () => {
  while (dbs.length > 0) {
    const db = dbs.pop();
    if (db) {
      await db.shutdown();
    }
  }
});

async function makeDb(): Promise<Db> {
  void permissions;
  const appId = `db-branch-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const db = await createDb({
    appId,
    driver: { type: "persistent", dbName: appId },
    cookieSession: { user_id: "alice", claims: {}, authMode: "external" },
    userBranch: "main",
  });
  dbs.push(db);
  return db;
}

describe("Db branch view", () => {
  it("routes reads and writes through the selected branch", async () => {
    const db = await makeDb();

    const projectInsert = db.insert(app.projects, {
      name: "Website",
      ownerId: "alice",
    });
    await projectInsert.wait({ tier: "local" });
    const project = projectInsert.value;

    const branchInsert = db.insert(app.branches, {
      projectId: project.id,
      ownerId: "alice",
    });
    await branchInsert.wait({ tier: "local" });
    const branch = branchInsert.value;

    const branchDb = db.branch(branch.id);
    const todoInsert = branchDb.insert(app.todos, {
      projectId: project.id,
      title: "Draft landing page",
      ownerId: "alice",
    });
    await todoInsert.wait({ tier: "local" });
    const todo = todoInsert.value;

    await expect(branchDb.all(app.todos.where({ projectId: project.id }))).resolves.toEqual([
      expect.objectContaining({ id: todo.id, title: "Draft landing page" }),
    ]);
    await expect(db.all(app.todos.where({ projectId: project.id }))).resolves.toEqual([]);
  });
});
