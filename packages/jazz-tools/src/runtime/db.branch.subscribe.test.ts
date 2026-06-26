import { afterEach, describe, expect, it } from "vitest";
import { schema as s } from "../index.js";
import { createDb, type Db } from "./db.js";
import type { SubscriptionDelta } from "../shared/index.js";

const app = s.defineApp({
  projects: s.table({ name: s.string(), ownerId: s.string() }),
  branches: s.table({ projectId: s.ref("projects"), ownerId: s.string() }),
  todos: s.table({ projectId: s.ref("projects"), title: s.string(), ownerId: s.string() }),
});

const dbs: Db[] = [];
afterEach(async () => {
  while (dbs.length > 0) {
    const db = dbs.pop();
    if (db) await db.shutdown();
  }
});

async function makeDb(): Promise<Db> {
  const appId = `db-branch-sub-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  const db = await createDb({
    appId,
    driver: { type: "persistent", dbName: appId },
    cookieSession: { user_id: "alice", claims: {}, authMode: "external" },
    userBranch: "main",
  });
  dbs.push(db);
  return db;
}

describe("branch subscription (useAll path) repro", () => {
  it("a branch-scoped subscription receives an optimistic branch insert", async () => {
    const db = await makeDb();
    const project = await db
      .insert(app.projects, { name: "P", ownerId: "alice" })
      .wait({ tier: "local" });
    const branch = await db
      .insert(app.branches, { projectId: project.id, ownerId: "alice" })
      .wait({ tier: "local" });

    let branchRows: string[] = [];
    let mainRows: string[] = [];
    const q = app.todos.where({ projectId: project.id });
    const unsubBranch = db.subscribeAll(
      q,
      (d: SubscriptionDelta<{ id: string; title: string }>) => {
        branchRows = d.all.map((t) => t.title);
      },
      { branch: branch.id },
    );
    const unsubMain = db.subscribeAll(q, (d: SubscriptionDelta<{ id: string; title: string }>) => {
      mainRows = d.all.map((t) => t.title);
    });

    // Optimistic insert on the branch, mirroring the example app (no .wait()).
    db.branch(branch.id).insert(app.todos, {
      projectId: project.id,
      title: "Branch todo",
      ownerId: "alice",
    });

    await new Promise((r) => setTimeout(r, 500));

    expect(branchRows).toContain("Branch todo");
    expect(mainRows).not.toContain("Branch todo");
    unsubBranch();
    unsubMain();
  });
});
