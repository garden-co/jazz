import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it, onTestFinished } from "vitest";
import { schema as s } from "../index.js";

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

  // Normal todo policy is intentionally permissive so branch deny-by-default is observable.
  policy.todos.allowRead.where({ ownerId: session.user_id });
  policy.todos.allowInsert.where({ ownerId: session.user_id });

  policy.forBranch(policy.branches, ({ $branch, branchPolicy }) => {
    branchPolicy.todos.allowRead.where({ projectId: $branch.projectId });
    branchPolicy.todos.allowInsert.where({
      projectId: $branch.projectId,
      ownerId: session.user_id,
    });
    branchPolicy.todos.allowUpdate.where({ projectId: $branch.projectId });
    branchPolicy.todos.allowDelete.where({ projectId: $branch.projectId });
  });
});

type JazzContext = import("../backend/create-jazz-context.js").JazzContext;

async function createBranchPermissionsContext(): Promise<JazzContext> {
  const appId = randomUUID();
  const dataRoot = await mkdtemp(join(tmpdir(), "jazz-branch-permissions-"));
  const dataPath = join(dataRoot, "runtime.db");
  const { createJazzContext } = await import("../backend/create-jazz-context.js");
  const context = createJazzContext({
    appId,
    app,
    permissions,
    driver: { type: "persistent", dataPath },
    env: "test",
    userBranch: "main",
    tier: "edge",
  });

  onTestFinished(async () => {
    context.flush();
    await context.shutdown();
    await new Promise((resolve) => setTimeout(resolve, 50));
    await rm(dataRoot, { recursive: true, force: true });
  });

  return context;
}

function titles(rows: Array<{ title: string }>): string[] {
  return rows.map((row) => row.title).sort();
}

async function expectDeniedWrite(write: () => Promise<unknown>): Promise<void> {
  // A branch the session cannot access is denied deny-by-default. For inserts this
  // surfaces as a policy-denied error; for update/delete of a row that is invisible
  // to the session (because the backing branch row is unreadable) the targeted row
  // is not found — both are valid denials. The denial is corroborated separately by
  // the session's branch reads returning [] and the owner's row remaining intact.
  await expect(write()).rejects.toThrow(/policy denied|denied|object not found|not found/i);
}

describe("branch permissions integration", () => {
  it("enforces branch-scoped CRUD permissions and isolation end to end", async () => {
    const context = await createBranchPermissionsContext();
    const aliceDb = context.forSession({ user_id: "alice", claims: {}, authMode: "external" }, app);
    const bobDb = context.forSession({ user_id: "bob", claims: {}, authMode: "external" }, app);

    const project = await aliceDb
      .insert(app.projects, { name: "Website", ownerId: "alice" })
      .wait({ tier: "edge" });
    const branch = await aliceDb
      .insert(app.branches, { projectId: project.id, ownerId: "alice" })
      .wait({ tier: "edge" });

    const aDraft = aliceDb.branch(branch.id);
    const bDraft = bobDb.branch(branch.id);

    const draftTodo = await aDraft
      .insert(app.todos, {
        projectId: project.id,
        title: "Draft branch todo",
        ownerId: "alice",
      })
      .wait({ tier: "edge" });

    await expect(
      aDraft.all(app.todos.where({ projectId: project.id }), { tier: "edge" }),
    ).resolves.toEqual([expect.objectContaining({ id: draftTodo.id, title: "Draft branch todo" })]);

    await expect(
      aliceDb.all(app.todos.where({ projectId: project.id }), { tier: "edge" }),
    ).resolves.toEqual([]);

    const mainTodo = await aliceDb
      .insert(app.todos, {
        projectId: project.id,
        title: "Main todo",
        ownerId: "alice",
      })
      .wait({ tier: "edge" });

    await expect(
      aDraft.all(app.todos.where({ projectId: project.id }), { tier: "edge" }),
    ).resolves.toEqual([expect.objectContaining({ id: draftTodo.id, title: "Draft branch todo" })]);
    await expect(
      aliceDb.all(app.todos.where({ projectId: project.id }), { tier: "edge" }),
    ).resolves.toEqual([expect.objectContaining({ id: mainTodo.id, title: "Main todo" })]);

    await expect(
      bDraft.all(app.todos.where({ projectId: project.id }), { tier: "edge" }),
    ).resolves.toEqual([]);

    await expectDeniedWrite(async () => {
      await bDraft
        .insert(app.todos, {
          projectId: project.id,
          title: "Bob branch todo",
          ownerId: "bob",
        })
        .wait({ tier: "edge" });
    });
    await expectDeniedWrite(async () => {
      await bDraft
        .update(app.todos, draftTodo.id, { title: "Bob branch update" })
        .wait({ tier: "edge" });
    });
    await expectDeniedWrite(async () => {
      await bDraft.delete(app.todos, draftTodo.id).wait({ tier: "edge" });
    });
    await expect(
      bDraft.all(app.todos.where({ projectId: project.id }), { tier: "edge" }),
    ).resolves.toEqual([]);

    await aDraft
      .update(app.todos, draftTodo.id, { title: "Updated branch todo" })
      .wait({ tier: "edge" });
    expect(
      titles(await aDraft.all(app.todos.where({ projectId: project.id }), { tier: "edge" })),
    ).toEqual(["Updated branch todo"]);

    await aDraft.delete(app.todos, draftTodo.id).wait({ tier: "edge" });
    await expect(
      aDraft.all(app.todos.where({ projectId: project.id }), { tier: "edge" }),
    ).resolves.toEqual([]);
    await expect(
      aliceDb.all(app.todos.where({ projectId: project.id }), { tier: "edge" }),
    ).resolves.toEqual([expect.objectContaining({ id: mainTodo.id, title: "Main todo" })]);
  });
});
