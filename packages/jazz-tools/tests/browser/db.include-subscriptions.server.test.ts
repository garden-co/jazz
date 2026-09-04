import { afterEach, describe, expect, it } from "vitest";
import {
  createDb,
  generateAuthSecret,
  schema,
  type CompiledPermissions,
  type Db,
  type RowOf,
} from "../../src/index.js";
import { deploy } from "../../src/dev/catalogue.js";
import { TestCleanup, uniqueDbName, waitForCondition, withTimeout } from "./support.js";
import { getJazzServerInfo } from "./testing-server.js";

const app = schema.defineApp({
  orgs: schema.table({
    name: schema.string(),
  }),
  todos: schema.table({
    title: schema.string(),
    org_id: schema.ref("orgs"),
  }),
  user_checks: schema.table({
    org_id: schema.ref("orgs"),
    todo_id: schema.ref("todos"),
  }),
  check_notes: schema.table({
    body: schema.string(),
    org_id: schema.ref("orgs"),
    user_check_id: schema.ref("user_checks"),
  }),
});

const permissions = schema.definePermissions(app, ({ policy }) => [
  policy.orgs.allowRead.always(),
  policy.orgs.allowInsert.always(),
  policy.orgs.allowUpdate.always(),
  policy.orgs.allowDelete.always(),
  policy.todos.allowRead.always(),
  policy.todos.allowInsert.always(),
  policy.todos.allowUpdate.always(),
  policy.todos.allowDelete.always(),
  policy.user_checks.allowRead.always(),
  policy.user_checks.allowInsert.always(),
  policy.user_checks.allowUpdate.always(),
  policy.user_checks.allowDelete.always(),
  policy.check_notes.allowRead.always(),
  policy.check_notes.allowInsert.always(),
  policy.check_notes.allowUpdate.always(),
  policy.check_notes.allowDelete.always(),
]);

type Org = RowOf<typeof app.orgs>;
type OrgWithDeepIncludes = Org & {
  todosViaOrg?: Array<{
    id: string;
    user_checksViaTodo?: Array<{
      id: string;
      check_notesViaUser_check?: Array<{ id: string; body: string }>;
    }>;
  }>;
};

const ctx = new TestCleanup();

afterEach(async () => {
  await ctx.cleanup();
});

describe("websocket include subscriptions", () => {
  /**
   * A client may attach several independently bound strict-edge reads after an
   * authority has accepted an exclusive transaction. Each attachment
   * needs its own current authority receipt; one must not strand the others.
   *
   * owner ──exclusive todo──► server
   * observer ──three Edge attachments──► server ──current receipts──► observer
   */
  it("covers concurrent edge queries attached after an exclusive commit", async () => {
    const { appId, serverUrl, adminSecret } = await getJazzServerInfo(
      uniqueDbName("exclusive-then-edge-coverage"),
    );
    await publishSchemaAndPermissions(appId, serverUrl, adminSecret, permissions);

    const sharedSecret = generateAuthSecret();
    const owner = await openDb(
      appId,
      serverUrl,
      adminSecret,
      "exclusive-then-edge-owner",
      sharedSecret,
    );
    const observer = await openDb(
      appId,
      serverUrl,
      adminSecret,
      "exclusive-then-edge-observer",
      sharedSecret,
    );
    await ensureNativeRuntimeAdapterReady(owner);
    await ensureNativeRuntimeAdapterReady(observer);

    const org = await owner.insert(app.orgs, { name: "North" }).wait({ tier: "edge" });
    expect(await observer.all(app.orgs.where({ id: org.id }), { tier: "edge" })).toMatchObject([
      { id: org.id },
    ]);
    const write = await owner.exclusiveTransaction((transaction) => {
      const todo = transaction.insert(app.todos, { title: "Ship", org_id: org.id });
      const check = transaction.insert(app.user_checks, { org_id: org.id, todo_id: todo.id });
      const note = transaction.insert(app.check_notes, {
        body: "ready",
        org_id: org.id,
        user_check_id: check.id,
      });
      return { todo, check, note };
    });
    const { todo, check, note } = await withTimeout(
      write.wait(),
      15_000,
      "exclusive transaction did not reach the authority",
    );

    const [todos, checks, notes] = await withTimeout(
      Promise.all([
        observer.all(app.todos.where({ org_id: org.id }), { tier: "edge" }),
        observer.all(app.user_checks.where({ todo_id: todo.id }), { tier: "edge" }),
        observer.all(app.check_notes.where({ user_check_id: check.id }), { tier: "edge" }),
      ]),
      20_000,
      "concurrent strict-edge query coverage did not settle",
    );
    expect(todos.map((row) => row.id)).toEqual([todo.id]);
    expect(checks.map((row) => ({ id: row.id, todoId: row.todo_id }))).toEqual([
      { id: check.id, todoId: todo.id },
    ]);
    expect(notes.map((row) => ({ id: row.id, checkId: row.user_check_id }))).toEqual([
      { id: note.id, checkId: check.id },
    ]);
  }, 45_000);

  it("delivers depth-3 reverse include material from client A to client B subscribe", async () => {
    const { appId, serverUrl, adminSecret } = await getJazzServerInfo(
      uniqueDbName("include-subscriptions"),
    );
    await publishSchemaAndPermissions(appId, serverUrl, adminSecret, permissions);

    const sharedSecret = generateAuthSecret();
    const dbA = await openDb(
      appId,
      serverUrl,
      adminSecret,
      "include-subscriptions-a",
      sharedSecret,
    );
    const dbB = await openDb(
      appId,
      serverUrl,
      adminSecret,
      "include-subscriptions-b",
      sharedSecret,
    );
    await ensureNativeRuntimeAdapterReady(dbA);
    await ensureNativeRuntimeAdapterReady(dbB);

    const snapshots: OrgWithDeepIncludes[][] = [];
    const selectedIncludeQuery = app.orgs
      .include({
        todosViaOrg: app.todos.select("title").include({
          user_checksViaTodo: { check_notesViaUser_check: true },
        }),
      })
      .requireIncludes();
    const unsubscribe = ctx.trackSubscription(
      dbB.subscribe(
        selectedIncludeQuery,
        (rows) => {
          snapshots.push(rows as OrgWithDeepIncludes[]);
        },
        { tier: "global" },
      ),
    );
    await waitForCondition(
      async () => snapshots.length > 0,
      10_000,
      "client B subscribe did not produce an initial snapshot",
    );
    expect(snapshots).toEqual([[]]);

    const org = await withTimeout(
      dbA.insert(app.orgs, { name: "Acme" }).wait({ tier: "global" }),
      10_000,
      "client A org insert did not reach the server",
    );
    const todo = await withTimeout(
      dbA.insert(app.todos, { title: "ship it", org_id: org.id }).wait({ tier: "global" }),
      10_000,
      "client A todo insert did not reach the server",
    );
    const userCheck = await withTimeout(
      dbA.insert(app.user_checks, { org_id: org.id, todo_id: todo.id }).wait({ tier: "global" }),
      10_000,
      "client A user_check insert did not reach the server",
    );

    const note = await withTimeout(
      dbA
        .insert(app.check_notes, {
          body: "looks good",
          org_id: org.id,
          user_check_id: userCheck.id,
        })
        .wait({ tier: "global" }),
      10_000,
      "client A check_note insert did not reach the server",
    );

    await waitForCondition(
      async () =>
        snapshots.some(
          (rows) =>
            includesNote(rows, org.id, todo.id, userCheck.id, note.id) &&
            hasProjectedTodo(rows, org.id, todo.id, "ship it"),
        ),
      15_000,
      `client B subscribe received client A's projected depth-3 reverse include; snapshots=${JSON.stringify(
        snapshots.slice(-3),
      )}`,
    );
    expect(
      snapshots.filter(
        (rows) =>
          includesNote(rows, org.id, todo.id, userCheck.id, note.id) &&
          hasProjectedTodo(rows, org.id, todo.id, "ship it"),
      ),
    ).toHaveLength(1);

    await withTimeout(
      dbA.update(app.todos, todo.id, { title: "ship it again" }).wait({ tier: "global" }),
      10_000,
      "client A todo update did not reach the server",
    );

    await waitForCondition(
      async () =>
        snapshots.some((rows) => hasProjectedTodo(rows, org.id, todo.id, "ship it again")),
      15_000,
      `client B subscribe received projected client A todo update; snapshots=${JSON.stringify(
        snapshots.slice(-3),
      )}`,
    );
    expect(
      snapshots.filter((rows) => hasProjectedTodo(rows, org.id, todo.id, "ship it again")),
    ).toHaveLength(1);

    unsubscribe();
    expect(
      snapshots.some((rows) => includesNote(rows, org.id, todo.id, userCheck.id, note.id)),
    ).toBe(true);
    expect(snapshots.some((rows) => hasProjectedTodo(rows, org.id, todo.id, "ship it again"))).toBe(
      true,
    );
  }, 60_000);
});

async function openDb(
  appId: string,
  serverUrl: string,
  adminSecret: string,
  label: string,
  secret: string,
): Promise<Db> {
  return ctx.track(
    await createDb({
      appId,
      serverUrl,
      adminSecret,
      secret,
      driver: { type: "persistent", dbName: uniqueDbName(label) },
    }),
  );
}

async function publishSchemaAndPermissions(
  appId: string,
  serverUrl: string,
  adminSecret: string,
  permissions: CompiledPermissions,
): Promise<void> {
  await deploy({
    appId,
    serverUrl,
    adminSecret,
    schema: app.wasmSchema,
    permissions,
  });
}

function includesNote(
  rows: OrgWithDeepIncludes[],
  orgId: string,
  todoId: string,
  userCheckId: string,
  noteId: string,
): boolean {
  return rows.some(
    (org) =>
      org.id === orgId &&
      Array.isArray(org.todosViaOrg) &&
      org.todosViaOrg.some(
        (todo) =>
          todo.id === todoId &&
          Array.isArray(todo.user_checksViaTodo) &&
          todo.user_checksViaTodo.some(
            (userCheck) =>
              userCheck.id === userCheckId &&
              Array.isArray(userCheck.check_notesViaUser_check) &&
              userCheck.check_notesViaUser_check.some((note) => note.id === noteId),
          ),
      ),
  );
}

function hasProjectedTodo(
  rows: OrgWithDeepIncludes[],
  orgId: string,
  todoId: string,
  title: string,
): boolean {
  return rows.some((org) => {
    if (org.id !== orgId) return false;
    if (!Array.isArray(org.todosViaOrg)) return false;
    const todo = org.todosViaOrg.find((candidate) => candidate.id === todoId);
    return (
      todo?.title === title &&
      !("org_id" in todo) &&
      !("org" in todo) &&
      Array.isArray(todo.user_checksViaTodo)
    );
  });
}

async function ensureNativeRuntimeAdapterReady(db: Db): Promise<void> {
  (db as unknown as { getClient(schema: unknown): unknown }).getClient(app.wasmSchema);
}
