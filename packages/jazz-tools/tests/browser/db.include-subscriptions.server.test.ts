import { afterEach, describe, expect, it } from "vitest";
import {
  createDb,
  generateAuthSecret,
  publishStoredPermissions,
  schema,
  type CompiledPermissions,
  type Db,
  type RowOf,
} from "../../src/index.js";
import { fetchPermissionsHead, publishStoredSchema } from "../../src/runtime/schema-fetch.js";
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

  it("keeps overlapping forward-include carriers canonical after a live global insert", async () => {
    const { appId, serverUrl, adminSecret } = await getJazzServerInfo(
      uniqueDbName("overlapping-forward-include-carriers"),
    );
    await publishSchemaAndPermissions(appId, serverUrl, adminSecret, permissions);

    const sharedSecret = generateAuthSecret();
    const authority = await openDb(
      appId,
      serverUrl,
      adminSecret,
      "overlapping-forward-include-authority",
      sharedSecret,
    );
    const browser = await openDb(
      appId,
      serverUrl,
      adminSecret,
      "overlapping-forward-include-browser",
      sharedSecret,
    );
    await ensureNativeRuntimeAdapterReady(authority);
    await ensureNativeRuntimeAdapterReady(browser);

    const org = await authority
      .insert(app.orgs, { name: "overlap parent" })
      .wait({ tier: "global" });
    const todo = await authority
      .insert(app.todos, { title: "unrelated browser read", org_id: org.id })
      .wait({ tier: "global" });
    const userCheck = await authority
      .insert(app.user_checks, { org_id: org.id, todo_id: todo.id })
      .wait({ tier: "global" });

    const includingSnapshots: Array<
      Array<{ id: string; org?: { id: string }; user_check?: { id: string } }>
    > = [];
    const refFilteredSnapshots: Array<Array<{ id: string }>> = [];
    const including = ctx.trackSubscription(
      browser.subscribe(
        app.check_notes
          .where({ org_id: org.id })
          .include({ org: true, user_check: true })
          .requireIncludes(),
        (rows) => includingSnapshots.push(rows as (typeof includingSnapshots)[number]),
        { tier: "global" },
      ),
    );
    const refFiltered = ctx.trackSubscription(
      browser.subscribe(
        app.check_notes.where({ user_check_id: userCheck.id }),
        (rows) => refFilteredSnapshots.push(rows as (typeof refFilteredSnapshots)[number]),
        { tier: "global" },
      ),
    );
    await waitForCondition(
      async () => includingSnapshots.length > 0 && refFilteredSnapshots.length > 0,
      10_000,
      "fresh browser replica did not hydrate both overlapping subscriptions",
    );
    await new Promise<void>((resolve) => setTimeout(resolve, 500));

    const note = await withTimeout(
      authority
        .insert(app.check_notes, {
          body: "live authoritative overlap",
          org_id: org.id,
          user_check_id: userCheck.id,
        })
        .wait({ tier: "global" }),
      10_000,
      "authoritative insert did not settle globally",
    );
    try {
      await waitForCondition(
        async () =>
          includingSnapshots.some(
            (rows) =>
              rows.length === 1 &&
              rows[0]?.id === note.id &&
              rows[0]?.org?.id === org.id &&
              rows[0]?.user_check?.id === userCheck.id,
          ) && refFilteredSnapshots.some((rows) => rows.length === 1 && rows[0]?.id === note.id),
        15_000,
        `overlapping include/ref carriers did not project the live row exactly once; including=${JSON.stringify(
          includingSnapshots.slice(-4),
        )}; refFiltered=${JSON.stringify(refFilteredSnapshots.slice(-4))}`,
      );
    } catch (error) {
      throw new Error(
        `${error}; workerLifecycle=${JSON.stringify(await workerLifecycle(browser))}`,
      );
    }

    await expect(
      browser.all(app.todos.where({ id: todo.id }), { tier: "local" }),
    ).resolves.toMatchObject([{ id: todo.id, title: "unrelated browser read" }]);
    expect(
      includingSnapshots.filter((rows) => rows.length === 1 && rows[0]?.id === note.id),
    ).toHaveLength(1);
    expect(
      refFilteredSnapshots.filter((rows) => rows.length === 1 && rows[0]?.id === note.id),
    ).toHaveLength(1);
    including();
    refFiltered();
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
      logLevel: "trace",
      driver: { type: "persistent", dbName: uniqueDbName(label) },
    }),
  );
}

async function workerLifecycle(db: Db): Promise<unknown> {
  const port = await (
    db as unknown as { openInspectorControlPort(): Promise<MessagePort> }
  ).openInspectorControlPort();
  const id = 1;
  try {
    const lifecycle = await new Promise<unknown[]>((resolve) => {
      const onMessage = (
        event: MessageEvent<{ type?: string; id?: number; entries?: unknown[] }>,
      ) => {
        if (event.data.type !== "lifecycle-trace" || event.data.id !== id) return;
        port.removeEventListener("message", onMessage);
        resolve(event.data.entries ?? []);
      };
      port.addEventListener("message", onMessage);
      port.start();
      port.postMessage({ type: "lifecycle-trace", id });
    });
    const autopsy = await new Promise<string>((resolve) => {
      const autopsyId = 2;
      const onMessage = (event: MessageEvent<{ type?: string; id?: number; dump?: string }>) => {
        if (event.data.type !== "sync-autopsy" || event.data.id !== autopsyId) return;
        port.removeEventListener("message", onMessage);
        resolve(event.data.dump ?? "");
      };
      port.addEventListener("message", onMessage);
      port.postMessage({ type: "sync-autopsy", id: autopsyId });
    });
    const foregroundAutopsy = (
      globalThis as typeof globalThis & {
        __jazzTestSyncAutopsy?: { __testSyncAutopsyDump?(): string };
      }
    ).__jazzTestSyncAutopsy?.__testSyncAutopsyDump?.();
    return { lifecycle, autopsy, foregroundAutopsy };
  } finally {
    port.postMessage({ type: "close" });
    port.close();
  }
}

async function publishSchemaAndPermissions(
  appId: string,
  serverUrl: string,
  adminSecret: string,
  permissions: CompiledPermissions,
): Promise<void> {
  const { hash: schemaHash } = await publishStoredSchema(serverUrl, {
    appId,
    adminSecret,
    schema: app.wasmSchema,
  });
  const { head } = await fetchPermissionsHead(serverUrl, {
    appId,
    adminSecret,
  });
  await publishStoredPermissions(serverUrl, {
    appId,
    adminSecret,
    schemaHash,
    permissions,
    expectedParentBundleObjectId: head?.bundleObjectId ?? null,
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
