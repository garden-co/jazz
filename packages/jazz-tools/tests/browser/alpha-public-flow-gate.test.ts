import { afterEach, describe, expect, it } from "vitest";
import {
  createDb,
  generateAuthSecret,
  publishStoredPermissions,
  RowChangeKind,
  schema,
  type CompiledPermissions,
  type Db,
  type Query,
  type RowDelta,
  type RowOf,
} from "../../src/index.js";
import { fetchPermissionsHead, publishStoredSchema } from "../../src/runtime/schema-fetch.js";
import {
  TestCleanup,
  uniqueDbName,
  waitForCondition,
  waitForQuery,
  withTimeout,
} from "./support.js";
import { getJazzServerInfo } from "./testing-server.js";
import {
  closeRemoteBrowserDb,
  createRemoteBrowserDb,
  waitForRemoteBrowserDbTitle,
} from "./remote-browser-db.js";

const app = schema.defineApp({
  todos: schema.table({
    title: schema.string(),
    done: schema.boolean(),
    list: schema.string(),
  }),
});

const richApp = schema.defineApp({
  users: schema.table({
    name: schema.string(),
  }),
  todos: schema.table({
    title: schema.string(),
    done: schema.boolean(),
    list: schema.string(),
    priority: schema.int(),
    tags: schema.array(schema.string()),
    payload: schema.bytes().optional(),
    ownerId: schema.ref("users").optional(),
  }),
});

const fileApp = schema.defineApp({
  files: schema.table({
    name: schema.string(),
    mime_type: schema.string(),
    data: schema.bytes(),
  }),
});

const permissions = schema.definePermissions(app, ({ policy }) => [
  policy.todos.allowRead.always(),
  policy.todos.allowInsert.always(),
  policy.todos.allowUpdate.always(),
  policy.todos.allowDelete.always(),
]);

const richPermissions = schema.definePermissions(richApp, ({ policy }) => [
  policy.users.allowRead.always(),
  policy.users.allowInsert.always(),
  policy.users.allowUpdate.always(),
  policy.users.allowDelete.always(),
  policy.todos.allowRead.always(),
  policy.todos.allowInsert.always(),
  policy.todos.allowUpdate.always(),
  policy.todos.allowDelete.always(),
]);

const filePermissions = schema.definePermissions(fileApp, ({ policy }) => [
  policy.files.allowRead.always(),
  policy.files.allowInsert.always(),
  policy.files.allowUpdate.always(),
  policy.files.allowDelete.always(),
]);

type Todo = RowOf<typeof app.todos>;
type RichTodo = RowOf<typeof richApp.todos>;

const ctx = new TestCleanup();

afterEach(async () => {
  await ctx.cleanup();
});

describe("alpha public package flow", () => {
  it("opens public createDb with persistent core locally, then runs todo CRUD and subscriptions", async () => {
    const appId = uniqueDbName("alpha-public-local-flow");
    const persistentDbName = uniqueDbName("alpha-public-local-opfs");
    const sharedSecret = generateAuthSecret();
    let db = ctx.track(
      await createDb({
        appId,
        secret: sharedSecret,
        driver: { type: "persistent", dbName: persistentDbName },
      }),
    );
    const snapshots: Todo[][] = [];
    const unsubscribe = ctx.trackSubscription(
      db.subscribeAll(app.todos.orderBy("title"), (delta) => {
        snapshots.push([...delta.all]);
      }),
    );

    const createdWrite = db.insert(app.todos, {
      title: "Adopt alpha public flow",
      done: false,
      list: "launch",
    });
    const created = await createdWrite.wait({ tier: "local" });
    await expectTodoTitles(db, snapshots, ["Adopt alpha public flow"]);

    const secondWrite = db.insert(app.todos, {
      title: "Prove public imports",
      done: false,
      list: "launch",
    });
    const second = await secondWrite.wait({ tier: "local" });
    await db.update(app.todos, created.id, { done: true }).wait({ tier: "local" });
    expect(await db.one(app.todos.where({ id: created.id }))).toEqual({
      id: created.id,
      title: "Adopt alpha public flow",
      done: true,
      list: "launch",
    });
    await expectTodoSummaries(db, ["Adopt alpha public flow:done", "Prove public imports:open"]);

    await db.delete(app.todos, second.id).wait({ tier: "local" });
    await expectTodoSummaries(db, ["Adopt alpha public flow:done"]);
    expect(await db.one(app.todos.where({ id: second.id }))).toBeNull();

    unsubscribe();
    expect(snapshots.some((rows) => rows.length === 0)).toBe(true);
    expect(
      snapshots.some((rows) =>
        rows.some((todo) => todo.title === "Adopt alpha public flow" && todo.done),
      ),
    ).toBe(true);
  });

  it("reopens public persistent OPFS data after local writes", async () => {
    const appId = uniqueDbName("alpha-public-local-reopen");
    const persistentDbName = uniqueDbName("alpha-public-local-reopen-opfs");
    const sharedSecret = generateAuthSecret();
    let db = ctx.track(
      await createDb({
        appId,
        secret: sharedSecret,
        driver: { type: "persistent", dbName: persistentDbName },
      }),
    );
    const created = await db
      .insert(app.todos, {
        title: "Persist alpha public flow",
        done: true,
        list: "launch",
      })
      .wait({ tier: "local" });

    await db.shutdown();
    ctx.untrack(db);

    db = ctx.track(
      await createDb({
        appId,
        secret: sharedSecret,
        driver: { type: "persistent", dbName: persistentDbName },
      }),
    );
    expect(await db.one(app.todos.where({ id: created.id }), { tier: "local" })).toEqual(created);
  });

  it("moves rows into and out of a filtered public subscription after local updates", async () => {
    const appId = uniqueDbName("alpha-public-local-predicate-move");
    const persistentDbName = uniqueDbName("alpha-public-local-predicate-move-opfs");
    const db = ctx.track(
      await createDb({
        appId,
        secret: generateAuthSecret(),
        driver: { type: "persistent", dbName: persistentDbName },
      }),
    );
    const openTodos = app.todos.where({ done: false }).orderBy("title");
    const snapshots: Todo[][] = [];
    const rowChanges: Array<RowDelta<Todo>> = [];
    const unsubscribe = ctx.trackSubscription(
      db.subscribeAll(openTodos, (delta) => {
        snapshots.push([...delta.all]);
        rowChanges.push(...delta.delta);
      }),
    );

    const startsOpen = await db
      .insert(app.todos, {
        title: "Starts open",
        done: false,
        list: "predicate",
      })
      .wait({ tier: "local" });
    const startsDone = await db
      .insert(app.todos, {
        title: "Starts done",
        done: true,
        list: "predicate",
      })
      .wait({ tier: "local" });

    await expectTodoSummariesForQuery(db, openTodos, ["Starts open:open"], "local");
    await waitForSnapshotSummaries(snapshots, ["Starts open:open"], "initial open predicate");

    await db.update(app.todos, startsOpen.id, { done: true }).wait({ tier: "local" });
    await expectTodoSummariesForQuery(db, openTodos, [], "local");
    await waitForSnapshotSummaries(snapshots, [], "row leaves open predicate after update");
    await waitForRowChange(
      rowChanges,
      (change) => change.kind === RowChangeKind.Removed && change.id === startsOpen.id,
      "removed row after it leaves the open predicate",
    );

    await db.update(app.todos, startsDone.id, { done: false }).wait({ tier: "local" });
    await expectTodoSummariesForQuery(db, openTodos, ["Starts done:open"], "local");
    await waitForSnapshotSummaries(
      snapshots,
      ["Starts done:open"],
      "row enters open predicate after update",
    );
    await waitForRowChange(
      rowChanges,
      (change) =>
        change.kind === RowChangeKind.Added &&
        change.id === startsDone.id &&
        change.item.title === "Starts done",
      "added row after it enters the open predicate",
    );

    unsubscribe();
    expect(await db.one(app.todos.where({ id: startsOpen.id }), { tier: "local" })).toEqual({
      ...startsOpen,
      done: true,
    });
    expect(await db.one(app.todos.where({ id: startsDone.id }), { tier: "local" })).toEqual({
      ...startsDone,
      done: false,
    });
  });

  it("gates richer public row shapes through local core queries and subscriptions", async () => {
    const appId = uniqueDbName("alpha-public-rich-local-flow");
    const persistentDbName = uniqueDbName("alpha-public-rich-local-opfs");
    const db = ctx.track(
      await createDb({
        appId,
        secret: generateAuthSecret(),
        driver: { type: "persistent", dbName: persistentDbName },
      }),
    );
    const snapshots: RichTodo[][] = [];
    const richQuery = richApp.todos
      .where({ tags: { contains: "alpha" }, priority: { gte: 5 } })
      .orderBy("priority", "desc")
      .limit(1);
    const unsubscribe = ctx.trackSubscription(
      db.subscribeAll(richQuery, (delta) => {
        snapshots.push([...delta.all]);
      }),
    );

    const owner = await db.insert(richApp.users, { name: "Alpha Owner" }).wait({ tier: "local" });
    await db
      .insert(richApp.todos, {
        title: "Ignore low-priority rich row",
        done: false,
        list: "launch",
        priority: 2,
        tags: ["alpha"],
        payload: null,
        ownerId: null,
      })
      .wait({ tier: "local" });
    const created = await db
      .insert(richApp.todos, {
        title: "Adopt alpha rich row",
        done: false,
        list: "launch",
        priority: 7,
        tags: ["alpha", "core"],
        payload: new Uint8Array([4, 5, 6, 7]),
        ownerId: owner.id,
      })
      .wait({ tier: "local" });

    expect(await db.one(richQuery, { tier: "local" })).toEqual(created);
    await waitForCondition(
      async () =>
        snapshots.some(
          (rows) =>
            rows.length === 1 &&
            rows[0]?.id === created.id &&
            rows[0].ownerId === owner.id &&
            rows[0].payload instanceof Uint8Array &&
            rows[0].payload.length === 4,
        ),
      5_000,
      "richer local subscribeAll snapshot",
    );

    await db.update(richApp.todos, created.id, { payload: null }).wait({ tier: "local" });
    expect(await db.one(richQuery, { tier: "local" })).toEqual({ ...created, payload: null });
    unsubscribe();
  });

  it("opens public createDb with websocket server config and converges between clients", async () => {
    const requestedAppId = uniqueDbName("alpha-public-websocket-flow");
    const { appId, serverUrl, adminSecret } = await getJazzServerInfo(requestedAppId);
    await publishSchemaAndPermissions(appId, serverUrl, adminSecret, permissions);

    const sharedSecret = generateAuthSecret();
    const dbA = await openAlphaMemoryDb(appId, serverUrl, adminSecret, sharedSecret);
    const dbB = await openAlphaMemoryDb(appId, serverUrl, adminSecret, sharedSecret);
    const snapshots: Todo[][] = [];
    const unsubscribe = ctx.trackSubscription(
      dbB.subscribeAll(app.todos.orderBy("title"), (delta) => {
        snapshots.push([...delta.all]);
      }),
    );

    const created = await withTimeout(
      dbA
        .insert(app.todos, {
          title: "Adopt alpha websocket flow",
          done: false,
          list: "launch",
        })
        .wait({ tier: "edge" }),
      10_000,
      "writer insert was not accepted at the server",
    );

    const rowsOnB = await waitForSubscribedTodoSummaries(dbB, ["Adopt alpha websocket flow:open"]);
    expect(rowsOnB).toEqual([created]);

    await withTimeout(
      dbA.update(app.todos, created.id, { done: true }).wait({ tier: "edge" }),
      10_000,
      "writer update was not accepted at the server",
    );
    await expectTodoSummaries(dbB, ["Adopt alpha websocket flow:done"], "local");

    const remoteBrowserDbId = uniqueDbName("alpha-public-remote-browser-reader");
    await createRemoteBrowserDb({
      id: remoteBrowserDbId,
      appId,
      dbName: uniqueDbName("alpha-public-remote-browser-opfs"),
      table: "todos",
      schemaJson: JSON.stringify(app.wasmSchema),
      serverUrl,
      adminSecret,
      localFirstSecret: sharedSecret,
    });
    try {
      const remoteRows = await waitForRemoteBrowserDbTitle({
        id: remoteBrowserDbId,
        title: "Adopt alpha websocket flow",
        timeoutMs: 15_000,
        tier: "local",
      });
      expect(remoteRows).toContainEqual({
        ...created,
        done: true,
      });
    } finally {
      await closeRemoteBrowserDb(remoteBrowserDbId);
    }

    unsubscribe();
    expect(
      snapshots.some((rows) =>
        rows.some((todo) => todo.id === created.id && todo.title === created.title),
      ),
    ).toBe(true);
  });

  it("publishes richer public row shapes over websocket and converges arrays, bytes, nullable refs, and integer predicates", async () => {
    const requestedAppId = uniqueDbName("alpha-public-rich-websocket-flow");
    const { appId, serverUrl, adminSecret } = await getJazzServerInfo(requestedAppId);
    await publishSchemaAndPermissions(appId, serverUrl, adminSecret, richPermissions, richApp);

    const sharedSecret = generateAuthSecret();
    const dbA = await openAlphaMemoryDb(appId, serverUrl, adminSecret, sharedSecret);
    const dbB = await openAlphaMemoryDb(appId, serverUrl, adminSecret, sharedSecret);
    const richQuery = richApp.todos
      .where({ tags: { contains: "alpha" }, priority: { gte: 5 } })
      .orderBy("priority", "desc")
      .limit(1);

    const snapshots: RichTodo[][] = [];
    const unsubscribe = ctx.trackSubscription(
      dbB.subscribeAll(richQuery, (delta) => {
        snapshots.push([...delta.all]);
      }),
    );

    const owner = await withTimeout(
      dbA.insert(richApp.users, { name: "Alpha Owner" }).wait({ tier: "edge" }),
      10_000,
      "rich owner insert was not accepted at the server",
    );
    await withTimeout(
      dbA
        .insert(richApp.todos, {
          title: "Ignore low-priority websocket rich row",
          done: false,
          list: "launch",
          priority: 2,
          tags: ["alpha"],
          payload: null,
          ownerId: null,
        })
        .wait({ tier: "edge" }),
      10_000,
      "low-priority rich row insert was not accepted at the server",
    );
    const created = await withTimeout(
      dbA
        .insert(richApp.todos, {
          title: "Adopt alpha websocket rich row",
          done: false,
          list: "launch",
          priority: 7,
          tags: ["alpha", "core"],
          payload: new Uint8Array([4, 5, 6, 7]),
          ownerId: owner.id,
        })
        .wait({ tier: "edge" }),
      10_000,
      "rich row insert was not accepted at the server",
    );

    const [rowOnB] = await waitForRichTodos(
      dbB,
      richQuery,
      (todos) => todos.length === 1 && todos[0]?.id === created.id,
      "richer websocket query convergence",
    );
    expect(rowOnB).toMatchObject({
      id: created.id,
      title: "Adopt alpha websocket rich row",
      priority: 7,
      tags: ["alpha", "core"],
      ownerId: owner.id,
    });
    expect(Array.from(rowOnB.payload ?? [])).toEqual([4, 5, 6, 7]);

    await withTimeout(
      dbA
        .update(richApp.todos, created.id, { payload: null, ownerId: null })
        .wait({ tier: "edge" }),
      10_000,
      "rich row nullable update was not accepted at the server",
    );
    const [updatedOnB] = await waitForRichTodos(
      dbB,
      richQuery,
      (todos) => todos.length === 1 && todos[0]?.payload === null && todos[0]?.ownerId === null,
      "richer websocket nullable update convergence",
    );
    expect(updatedOnB).toEqual({ ...created, payload: null, ownerId: null });

    unsubscribe();
    expect(snapshots.some((rows) => rows.some((todo) => todo.id === created.id))).toBe(true);
  });

  it("converges memory writer to persistent OPFS reader over websocket and reopens locally", async () => {
    const requestedAppId = uniqueDbName("alpha-public-mixed-websocket-flow");
    const { appId, serverUrl, adminSecret } = await getJazzServerInfo(requestedAppId);
    await publishSchemaAndPermissions(appId, serverUrl, adminSecret, richPermissions, richApp);

    const sharedSecret = generateAuthSecret();
    const readerDbName = uniqueDbName("alpha-public-mixed-reader-opfs");
    const writer = await openAlphaMemoryDb(appId, serverUrl, adminSecret, sharedSecret);
    let reader = await openAlphaDb(appId, serverUrl, adminSecret, readerDbName, sharedSecret, {
      uniqueLabel: false,
    });
    const richQuery = richApp.todos.where({ tags: { contains: "mixed-boundary" } });

    const snapshots: RichTodo[][] = [];
    const unsubscribe = ctx.trackSubscription(
      reader.subscribeAll(richQuery, (delta) => {
        snapshots.push([...delta.all]);
      }),
    );

    const created = await withTimeout(
      writer
        .insert(richApp.todos, {
          title: "Adopt mixed alpha boundary",
          done: false,
          list: "launch",
          priority: 9,
          tags: ["alpha", "mixed-boundary"],
          payload: new Uint8Array([9, 8, 7, 6, 5]),
          ownerId: null,
        })
        .wait({ tier: "edge" }),
      10_000,
      "mixed memory writer insert was not accepted at the server",
    );

    const [rowOnReader] = await waitForRichTodos(
      reader,
      richQuery,
      (todos) => todos.length === 1 && todos[0]?.id === created.id,
      "mixed persistent reader websocket convergence",
    );
    expect(rowOnReader).toMatchObject({
      id: created.id,
      title: "Adopt mixed alpha boundary",
      priority: 9,
      tags: ["alpha", "mixed-boundary"],
      ownerId: null,
    });
    expect(Array.from(rowOnReader.payload ?? [])).toEqual([9, 8, 7, 6, 5]);
    expect(snapshots.some((rows) => rows.some((todo) => todo.id === created.id))).toBe(true);

    unsubscribe();
    await reader.shutdown();
    ctx.untrack(reader);

    reader = await openAlphaDb(appId, serverUrl, adminSecret, readerDbName, sharedSecret, {
      uniqueLabel: false,
    });
    const [reopenedRow] = await waitForQuery(
      reader,
      richQuery,
      (todos) => todos.length === 1 && todos[0]?.id === created.id,
      "mixed persistent reader local reopen",
      15_000,
      "local",
    );
    expect(reopenedRow).toMatchObject({
      id: created.id,
      title: "Adopt mixed alpha boundary",
      priority: 9,
      tags: ["alpha", "mixed-boundary"],
      ownerId: null,
    });
    expect(Array.from(reopenedRow.payload ?? [])).toEqual([9, 8, 7, 6, 5]);
  });

  it("reopens a public persistent websocket client and catches up writes made while offline", async () => {
    const requestedAppId = uniqueDbName("alpha-public-reconnect-canary");
    const { appId, serverUrl, adminSecret } = await getJazzServerInfo(requestedAppId);
    await publishSchemaAndPermissions(appId, serverUrl, adminSecret, permissions);

    const sharedSecret = generateAuthSecret();
    const readerDbName = uniqueDbName("alpha-public-reconnect-reader-opfs");
    const writer = await openAlphaMemoryDb(appId, serverUrl, adminSecret, sharedSecret);
    let reader = await openAlphaDb(appId, serverUrl, adminSecret, readerDbName, sharedSecret, {
      uniqueLabel: false,
    });

    const initial = await withTimeout(
      writer
        .insert(app.todos, {
          title: "Online before alpha reconnect",
          done: false,
          list: "reconnect",
        })
        .wait({ tier: "edge" }),
      10_000,
      "online insert before reconnect was not accepted at the server",
    );
    expect(
      await waitForSubscribedTodoSummaries(reader, ["Online before alpha reconnect:open"]),
    ).toEqual([initial]);

    await reader.shutdown();
    ctx.untrack(reader);

    const offlineWrite = await withTimeout(
      writer
        .insert(app.todos, {
          title: "Written while alpha client offline",
          done: true,
          list: "reconnect",
        })
        .wait({ tier: "edge" }),
      10_000,
      "offline-window insert was not accepted at the server",
    );

    reader = await openAlphaDb(appId, serverUrl, adminSecret, readerDbName, sharedSecret, {
      uniqueLabel: false,
    });

    const summaries = [
      "Online before alpha reconnect:open",
      "Written while alpha client offline:done",
    ];
    const subscribedRows = await waitForSubscribedTodoSummaries(reader, summaries);
    expect(subscribedRows.map((todo) => todo.id).sort()).toEqual(
      [initial.id, offlineWrite.id].sort(),
    );

    const allRows = await waitForQuery(
      reader,
      app.todos.orderBy("title"),
      (todos) => summariesEqual(todos, summaries),
      "reopened public websocket client catches up via all",
      15_000,
      "local",
    );
    expect(allRows).toEqual([initial, offlineWrite]);
    expect(await reader.one(app.todos.where({ id: offlineWrite.id }), { tier: "local" })).toEqual(
      offlineWrite,
    );
  });

  it("opens public createDb with persistent OPFS and websocket server config, then converges todo CRUD", async () => {
    const requestedAppId = uniqueDbName("alpha-public-flow");
    const { appId, serverUrl, adminSecret } = await getJazzServerInfo(requestedAppId);
    await publishSchemaAndPermissions(appId, serverUrl, adminSecret, permissions);

    const sharedSecret = generateAuthSecret();
    const persistentDbName = uniqueDbName("alpha-public-opfs");
    let db = await openAlphaDb(appId, serverUrl, adminSecret, persistentDbName, sharedSecret, {
      uniqueLabel: false,
    });
    const snapshots: Todo[][] = [];
    const unsubscribe = ctx.trackSubscription(
      db.subscribeAll(app.todos.orderBy("title"), (delta) => {
        snapshots.push([...delta.all]);
      }),
    );

    const created = db.insert(app.todos, {
      title: "Adopt alpha public flow",
      done: false,
      list: "launch",
    });
    const createdRow = await withTimeout(
      created.wait({ tier: "edge" }),
      10_000,
      "initial insert was not accepted at the server",
    );
    await expectTodoTitles(db, snapshots, ["Adopt alpha public flow"]);

    const second = db.insert(app.todos, {
      title: "Prove public imports",
      done: false,
      list: "launch",
    });
    const secondRow = await withTimeout(
      second.wait({ tier: "edge" }),
      10_000,
      "second insert was not accepted at the server",
    );
    await withTimeout(
      db.update(app.todos, createdRow.id, { done: true }).wait({ tier: "edge" }),
      10_000,
      "update was not accepted at the server",
    );
    expect(
      await db.one(app.todos.where({ id: createdRow.id }), {
        tier: "edge",
      }),
    ).toEqual({
      id: createdRow.id,
      title: "Adopt alpha public flow",
      done: true,
      list: "launch",
    });
    await expectTodoSummaries(db, ["Adopt alpha public flow:done", "Prove public imports:open"]);

    await withTimeout(
      db.delete(app.todos, secondRow.id).wait({ tier: "edge" }),
      10_000,
      "delete was not accepted at the server",
    );
    await expectTodoSummaries(db, ["Adopt alpha public flow:done"]);

    expect(
      await db.one(app.todos.where({ id: secondRow.id }), {
        tier: "edge",
      }),
    ).toBeNull();

    unsubscribe();
    expect(
      snapshots.some(
        (rows) => rows.length === 1 && rows[0]?.title === "Adopt alpha public flow" && rows[0].done,
      ),
    ).toBe(true);

    await db.shutdown();
    ctx.untrack(db);

    db = await openAlphaDb(appId, serverUrl, adminSecret, persistentDbName, sharedSecret, {
      uniqueLabel: false,
    });
    expect(
      await db.one(app.todos.where({ id: createdRow.id }), {
        tier: "local",
      }),
    ).toEqual({
      id: createdRow.id,
      title: "Adopt alpha public flow",
      done: true,
      list: "launch",
    });

    const dbB = await openAlphaDb(appId, serverUrl, adminSecret, "alpha-public-b", sharedSecret);
    const rowsOnB = await waitForSubscribedTodoSummaries(dbB, ["Adopt alpha public flow:done"]);
    expect(rowsOnB.some((todo) => todo.id === createdRow.id && todo.done)).toBe(true);
    expect((await dbB.all(app.todos)).some((todo) => todo.id === secondRow.id)).toBe(false);
  });

  it("keeps deleted rows hidden by default and restores them over websocket", async () => {
    const requestedAppId = uniqueDbName("alpha-public-delete-restore");
    const { appId, serverUrl, adminSecret } = await getJazzServerInfo(requestedAppId);
    await publishSchemaAndPermissions(appId, serverUrl, adminSecret, permissions);

    const sharedSecret = generateAuthSecret();
    const db = await openAlphaDb(
      appId,
      serverUrl,
      adminSecret,
      "alpha-public-delete-a",
      sharedSecret,
    );
    const todo = await withTimeout(
      db
        .insert(app.todos, {
          title: "Restore alpha public flow",
          done: false,
          list: "tombstones",
        })
        .wait({ tier: "edge" }),
      10_000,
      "insert before delete was not accepted at the server",
    );

    await withTimeout(
      db.delete(app.todos, todo.id).wait({ tier: "edge" }),
      10_000,
      "delete was not accepted at the server",
    );

    await waitForQuery(
      db,
      app.todos.where({ id: todo.id }),
      (todos) => todos.length === 0,
      "deleted todo is hidden from default reads",
      15_000,
      "edge",
    );
    const restored = await withTimeout(
      db
        .restore(app.todos, todo.id, {
          title: "Restored alpha public flow",
          done: true,
          list: "tombstones",
        })
        .wait({ tier: "edge" }),
      10_000,
      "restore was not accepted at the server",
    );
    expect(restored).toEqual({
      id: todo.id,
      title: "Restored alpha public flow",
      done: true,
      list: "tombstones",
    });

    const dbB = await openAlphaDb(
      appId,
      serverUrl,
      adminSecret,
      "alpha-public-delete-b",
      sharedSecret,
    );
    const rowsOnB = await waitForSubscribedTodoSummaries(dbB, ["Restored alpha public flow:done"]);
    expect(rowsOnB).toEqual([restored]);
  });

  it("exposes edge-confirmed browser deletes through includeDeleted over websocket", async () => {
    const requestedAppId = uniqueDbName("alpha-public-include-deleted");
    const { appId, serverUrl, adminSecret } = await getJazzServerInfo(requestedAppId);
    await publishSchemaAndPermissions(appId, serverUrl, adminSecret, permissions);

    const db = await openAlphaDb(
      appId,
      serverUrl,
      adminSecret,
      "alpha-public-include-deleted",
      generateAuthSecret(),
    );
    const todo = await db
      .insert(app.todos, {
        title: "Include deleted alpha public flow",
        done: false,
        list: "tombstones",
      })
      .wait({ tier: "edge" });
    await db.delete(app.todos, todo.id).wait({ tier: "edge" });

    const [deletedTodo] = await waitForQuery(
      db,
      app.todos.includeDeleted().where({ id: todo.id }),
      (todos) => todos.length === 1,
      "deleted todo is visible with includeDeleted",
      15_000,
      "edge",
    );
    expect(deletedTodo).toEqual(todo);
    expect(Object.keys(deletedTodo).includes("deleted")).toBe(false);
  });

  it("opens public file/blob helpers with persistent OPFS and websocket server config, then converges file rows", async () => {
    const requestedAppId = uniqueDbName("alpha-public-file-flow");
    const { appId, serverUrl, adminSecret } = await getJazzServerInfo(requestedAppId);
    await publishSchemaAndPermissions(appId, serverUrl, adminSecret, filePermissions, fileApp);

    const sharedSecret = generateAuthSecret();
    const persistentDbName = uniqueDbName("alpha-public-file-opfs");
    const sourceBytes = makeLargeProbeBytes();
    const sourceBlob = new Blob([sourceBytes], { type: "application/x-jazz-probe" });
    const sourceFile = new File([sourceBlob], "probe.bin", { type: sourceBlob.type });

    const db = await openAlphaDb(appId, serverUrl, adminSecret, persistentDbName, sharedSecret, {
      uniqueLabel: false,
    });
    const file = await withTimeout(
      db.createFileFromBlob(fileApp, sourceFile, {
        tier: "edge",
      }),
      20_000,
      "file blob chunks were not accepted at the server",
    );

    expect(file.name).toBe("probe.bin");
    expect(file.mime_type).toBe("application/x-jazz-probe");
    expect(Array.from(file.data)).toEqual(Array.from(sourceBytes));

    await withTimeout(
      waitForFileRecord(db, file.id),
      20_000,
      "created file metadata was not readable locally",
    );

    await db.shutdown();
    ctx.untrack(db);

    const reopenedDb = await openAlphaDb(
      appId,
      serverUrl,
      adminSecret,
      persistentDbName,
      sharedSecret,
      { uniqueLabel: false },
    );
    await withTimeout(
      waitForFileRecord(reopenedDb, file.id),
      20_000,
      "file metadata did not reload from persistent OPFS after reopen",
    );
    const reopenedBlob = await withTimeout(
      reopenedDb.loadFileAsBlob(fileApp, file.id, { tier: "local" }),
      10_000,
      "file was not readable from persistent OPFS after reopen",
    );
    expect(reopenedBlob.type).toBe("application/x-jazz-probe");
    await expectBlobBytes(reopenedBlob, sourceBytes);

    const secondDb = await openAlphaDb(
      appId,
      serverUrl,
      adminSecret,
      "alpha-public-file-b",
      sharedSecret,
    );
    await withTimeout(
      waitForSubscribedFileRecord(secondDb, file.id),
      20_000,
      "file metadata did not converge to the second websocket client",
    );
    const secondClientBlob = await withTimeout(
      secondDb.loadFileAsBlob(fileApp, file.id, { tier: "edge" }),
      20_000,
      "file was not readable from second websocket client",
    );
    expect(secondClientBlob.type).toBe("application/x-jazz-probe");
    await expectBlobBytes(secondClientBlob, sourceBytes);
  });
});

async function openAlphaDb(
  appId: string,
  serverUrl: string,
  adminSecret: string,
  label: string,
  secret: string,
  options: { uniqueLabel?: boolean } = {},
): Promise<Db> {
  return ctx.track(
    await createDb({
      appId,
      serverUrl,
      adminSecret,
      secret,
      driver: {
        type: "persistent",
        dbName: options.uniqueLabel === false ? label : uniqueDbName(label),
      },
    }),
  );
}

async function openAlphaMemoryDb(
  appId: string,
  serverUrl: string,
  adminSecret: string,
  secret: string,
): Promise<Db> {
  return ctx.track(
    await createDb({
      appId,
      serverUrl,
      adminSecret,
      secret,
      driver: { type: "memory" },
    }),
  );
}

async function publishSchemaAndPermissions(
  appId: string,
  serverUrl: string,
  adminSecret: string,
  permissions: CompiledPermissions,
  schemaApp: { wasmSchema: typeof app.wasmSchema } = app,
): Promise<void> {
  const { hash: schemaHash } = await publishStoredSchema(serverUrl, {
    appId,
    adminSecret,
    schema: schemaApp.wasmSchema,
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

async function expectTodoTitles(db: Db, snapshots: Todo[][], titles: string[]): Promise<void> {
  const rows = await waitForQuery(
    db,
    app.todos.orderBy("title"),
    (todos) => titlesEqual(todos, titles),
    `todos converge to ${titles.join(", ")}`,
    15_000,
  );
  expect(rows.map((todo) => todo.title)).toEqual(titles);
  await waitForCondition(
    async () => snapshots.some((todos) => titlesEqual(todos, titles)),
    5_000,
    `subscription snapshot for ${titles.join(", ")}`,
  );
}

async function expectTodoSummaries(
  db: Db,
  summaries: string[],
  tier?: "local" | "edge",
): Promise<void> {
  await expectTodoSummariesForQuery(db, app.todos.orderBy("title"), summaries, tier);
}

async function expectTodoSummariesForQuery(
  db: Db,
  query: Query<"todos">,
  summaries: string[],
  tier?: "local" | "edge",
): Promise<void> {
  const rows = await waitForQuery(
    db,
    query,
    (todos) => summariesEqual([...todos].sort(byTitle), summaries),
    `todos converge to ${summaries.join(", ")}`,
    15_000,
    tier,
  );
  expect([...rows].sort(byTitle).map(summary)).toEqual(summaries);
}

async function waitForSnapshotSummaries(
  snapshots: Todo[][],
  summaries: string[],
  label: string,
): Promise<void> {
  await waitForCondition(
    async () => snapshots.some((todos) => summariesEqual([...todos].sort(byTitle), summaries)),
    5_000,
    `subscription snapshot for ${label}`,
  );
}

async function waitForRowChange(
  changes: Array<RowDelta<Todo>>,
  predicate: (change: RowDelta<Todo>) => boolean,
  label: string,
): Promise<void> {
  await waitForCondition(
    async () => changes.some(predicate),
    5_000,
    `subscription row delta for ${label}`,
  );
}

async function waitForSubscribedTodoSummaries(
  db: Db,
  summaries: string[],
  query: Query<"todos"> = app.todos.orderBy("title"),
): Promise<Todo[]> {
  return await new Promise<Todo[]>((resolve, reject) => {
    let lastRows: Todo[] = [];
    let unsubscribe: () => void = () => {};
    const timeout = setTimeout(() => {
      unsubscribe();
      reject(
        new Error(
          `subscription snapshot for ${summaries.join(", ")} timed out; ` +
            `lastRows=${JSON.stringify(lastRows.slice(0, 10))}`,
        ),
      );
    }, 15_000);
    unsubscribe = ctx.trackSubscription(
      db.subscribeAll(query, (delta) => {
        lastRows = [...delta.all];
        if (summariesEqual([...lastRows].sort(byTitle), summaries)) {
          clearTimeout(timeout);
          unsubscribe();
          resolve(lastRows);
        }
      }),
    );
  });
}

async function waitForRichTodos(
  db: Db,
  query: Query<"todos">,
  predicate: (todos: RichTodo[]) => boolean,
  label: string,
): Promise<RichTodo[]> {
  return await waitForQuery(db, query, predicate, label, 15_000, "edge");
}

function titlesEqual(rows: Todo[], titles: string[]): boolean {
  return rows.map((todo) => todo.title).join("\n") === titles.join("\n");
}

function summariesEqual(rows: Todo[], summaries: string[]): boolean {
  return rows.map(summary).join("\n") === summaries.join("\n");
}

function summary(todo: Todo): string {
  return `${todo.title}:${todo.done ? "done" : "open"}`;
}

function byTitle(left: Todo, right: Todo): number {
  return left.title.localeCompare(right.title);
}

async function waitForFileRecord(db: Db, fileId: string): Promise<void> {
  await waitForQuery(
    db,
    fileApp.files.where({ id: fileId }),
    (files) => files.length === 1,
    `file ${fileId}`,
    15_000,
    "local",
  );
}

async function waitForSubscribedFileRecord(db: Db, fileId: string): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    let lastRows: Array<{ id: string }> = [];
    let unsubscribe: () => void = () => {};
    const timeout = setTimeout(() => {
      unsubscribe();
      reject(
        new Error(
          `file metadata subscription timed out for ${fileId}; ` +
            `lastRows=${JSON.stringify(lastRows.slice(0, 10))}`,
        ),
      );
    }, 15_000);
    unsubscribe = ctx.trackSubscription(
      db.subscribeAll(fileApp.files.where({ id: fileId }), (delta) => {
        lastRows = [...delta.all];
        if (lastRows.length === 1) {
          clearTimeout(timeout);
          unsubscribe();
          resolve();
        }
      }),
    );
  });
}

async function expectBlobBytes(blob: Blob, expected: Uint8Array): Promise<void> {
  const actual = new Uint8Array(await blob.arrayBuffer());
  expect(actual.length).toBe(expected.length);
  expect(Array.from(actual)).toEqual(Array.from(expected));
}

function makeLargeProbeBytes(): Uint8Array {
  const bytes = new Uint8Array(170_000);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = (index * 31 + (index >>> 8)) % 256;
  }
  return bytes;
}
