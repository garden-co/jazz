/// <reference types="vite/client" />

/**
 * Browser integration tests for the SharedWorker + IndexedDB runtime.
 *
 * Runs in a real Chromium browser via @vitest/browser + playwright.
 * Uses real jazz-wasm, a real SharedWorker, and real IndexedDB storage.
 *
 * Server sync tests use a real jazz-tools server spawned by global-setup.
 *
 * Part 2 of the bridge suite: local and server sync, subscriptions,
 * transaction identities and write rejection. See worker-bridge.test.ts.
 */

import { describe, it, expect, vi } from "vitest";
import {
  createBrowserTestDb as createDb,
  acquireBrowserTestAccount,
  createSyncedDb,
  sleep,
  uniqueDbName,
  waitForCondition,
  withTimeout,
} from "./support.js";
import { createDb as createPublicDb } from "../../src/runtime/default-create-db.js";
import { generateAuthSecret } from "../../src/runtime/auth-secret-store.js";
import { getJazzServerJwtForUser } from "./testing-server.js";
import { type BrowserInspectorControlRequest } from "../../src/runtime/native-runtime/browser-worker-protocol.js";
import {
  listWorkerContexts,
  listWorkerLifecycle,
  app,
  projects,
  todos,
  Todo,
  maintainedIndexedApp,
  maintainedIndexedTodos,
  MaintainedIndexedTodo,
  transactionIdentityApp,
  transactionIdentityPermissions,
  readOnlyPermissions,
  recoveryTerminalPermissions,
  allTodos,
  todosByProject,
  waitForTodos,
  publishSyncServerSchemaAndPermissions,
  publishPermissionsForServer,
  useSharedWorkerBridgeHarness,
} from "./worker-bridge-harness.js";

describe("SharedWorker bridge with IndexedDB", () => {
  const { ctx, track, trackSubscription, untrack, shutdownDbAndWorker } =
    useSharedWorkerBridgeHarness();

  // -------------------------------------------------------------------------
  // 5. Durable insert resolves at local tier
  // -------------------------------------------------------------------------

  it("insert resolves when local acks", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("with-ack") },
      }),
    );

    // insert("local") should resolve once the worker persistence has it
    const result = db.insert(todos, { title: "Durable", done: false });
    await result.wait({ tier: "local" });
  });

  // -------------------------------------------------------------------------
  // 6. Subscription through worker bridge
  // -------------------------------------------------------------------------

  it("subscriptions fire on insert", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("subscribe") },
      }),
    );

    const received: Todo[][] = [];

    const unsub = trackSubscription(
      db.subscribe(allTodos, (rows) => {
        received.push(rows);
      }),
    );

    db.insert(todos, { title: "Observed", done: false });

    // Wait for subscription to fire
    await waitForCondition(
      async () => received.some((r) => r.length > 0),
      3000,
      "Subscription should fire after insert",
    );

    const last = received[received.length - 1];
    expect(last.length).toBe(1);
    expect(last[0].title).toBe("Observed");

    unsub();
  });

  it("subscriptions fire when using queries with filters", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("subscribe") },
      }),
    );

    const received: Todo[][] = [];

    const {
      value: { id: projectId },
    } = db.insert(projects, { name: "Observed Project" });
    const unsub = trackSubscription(
      db.subscribe(todosByProject(projectId), (rows) => {
        received.push(rows);
      }),
    );

    db.insert(todos, { title: "Observed", done: false, projectId });
    const {
      value: { id: anotherProjectId },
    } = db.insert(projects, { name: "Ignored Project" });
    db.insert(todos, {
      title: "Not observed",
      done: false,
      projectId: anotherProjectId,
    });

    // Wait for subscription to fire
    await waitForCondition(
      async () => received.some((r) => r.length > 0),
      3000,
      "Subscription should fire after insert",
    );

    const last = received[received.length - 1];
    expect(last.length).toBe(1);
    expect(last[0].title).toBe("Observed");

    unsub();
  });

  it("maintains an IndexedDB-backed equality window across a tombstone", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: {
          type: "persistent",
          dbName: uniqueDbName("maintained-equality-window-tombstone"),
        },
      }),
    );
    const openWindow = app.todos.where({ done: false }).orderBy("title", "asc").limit(2);
    const snapshots: Todo[][] = [];
    const unsubscribe = trackSubscription(
      db.subscribe(openWindow, (rows) => snapshots.push(rows), { tier: "local" }),
    );

    const alpha = await db.insert(todos, { title: "alpha", done: false }).wait({ tier: "local" });
    await db.insert(todos, { title: "bravo", done: false }).wait({ tier: "local" });
    await db.insert(todos, { title: "charlie", done: false }).wait({ tier: "local" });

    await waitForCondition(
      async () =>
        snapshots.some((rows) => rows.map((row) => row.title).join(",") === "alpha,bravo"),
      8_000,
      "maintained equality query should retain its ordered local window",
    );

    await db.delete(todos, alpha.id).wait({ tier: "local" });
    await waitForCondition(
      async () =>
        snapshots.some((rows) => rows.map((row) => row.title).join(",") === "bravo,charlie"),
      8_000,
      "tombstoning the first window member should promote the next equality match",
    );
    expect((await db.all(openWindow, { tier: "local" })).map((row) => row.title)).toEqual([
      "bravo",
      "charlie",
    ]);
    unsubscribe();
  });

  it("tiered subscriptions gate the first callback until the worker's settled snapshot content is local", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions("subscribe-global-gated");
    const sharedLocalAuthToken = generateAuthSecret();
    const seeder = track(
      await createDb({
        appId: syncServer.appId,
        driver: {
          type: "persistent",
          dbName: uniqueDbName("subscribe-global-gated-seeder"),
        },
        serverUrl: syncServer.serverUrl,
        secret: sharedLocalAuthToken,
      }),
    );

    const {
      value: { id: projectId },
    } = seeder.insert(projects, { name: `server-project-${Date.now()}` });
    await seeder.all(app.projects.where({ id: projectId }), { tier: "global" });

    const expectedTitles: string[] = [];
    for (let i = 0; i < 12; i += 1) {
      const title = `server-seeded-${i}`;
      expectedTitles.push(title);
      await seeder.insert(todos, { title, done: i % 2 === 0, projectId }).wait({ tier: "global" });
    }
    await seeder.shutdown();
    ctx.untrack(seeder);

    const fresh = track(
      await createDb({
        appId: syncServer.appId,
        driver: {
          type: "persistent",
          dbName: uniqueDbName("subscribe-global-gated-fresh"),
        },
        serverUrl: syncServer.serverUrl,
        secret: sharedLocalAuthToken,
      }),
    );
    const snapshots: Todo[][] = [];
    const unsubscribe = trackSubscription(
      fresh.subscribe(
        todosByProject(projectId),
        (rows) => {
          snapshots.push(rows);
        },
        { tier: "global" },
      ),
    );

    await waitForCondition(
      async () => snapshots.some((snapshot) => snapshot.length === expectedTitles.length),
      15000,
      "global tier subscription should deliver the settled snapshot",
    );

    const firstSnapshot = snapshots[0];
    expect(firstSnapshot).toHaveLength(expectedTitles.length);
    expect(firstSnapshot.map((row) => row.title).sort()).toEqual([...expectedTitles].sort());

    unsubscribe();
  }, 90000);

  /**
   * The browser worker uses the same maintained indexed source as native
   * subscriptions.  A fresh remote reader must hydrate its settled snapshot,
   * keep an empty intersected equality source live for its first insert, and
   * apply enter/leave changes when either equality changes.
   *
   * writer ──global write──► server ──settled indexed source──► fresh worker
   *                                                         └──► subscription
   */
  it("maintains indexed remote subscriptions through IndexedDB worker hydration", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions(
      "maintained-indexed-worker-hydration",
      undefined,
      maintainedIndexedApp.wasmSchema,
    );
    const sharedLocalAuthToken = generateAuthSecret();
    const writer = track(
      await createDb({
        appId: syncServer.appId,
        driver: {
          type: "persistent",
          dbName: uniqueDbName("maintained-indexed-worker-writer"),
        },
        serverUrl: syncServer.serverUrl,
        secret: sharedLocalAuthToken,
      }),
    );
    const seededTitle = `indexed-seeded-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const seededFirst = await writer
      .insert(maintainedIndexedTodos, { title: seededTitle, done: false })
      .wait({ tier: "global" });
    const seededSecond = await writer
      .insert(maintainedIndexedTodos, { title: seededTitle, done: false })
      .wait({ tier: "global" });
    const expectedSeededIds = [seededFirst.id, seededSecond.id].sort();

    const fresh = track(
      await createDb({
        appId: syncServer.appId,
        driver: {
          type: "persistent",
          dbName: uniqueDbName("maintained-indexed-worker-fresh"),
        },
        serverUrl: syncServer.serverUrl,
        secret: sharedLocalAuthToken,
      }),
    );

    const settledSnapshots: MaintainedIndexedTodo[][] = [];
    const stopSettled = trackSubscription(
      fresh.subscribe(
        maintainedIndexedTodos.where({ title: seededTitle, done: false }),
        (rows) => settledSnapshots.push(rows),
        { tier: "global" },
      ),
    );
    await waitForCondition(
      async () => settledSnapshots.length > 0,
      15_000,
      "fresh IndexedDB worker must receive an authoritative settled indexed snapshot",
    );
    // The first global callback is authoritative, not merely nonempty: a
    // partial hydration that delivers either matching row is a failure.
    expect(settledSnapshots[0]).toHaveLength(2);
    expect(settledSnapshots[0]?.map((row) => row.id).sort()).toEqual(expectedSeededIds);
    expect(settledSnapshots[0]?.every((row) => row.title === seededTitle && !row.done)).toBe(true);
    // Keep the opening live long enough to catch a duplicated, partial, or
    // stale settled snapshot before retiring this subscription.
    await sleep(500);
    expect(settledSnapshots).toHaveLength(1);
    stopSettled();

    const emptyTitle = `indexed-empty-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const emptySnapshots: MaintainedIndexedTodo[][] = [];
    const stopEmpty = trackSubscription(
      fresh.subscribe(
        maintainedIndexedTodos.where({ title: emptyTitle, done: false }),
        (rows) => emptySnapshots.push(rows),
        {
          tier: "global",
        },
      ),
    );
    await waitForCondition(
      async () => emptySnapshots.some((rows) => rows.length === 0),
      15_000,
      "empty intersected equality source must settle before its first matching insert",
    );
    await writer
      .insert(maintainedIndexedTodos, { title: emptyTitle, done: false })
      .wait({ tier: "global" });
    await waitForCondition(
      async () =>
        emptySnapshots.some(
          (rows) => rows.length === 1 && rows[0]?.title === emptyTitle && rows[0]?.done === false,
        ),
      15_000,
      "first matching remote insert must enter an initially empty indexed subscription",
    );
    await sleep(500);
    expect(
      emptySnapshots.filter(
        (rows) => rows.length === 1 && rows[0]?.title === emptyTitle && rows[0]?.done === false,
      ),
    ).toHaveLength(1);
    stopEmpty();

    const transitionTitle = `indexed-transition-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const transition = await writer
      .insert(maintainedIndexedTodos, { title: transitionTitle, done: true })
      .wait({ tier: "global" });
    const transitionSnapshots: MaintainedIndexedTodo[][] = [];
    const stopTransition = trackSubscription(
      fresh.subscribe(
        maintainedIndexedTodos.where({ title: transitionTitle, done: false }),
        (rows) => transitionSnapshots.push(rows),
        { tier: "global" },
      ),
    );
    const matchingTransition = { id: transition.id, title: transitionTitle, done: false };
    const expectedTransitionSnapshots: { id: string; title: string; done: boolean }[][] = [];
    const assertTransitionSnapshots = () => {
      expect(
        transitionSnapshots.map((rows) => rows.map(({ id, title, done }) => ({ id, title, done }))),
      ).toEqual(expectedTransitionSnapshots);
    };
    const awaitTransitionSnapshot = async (
      cursor: number,
      expectedRows: { id: string; title: string; done: boolean }[],
      message: string,
    ) => {
      await waitForCondition(async () => transitionSnapshots.length > cursor, 15_000, message);
      expectedTransitionSnapshots.push(expectedRows);
      assertTransitionSnapshots();
    };
    await awaitTransitionSnapshot(
      0,
      [],
      "non-matching indexed row must not appear in the initial intersected snapshot",
    );
    let transitionCursor = transitionSnapshots.length;
    await writer
      .update(maintainedIndexedTodos, transition.id, { done: false })
      .wait({ tier: "global" });
    await awaitTransitionSnapshot(
      transitionCursor,
      [matchingTransition],
      "changing an indexed equality must enter the remote maintained subscription",
    );
    transitionCursor = transitionSnapshots.length;
    await writer
      .update(maintainedIndexedTodos, transition.id, { title: `${transitionTitle}-outside` })
      .wait({ tier: "global" });
    await awaitTransitionSnapshot(
      transitionCursor,
      [],
      "changing title equality must leave the remote maintained subscription",
    );
    transitionCursor = transitionSnapshots.length;
    await writer
      .update(maintainedIndexedTodos, transition.id, { title: transitionTitle })
      .wait({ tier: "global" });
    await awaitTransitionSnapshot(
      transitionCursor,
      [matchingTransition],
      "restoring title equality must re-enter the remote maintained subscription",
    );
    transitionCursor = transitionSnapshots.length;
    await writer
      .update(maintainedIndexedTodos, transition.id, { done: true })
      .wait({ tier: "global" });
    await awaitTransitionSnapshot(
      transitionCursor,
      [],
      "changing done equality back must leave the remote maintained subscription",
    );
    await sleep(500);
    assertTransitionSnapshots();
    stopTransition();
  }, 120000);

  it("delivers an initial scoped subscription snapshot after seeding many synced rows", async () => {
    const sharedLocalAuthToken = generateAuthSecret();
    const syncServer = await publishSyncServerSchemaAndPermissions("subscribe-initial-snapshot");
    const db = await createSyncedDb(
      ctx,
      "subscribe-initial-snapshot",
      sharedLocalAuthToken,
      syncServer,
    );

    const insertedIds: string[] = [];
    for (let i = 0; i < 120; i += 1) {
      const { id } = await db
        .insert(todos, { title: `seeded-${i}`, done: i % 2 === 0 })
        .wait({ tier: "local" });
      insertedIds.push(id);
    }

    const targetId = insertedIds[0];
    const received: Todo[][] = [];
    const unsub = trackSubscription(
      db.subscribe(todos.where({ id: targetId }), (rows) => {
        received.push(rows);
      }),
    );

    await waitForCondition(
      async () =>
        received.some((rows) => rows.length === 1 && rows[0]?.id === targetId && rows[0]?.title),
      8000,
      "Seeded synced row should appear in initial scoped subscription snapshot",
    );

    const last = received[received.length - 1];
    expect(last).toHaveLength(1);
    expect(last[0].id).toBe(targetId);
    expect(last[0].title).toBe("seeded-0");

    unsub();
  }, 60000);

  it("delivers an initial scoped subscription snapshot for jwt-backed synced rows", async () => {
    const { appId, serverUrl } =
      await publishSyncServerSchemaAndPermissions("subscribe-initial-jwt");
    const db = track(
      await createDb({
        appId,
        driver: {
          type: "persistent",
          dbName: uniqueDbName("subscribe-initial-jwt"),
        },
        serverUrl,
        jwtToken: await getJazzServerJwtForUser("subscribe-initial-jwt", undefined, appId),
        registerJwt: true,
      }),
    );

    const insertedIds: string[] = [];
    for (let i = 0; i < 120; i += 1) {
      const { id } = await db
        .insert(todos, { title: `seeded-jwt-${i}`, done: i % 2 === 0 })
        .wait({ tier: "local" });
      insertedIds.push(id);
    }

    const targetId = insertedIds[0];
    const received: Todo[][] = [];
    const unsub = trackSubscription(
      db.subscribe(todos.where({ id: targetId }), (rows) => {
        received.push(rows);
      }),
    );

    await waitForCondition(
      async () =>
        received.some((rows) => rows.length === 1 && rows[0]?.id === targetId && rows[0]?.title),
      8000,
      "JWT-backed seeded row should appear in initial scoped subscription snapshot",
    );

    const last = received[received.length - 1];
    expect(last).toHaveLength(1);
    expect(last[0].id).toBe(targetId);
    expect(last[0].title).toBe("seeded-jwt-0");

    unsub();
  }, 60000);

  // -------------------------------------------------------------------------
  // 7. Server sync through worker
  // -------------------------------------------------------------------------

  it("propagates synced row from client A to client B", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions("sync-a-to-b");
    const sharedLocalAuthToken = generateAuthSecret();
    const dbA = await createSyncedDb(ctx, "sync-a", sharedLocalAuthToken, syncServer);
    const dbB = await createSyncedDb(ctx, "sync-b", sharedLocalAuthToken, syncServer);

    const title = `sync-a-to-b-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    await withTimeout(
      dbA.insert(todos, { title, done: false }).wait({ tier: "local" }),
      10000,
      "A insert(worker) did not resolve",
    );

    const rowsOnB = await waitForTodos(
      dbB,
      (rows) => rows.some((row) => row.title === title),
      "A -> B propagation",
      20000,
    );
    expect(rowsOnB.some((row) => row.title === title)).toBe(true);
  }, 60000);

  it("propagates synced row from client B to client A", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions("sync-b-to-a");
    const sharedLocalAuthToken = generateAuthSecret();
    const dbA = await createSyncedDb(ctx, "sync-a-reverse", sharedLocalAuthToken, syncServer);
    const dbB = await createSyncedDb(ctx, "sync-b-reverse", sharedLocalAuthToken, syncServer);

    const title = `sync-b-to-a-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    await withTimeout(
      dbB.insert(todos, { title, done: true }).wait({ tier: "local" }),
      10000,
      "B insert(worker) did not resolve",
    );

    const rowsOnA = await waitForTodos(
      dbA,
      (rows) => rows.some((row) => row.title === title),
      "B -> A propagation",
      20000,
    );
    expect(rowsOnA.some((row) => row.title === title)).toBe(true);
  }, 60000);

  /**
   * Two fresh foreground runtimes can share a persistent worker. Each runtime
   * starts with an empty HLC register, so their first writes must not alias
   * one transaction identity when the browser gives both writes the same
   * millisecond.
   *
   * alice tab A ──insert project────────────► shared worker ──► server
   * alice tab B ──insert large branch doc───► shared worker ──► server
   */
  it("prevents foreground transaction identity aliasing in one millisecond", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions(
      "distinct-client-tx-ids",
      transactionIdentityPermissions,
      transactionIdentityApp.wasmSchema,
    );
    const secret = generateAuthSecret();
    const dbName = uniqueDbName("distinct-client-tx-ids");
    const config = {
      appId: syncServer.appId,
      serverUrl: syncServer.serverUrl,
      secret,
      driver: { type: "persistent" as const, dbName },
      schema: transactionIdentityApp,
    };
    const first = track(await createDb(config));
    const second = track(await createDb(config));
    const fixedNow = Date.now();
    const now = vi.spyOn(Date, "now").mockReturnValue(fixedNow);
    const { project, document } = (() => {
      try {
        const project = first.insert(transactionIdentityApp.projects, {
          name: "shared-worker project",
        });
        const document = second.insert(
          transactionIdentityApp.documents,
          {
            branch: "main",
            title: "first title",
            projectId: project.value.id,
            body: "large browser value ".repeat(20_000),
          },
          { branch: "main" },
        );
        return { project, document };
      } finally {
        now.mockRestore();
      }
    })();
    const projectTxId = await project.txId;
    const documentTxId = await document.txId;
    expect(documentTxId).not.toBe(projectTxId);
    await withTimeout(
      Promise.all([project.wait({ tier: "global" }), document.wait({ tier: "global" })]),
      20_000,
      "aliased foreground transactions did not both settle globally",
    );
  }, 60_000);

  /**
   * Two independent browser storage replicas can intentionally share every
   * logical input (app, schema, server, author, and first-write clock). Their
   * `dbName` is the physical-storage locator only, so each opens a separate
   * SharedWorker + Wasm + IndexedDB realm and must receive a distinct durable
   * replica node. The public TxIds are therefore distinct even at one fixed
   * first-write clock, both settle, and each replica can be reopened.
   */
  it("keeps public transaction identities distinct across physical browser replicas", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions(
      "distinct-physical-replica-tx-ids",
      transactionIdentityPermissions,
      transactionIdentityApp.wasmSchema,
    );
    const secret = generateAuthSecret();
    const firstName = uniqueDbName("physical-replica-a");
    const secondName = uniqueDbName("physical-replica-b");
    const config = (dbName: string) => ({
      appId: syncServer.appId,
      serverUrl: syncServer.serverUrl,
      secret,
      driver: { type: "persistent" as const, dbName },
      schema: transactionIdentityApp,
    });
    const [first, second] = await Promise.all([
      createDb(config(firstName)),
      createDb(config(secondName)),
    ]);
    track(first);
    track(second);

    const fixedNow = Date.now();
    const now = vi.spyOn(Date, "now").mockReturnValue(fixedNow);
    const { firstWrite, secondWrite } = (() => {
      try {
        return {
          firstWrite: first.insert(transactionIdentityApp.projects, {
            name: "physical replica a project",
          }),
          secondWrite: second.insert(transactionIdentityApp.projects, {
            name: "physical replica b project",
          }),
        };
      } finally {
        now.mockRestore();
      }
    })();
    const [firstTxId, secondTxId] = await Promise.all([firstWrite.txId, secondWrite.txId]);
    expect(firstTxId).not.toBe(secondTxId);
    await withTimeout(
      Promise.all([firstWrite.wait({ tier: "global" }), secondWrite.wait({ tier: "global" })]),
      20_000,
      "physical-replica writes did not both settle globally",
    );

    await first.shutdown();
    await second.shutdown();
    untrack(first);
    untrack(second);
    const [reopenedFirst, reopenedSecond] = await Promise.all([
      createDb(config(firstName)),
      createDb(config(secondName)),
    ]);
    track(reopenedFirst);
    track(reopenedSecond);
    await expect(
      reopenedFirst.all(transactionIdentityApp.projects, { tier: "local" }),
    ).resolves.toEqual(
      expect.arrayContaining([expect.objectContaining({ id: firstWrite.value.id })]),
    );
    await expect(
      reopenedSecond.all(transactionIdentityApp.projects, { tier: "local" }),
    ).resolves.toEqual(
      expect.arrayContaining([expect.objectContaining({ id: secondWrite.value.id })]),
    );
  }, 60_000);

  it("resolves insert wait at global tier through the worker bridge", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions("sync-wait-edge");
    const sharedLocalAuthToken = generateAuthSecret();
    const db = await createSyncedDb(ctx, "sync-wait-edge", sharedLocalAuthToken, syncServer);

    const title = `wait-edge-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const inserted = db.insert(todos, { title, done: false });
    const { value: insertedTodo } = inserted;

    await withTimeout(
      inserted.wait({ tier: "global" }),
      10000,
      "insert wait(global) did not resolve",
    );

    expect(insertedTodo.id).toBeTruthy();
    expect(insertedTodo.title).toBe(title);

    const rowsAtGlobal = await waitForTodos(
      db,
      (rows) => rows.some((row) => row.id === insertedTodo.id && row.title === title),
      "insert wait(global) row becomes queryable at global tier",
      20000,
      "global",
    );
    expect(rowsAtGlobal.some((row) => row.id === insertedTodo.id)).toBe(true);
  }, 60000);

  it("rejects backend credentials through the SharedWorker relay", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions(
      "sync-admin-write-authority",
      readOnlyPermissions,
    );
    // A browser worker is a persistent client runtime, never a trusted
    // backend. Keeping backend credentials out of it avoids handing a
    // privileged capability to browser storage or worker ports.
    await expect(
      createPublicDb({
        appId: syncServer.appId,
        serverUrl: syncServer.serverUrl,
        adminSecret: syncServer.adminSecret,
        driver: {
          type: "persistent",
          dbName: uniqueDbName("sync-admin-write-authority"),
        },
        schema: app,
      } as never),
    ).rejects.toThrow("account_handle_required");
  });

  it("server permissions check rejects client optimistic insert - wait notification", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions(
      "sync-wait-edge",
      readOnlyPermissions,
    );

    const sharedLocalAuthToken = generateAuthSecret();
    const db = await createSyncedDb(ctx, "sync-wait-edge", sharedLocalAuthToken, syncServer);

    const insertResult = db.insert(todos, { title: "Rejected", done: false });
    const txId = await insertResult.txId;
    await expect(insertResult.wait({ tier: "global" })).rejects.toMatchObject({
      name: "PersistedWriteRejectedError",
      transactionId: txId,
      code: "permission_denied",
    });

    const todosAfterRevert = await db.all(allTodos, { tier: "local" });
    expect(todosAfterRevert.length).toBe(0);
  });

  /**
   * 1. Two in-memory `Db`s attach to the same persistent browser worker.
   * 2. One DB inserts a row.
   * 3. The other DB receives the optimistic row through its subscription.
   * 4. The server rejects the transaction.
   * 5. The persistent worker rolls back.
   * 6. The writer DB rolls back.
   * 7. The other in-memory DB rolls back as well.
   */
  it("rejected write from one live peer reverts every attached peer", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions(
      "sync-cross-peer-rejection",
      readOnlyPermissions,
    );
    const secret = generateAuthSecret();
    const dbName = uniqueDbName("sync-cross-peer-rejection");
    const config = {
      appId: syncServer.appId,
      serverUrl: syncServer.serverUrl,
      secret,
      driver: { type: "persistent" as const, dbName },
      schema: app,
    };
    // Both `Db`s attach to the same persistent worker
    const appPeer = track(await createDb(config));
    const writerPeer = track(await createDb(config));

    await Promise.all([
      appPeer.all(allTodos, { tier: "global" }),
      writerPeer.all(allTodos, { tier: "global" }),
    ]);
    // Disconnect from server so both in-memory `Db`s receive the optimistic insert
    // before the server rejection
    await appPeer.disconnect();

    const rejected = writerPeer.insert(todos, {
      title: "Rejected from the other peer",
      done: false,
    });
    await rejected.wait({ tier: "local" });
    await waitForCondition(
      async () => (await appPeer.all(allTodos, { tier: "local" })).length === 1,
      5000,
      "non-originating app peer should observe the optimistic insert",
    );

    await appPeer.reconnect();
    await expect(rejected.wait({ tier: "global" })).rejects.toMatchObject({
      name: "PersistedWriteRejectedError",
      code: "permission_denied",
    });
    expect(await writerPeer.all(allTodos, { tier: "local" })).toEqual([]);
    expect(await appPeer.all(allTodos, { tier: "global" })).toEqual([]);
    await waitForCondition(
      async () => (await appPeer.all(allTodos, { tier: "local" })).length === 0,
      5000,
      "non-originating app peer should receive the rejection rollback",
    );
  });

  it("server permissions check rejects client optimistic insert - onMutationError notification", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions(
      "sync-wait-edge",
      readOnlyPermissions,
    );

    const sharedLocalAuthToken = generateAuthSecret();
    const db = await createSyncedDb(ctx, "sync-wait-edge", sharedLocalAuthToken, syncServer);

    const mutationErrorSpy = vi.fn();
    db.onMutationError(mutationErrorSpy);

    const insertResult = db.insert(todos, { title: "Rejected", done: false });
    const txId = await insertResult.txId;
    try {
      await waitForCondition(
        async () => mutationErrorSpy.mock.calls.length > 0,
        5000,
        "onMutationError handler should be called",
      );
    } catch (error) {
      console.error("[mutation notification failure]", {
        transactionAllocated: txId !== undefined,
        callbackCount: mutationErrorSpy.mock.calls.length,
      });
      // #2677: Read the existing redacted ledger only after failure. A global-tier wait
      // would consume rejection handling and change the behavior under test.
      const inspection = (async () => {
        const port = await db.openInspectorControlPort();
        port.start();
        try {
          return await withTimeout(listWorkerLifecycle(port), 1000, "worker lifecycle reply");
        } finally {
          port.postMessage({ type: "close" } satisfies BrowserInspectorControlRequest);
          port.close();
        }
      })();
      try {
        console.error(
          "[mutation notification worker lifecycle]",
          await withTimeout(inspection, 1500, "worker lifecycle inspection"),
        );
      } catch (diagnosticError) {
        console.error("[mutation notification inspection unavailable]", String(diagnosticError));
      }
      throw error;
    }
    expect(mutationErrorSpy).toHaveBeenCalledWith({
      code: "permission_denied",
      reason: "Write rejected by server authorization",
      transaction: {
        transactionId: txId,
        kind: "mergeable",
        sealed: true,
        latestSettlement: {
          kind: "rejected",
          transactionId: txId,
          code: "permission_denied",
          reason: "Write rejected by server authorization",
        },
      },
    });
    expect(mutationErrorSpy).toHaveBeenCalledTimes(1);

    const todosAfterRevert = await db.all(allTodos, { tier: "local" });
    expect(todosAfterRevert.length).toBe(0);
  });

  it("wait() prevents onMutationError handler from firing", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions(
      "sync-wait-edge",
      readOnlyPermissions,
    );

    const sharedLocalAuthToken = generateAuthSecret();
    const db = await createSyncedDb(ctx, "sync-wait-edge", sharedLocalAuthToken, syncServer);

    const mutationErrorSpy = vi.fn();
    db.onMutationError(mutationErrorSpy);

    const insertResult = db.insert(todos, { title: "Rejected", done: false });
    await expect(insertResult.wait({ tier: "global" })).rejects.toMatchObject({
      name: "PersistedWriteRejectedError",
      transactionId: insertResult.txId,
      code: "permission_denied",
    });
    expect(mutationErrorSpy).not.toHaveBeenCalled();
  });

  it("does not send a live rejection to a runtime attached after its originating peer closes", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions(
      "sync-on-mutation-error-restart",
    );

    const dbName = uniqueDbName("sync-on-mutation-error-restart");
    const account = await acquireBrowserTestAccount({
      appId: syncServer.appId,
      serverUrl: syncServer.serverUrl,
      key: dbName,
    });
    const createPersistentDb = (serverUrl?: string) =>
      createDb({
        appId: syncServer.appId,
        driver: { type: "persistent" as const, dbName },
        serverUrl,
        account,
      });

    const dbBeforeRestart = track(await createPersistentDb(syncServer.serverUrl));
    const durableControl = dbBeforeRestart.insert(todos, {
      title: "Durable control across rejection restart",
      done: false,
    });
    await durableControl.wait({ tier: "global" });
    await publishPermissionsForServer(syncServer, readOnlyPermissions);

    const mutationErrorSpy = vi.fn();
    dbBeforeRestart.onMutationError(mutationErrorSpy);

    const insertResult = dbBeforeRestart.insert(todos, {
      title: "Rejected across restart",
      done: false,
    });
    const txId = await insertResult.txId;

    await waitForCondition(
      async () => mutationErrorSpy.mock.calls.length > 0,
      5000,
      "onMutationError handler should receive rejection before restart",
    );
    expect(mutationErrorSpy).toHaveBeenCalledWith({
      code: "permission_denied",
      reason: "Write rejected by server authorization",
      transaction: {
        transactionId: txId,
        kind: "mergeable",
        sealed: true,
        latestSettlement: {
          kind: "rejected",
          transactionId: txId,
          code: "permission_denied",
          reason: "Write rejected by server authorization",
        },
      },
    });

    const inspectorControl = await dbBeforeRestart.openInspectorControlPort();
    inspectorControl.start();
    const [initialContext] = await listWorkerContexts(inspectorControl);
    expect(initialContext).toBeDefined();
    // Persistent browser roots are scoped by the auth session. Inspector
    // contexts deliberately report that physical name, not the caller's raw
    // driver.dbName; retain it only as an opaque same-root handle across the
    // worker restart below.
    const workerDbName = initialContext!.dbName;
    try {
      await shutdownDbAndWorker(dbBeforeRestart, inspectorControl);

      const dbAfterAcknowledgement = track(await createPersistentDb(undefined));
      const replayAfterAckSpy = vi.fn();
      dbAfterAcknowledgement.onMutationError(replayAfterAckSpy);
      expect(await dbAfterAcknowledgement.all(allTodos, { tier: "local" })).toEqual([
        durableControl.value,
      ]);
      const secondInspectorControl = await dbAfterAcknowledgement.openInspectorControlPort();
      secondInspectorControl.start();
      const [secondContext] = (await listWorkerContexts(secondInspectorControl)).filter(
        (context) => context.dbName === workerDbName,
      );
      expect(secondContext?.workerRealmId).not.toBe(initialContext?.workerRealmId);
      // The destroyed worker context rehydrated the settled local view, but the
      // original tab's application notification is not a backlog for a later tab.
      await sleep(500);
      expect(replayAfterAckSpy).not.toHaveBeenCalled();

      await shutdownDbAndWorker(dbAfterAcknowledgement, secondInspectorControl);

      const dbAfterSecondRestart = track(await createPersistentDb(undefined));
      expect(await dbAfterSecondRestart.all(allTodos, { tier: "local" })).toEqual([
        durableControl.value,
      ]);
      const thirdInspectorControl = await dbAfterSecondRestart.openInspectorControlPort();
      thirdInspectorControl.start();
      const [thirdContext] = (await listWorkerContexts(thirdInspectorControl)).filter(
        (context) => context.dbName === workerDbName,
      );
      expect(thirdContext?.workerRealmId).not.toBe(secondContext?.workerRealmId);
      thirdInspectorControl.postMessage({
        type: "close",
      } satisfies BrowserInspectorControlRequest);
    } finally {
      inspectorControl.postMessage({ type: "close" } satisfies BrowserInspectorControlRequest);
    }
  });

  it("delivers a rejection to a runtime attached while the worker rehydrates", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions(
      "sync-on-mutation-error-undelivered-restart",
      readOnlyPermissions,
    );

    const dbName = uniqueDbName("sync-on-mutation-error-undelivered-restart");
    const account = await acquireBrowserTestAccount({
      appId: syncServer.appId,
      serverUrl: syncServer.serverUrl,
      key: dbName,
    });
    const createPersistentDb = (serverUrl?: string) =>
      createDb({
        appId: syncServer.appId,
        driver: { type: "persistent" as const, dbName },
        serverUrl,
        account,
      });

    const dbBeforeRestart = track(await createPersistentDb(undefined));
    const insertResult = dbBeforeRestart.insert(todos, {
      title: "Rejected replayed after restart",
      done: false,
    });
    await withTimeout(
      insertResult.wait({ tier: "local" }),
      5000,
      "pending rejected insert should be durably recorded locally before restart",
    );

    const inspectorBeforeRestart = await dbBeforeRestart.openInspectorControlPort();
    inspectorBeforeRestart.start();
    const [contextBeforeRestart] = await listWorkerContexts(inspectorBeforeRestart);
    expect(contextBeforeRestart).toBeDefined();
    const workerDbName = contextBeforeRestart!.dbName;
    await shutdownDbAndWorker(dbBeforeRestart, inspectorBeforeRestart);

    const dbAfterRestart = track(await createPersistentDb(syncServer.serverUrl));
    const replayAfterRestartSpy = vi.fn();
    dbAfterRestart.onMutationError(replayAfterRestartSpy);

    // Run a query to set up the runtime
    await dbAfterRestart.all(allTodos, { tier: "global" });
    const inspectorAfterRestart = await dbAfterRestart.openInspectorControlPort();
    inspectorAfterRestart.start();
    const [contextAfterRestart] = (await listWorkerContexts(inspectorAfterRestart)).filter(
      (context) => context.dbName === workerDbName,
    );
    expect(contextAfterRestart?.workerRealmId).not.toBe(contextBeforeRestart?.workerRealmId);

    await waitForCondition(
      async () => (await dbAfterRestart.all(allTodos, { tier: "local" })).length === 0,
      5000,
      "rejected transaction should not rehydrate into the restarted local view",
    );
    // This runtime is already attached when the restored worker receives the
    // settlement, so it is a live notification rather than unsupported
    // cross-lifecycle toast continuity. A later runtime still only observes
    // the reconciled row state below.
    await waitForCondition(
      () => replayAfterRestartSpy.mock.calls.length === 1,
      5000,
      "attached runtime should receive the restored worker's live rejection",
    );

    await shutdownDbAndWorker(dbAfterRestart, inspectorAfterRestart);

    const dbAfterSecondRestart = track(await createPersistentDb(undefined));
    expect(await dbAfterSecondRestart.all(allTodos, { tier: "local" })).toEqual([]);
    const inspectorAfterSecondRestart = await dbAfterSecondRestart.openInspectorControlPort();
    inspectorAfterSecondRestart.start();
    const [contextAfterSecondRestart] = (
      await listWorkerContexts(inspectorAfterSecondRestart)
    ).filter((context) => context.dbName === workerDbName);
    expect(contextAfterSecondRestart?.workerRealmId).not.toBe(contextAfterRestart?.workerRealmId);
    inspectorAfterSecondRestart.postMessage({
      type: "close",
    } satisfies BrowserInspectorControlRequest);
  });

  /**
   * Physical browser receipt for the complete identity/recovery path. The
   * first `createDb` acquires a foreground lease; after its SharedWorker ends,
   * a successor lease attaches to the reopened durable replica. The recovered
   * relay must route each former foreground terminal exactly once: rejection
   * is one live callback, acceptance is one normal Global row.
   */
  it("settles recovered accepted and rejected foreground writes exactly once", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions(
      "sync-recovery-terminal-pair",
      recoveryTerminalPermissions,
    );
    const dbName = uniqueDbName("sync-recovery-terminal-pair");
    const account = await acquireBrowserTestAccount({
      appId: syncServer.appId,
      serverUrl: syncServer.serverUrl,
      key: dbName,
    });
    const createPersistentDb = (serverUrl?: string) =>
      createDb({
        appId: syncServer.appId,
        driver: { type: "persistent" as const, dbName },
        serverUrl,
        account,
      });

    const first = track(await createPersistentDb(undefined));
    const accepted = first.insert(todos, {
      title: "accepted after worker restart",
      done: false,
    });
    const rejected = first.insert(todos, {
      title: "rejected after worker restart",
      done: true,
    });
    const rejectedTxId = await rejected.txId;
    await withTimeout(
      Promise.all([accepted.wait({ tier: "local" }), rejected.wait({ tier: "local" })]),
      5000,
      "foreground writes should be durable in the worker before restart",
    );

    await shutdownDbAndWorker(first);

    const successor = track(await createPersistentDb(syncServer.serverUrl));
    const mutationErrors = vi.fn();
    successor.onMutationError(mutationErrors);
    // `createDb` is intentionally lazy. Attach the foreground runtime before
    // opening the inspector so this receipt observes the same public startup
    // path as an application's first local query.
    await successor.all(allTodos, { tier: "local" });

    await waitForCondition(
      async () => {
        const rows = await successor.all(allTodos, { tier: "local" });
        return rows.length === 1 && rows[0]?.id === accepted.value.id;
      },
      10_000,
      "recovered accepted write should settle once into the successor local view",
    );
    await waitForCondition(
      () => mutationErrors.mock.calls.length === 1,
      10_000,
      "recovered rejection should produce exactly one live successor callback",
    );
    expect(mutationErrors).toHaveBeenCalledWith(
      expect.objectContaining({
        code: "permission_denied",
        transaction: expect.objectContaining({ transactionId: rejectedTxId }),
      }),
    );

    await successor.all(allTodos, { tier: "global" });
    await sleep(250);
    expect(mutationErrors).toHaveBeenCalledTimes(1);
    await expect(successor.all(allTodos, { tier: "local" })).resolves.toEqual([
      expect.objectContaining({ id: accepted.value.id, title: "accepted after worker restart" }),
    ]);

    await shutdownDbAndWorker(successor);

    const later = track(await createPersistentDb(undefined));
    const laterErrors = vi.fn();
    later.onMutationError(laterErrors);
    await expect(later.all(allTodos, { tier: "local" })).resolves.toEqual([
      expect.objectContaining({ id: accepted.value.id }),
    ]);
    await sleep(250);
    expect(laterErrors).not.toHaveBeenCalled();
    await later.shutdown();
    untrack(later);
  }, 60_000);
});
