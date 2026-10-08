/// <reference types="vite/client" />

/**
 * Exact multi-tab convergence, included rows, credentials and schema recovery.
 */

import { describe, it, expect, vi } from "vitest";
import {
  createBrowserTestDb as createDb,
  createSyncedDb,
  uniqueDbName,
  waitForCondition,
  withTimeout,
} from "./support.js";
import { Db, resolveDefaultPersistentDbName, type QueryBuilder } from "../../src/runtime/db.js";

import { generateAuthSecret } from "../../src/runtime/auth-secret-store.js";
import { getJazzServerJwtForUser } from "./testing-server.js";
import {
  createRemoteBrowserDb,
  insertRemoteBrowserDbRow,
  queryRemoteBrowserDbRows,
  updateRemoteBrowserDbRow,
  restartRemoteBrowserDb,
} from "./remote-browser-db.js";
import {
  app,
  todos,
  Todo,
  nullableApp,
  nullablePermissions,
  allTodos,
  catalogueAppV1,
  catalogueAppV2,
  makeStructurallyValidJwt,
  waitForTodos,
  publishCatalogueSchemaFamily,
  publishSyncServerSchemaAndPermissions,
  useSharedWorkerBridgeHarness,
} from "./worker-bridge-harness.js";

describe("SharedWorker bridge with IndexedDB", () => {
  const { ctx, trackRemoteBrowserDb, track, trackSubscription, untrack } =
    useSharedWorkerBridgeHarness();
  // -------------------------------------------------------------------------
  // 8. Cross-tab SharedWorker routing
  // -------------------------------------------------------------------------

  it("routes writes between tabs through the shared runtime", async () => {
    const dbName = uniqueDbName("shared-runtime-route");
    const dbA = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );
    const dbB = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );
    await Promise.all([dbA.all(allTodos, { tier: "local" }), dbB.all(allTodos, { tier: "local" })]);

    const receivedByLeader: string[] = [];
    const unsubscribe = dbA.subscribe(allTodos as QueryBuilder<Todo & { id: string }>, (rows) => {
      for (const todo of rows) {
        receivedByLeader.push(todo.title);
      }
    });

    dbB.insert(todos, { title: "Routed through SharedWorker", done: false });

    await waitForCondition(
      async () => receivedByLeader.includes("Routed through SharedWorker"),
      8000,
      "First tab should receive the second tab's write",
    );

    await waitForCondition(
      async () => {
        const firstRows = await dbA.all(allTodos, { tier: "local" });
        const secondRows = await dbB.all(allTodos, { tier: "local" });
        return [firstRows, secondRows].every((rows) =>
          rows.some((row) => row.title === "Routed through SharedWorker"),
        );
      },
      8000,
      "Both tabs should observe the routed write",
    );

    unsubscribe();
  });

  it("converges concurrent writes across three tabs with exact cardinality", async () => {
    const remoteDbId = trackRemoteBrowserDb(uniqueDbName("three-tab-cardinality"));
    const dbName = uniqueDbName("three-tab-cardinality-store");
    await createRemoteBrowserDb({
      id: remoteDbId,
      appId: "test-app",
      dbName,
      table: "todos",
      schemaJson: JSON.stringify(app.wasmSchema),
      tabCount: 3,
      initialize: true,
    });

    const rows = Array.from({ length: 18 }, (_, index) => ({
      title: `tab-${index % 3}-row-${index}`,
      done: index % 2 === 0,
    }));
    await Promise.all(
      rows.map((row, index) =>
        Promise.race([
          insertRemoteBrowserDbRow(remoteDbId, index % 3, row),
          new Promise<never>((_, reject) =>
            setTimeout(
              () => reject(new Error(`local settlement timed out for write ${index}`)),
              8000,
            ),
          ),
        ]),
      ),
    );

    await waitForCondition(
      async () => {
        const snapshots = await Promise.all(
          [0, 1, 2].map((tabIndex) => queryRemoteBrowserDbRows(remoteDbId, tabIndex)),
        );
        return snapshots.every(
          (snapshot) =>
            snapshot.length === rows.length &&
            new Set(snapshot.map((row) => row.title)).size === rows.length,
        );
      },
      10_000,
      "All tabs should observe every concurrent write exactly once",
    );

    for (let tabIndex = 0; tabIndex < 3; tabIndex += 1) {
      const snapshot = await queryRemoteBrowserDbRows(remoteDbId, tabIndex);
      expect(snapshot).toHaveLength(rows.length);
      expect(snapshot.map((row) => row.title).sort()).toEqual(rows.map((row) => row.title).sort());
    }
  });

  it("converges conflicting updates across tabs to one exact row", async () => {
    const remoteDbId = trackRemoteBrowserDb(uniqueDbName("three-tab-conflict"));
    await createRemoteBrowserDb({
      id: remoteDbId,
      appId: "test-app",
      dbName: uniqueDbName("three-tab-conflict-store"),
      table: "todos",
      schemaJson: JSON.stringify(app.wasmSchema),
      tabCount: 3,
      initialize: true,
    });
    const rowId = await insertRemoteBrowserDbRow(remoteDbId, 0, {
      title: "before-conflict",
      done: false,
    });
    await waitForCondition(
      async () => {
        const tabSnapshots = await Promise.all(
          [0, 1, 2].map((tabIndex) => queryRemoteBrowserDbRows(remoteDbId, tabIndex)),
        );
        return tabSnapshots.every((rows) => rows.some((row) => row.id === rowId));
      },
      8_000,
      "Seed row should be locally observed by every tab before conflicting updates",
    );

    await Promise.all([
      updateRemoteBrowserDbRow(remoteDbId, 0, rowId, {
        title: "conflict-from-a",
        done: true,
        projectId: null,
        tags: null,
      }),
      updateRemoteBrowserDbRow(remoteDbId, 1, rowId, {
        title: "conflict-from-b",
        done: true,
        projectId: null,
        tags: null,
      }),
    ]);
    await waitForCondition(
      async () => {
        const snapshots = await Promise.all(
          [0, 1, 2].map((tabIndex) => queryRemoteBrowserDbRows(remoteDbId, tabIndex)),
        );
        const titles = snapshots.map((rows) => rows[0]?.title);
        return (
          snapshots.every((rows) => rows.length === 1 && rows[0]?.id === rowId) &&
          new Set(titles).size === 1
        );
      },
      10_000,
      "Every tab should converge to the same conflict winner without duplicating the row",
    );

    const snapshots = await Promise.all(
      [0, 1, 2].map((tabIndex) => queryRemoteBrowserDbRows(remoteDbId, tabIndex)),
    );
    expect(snapshots.every((rows) => rows.length === 1 && rows[0]?.id === rowId)).toBe(true);
    expect(new Set(snapshots.map((rows) => rows[0]?.title))).toHaveLength(1);
    expect(["conflict-from-a", "conflict-from-b"]).toContain(snapshots[0]![0]!.title);
  });

  it("hydrates and updates an included row consistently across tabs", async () => {
    const remoteDbId = trackRemoteBrowserDb(uniqueDbName("three-tab-include"));
    await createRemoteBrowserDb({
      id: remoteDbId,
      appId: "test-app",
      dbName: uniqueDbName("three-tab-include-store"),
      table: "todos",
      queryJson: app.todos.include({ project: true })._build(),
      schemaJson: JSON.stringify(app.wasmSchema),
      tabCount: 3,
      initialize: true,
    });
    const projectId = await insertRemoteBrowserDbRow(
      remoteDbId,
      0,
      { name: "Shared project" },
      "projects",
    );
    const todoId = await insertRemoteBrowserDbRow(remoteDbId, 1, {
      title: "Cross-tab include",
      done: false,
      projectId,
    });

    await waitForCondition(
      async () => {
        const snapshots = await Promise.all(
          [0, 1, 2].map((tabIndex) => queryRemoteBrowserDbRows(remoteDbId, tabIndex)),
        );
        return snapshots.every(
          (rows) =>
            rows.length === 1 &&
            rows[0]?.id === todoId &&
            (rows[0]?.project as Record<string, unknown> | undefined)?.name === "Shared project",
        );
      },
      10_000,
      "Every tab should hydrate the same included project exactly once",
    );

    await updateRemoteBrowserDbRow(
      remoteDbId,
      2,
      projectId,
      { name: "Updated project" },
      "projects",
    );
    await waitForCondition(
      async () => {
        const snapshots = await Promise.all(
          [0, 1, 2].map((tabIndex) => queryRemoteBrowserDbRows(remoteDbId, tabIndex)),
        );
        return snapshots.every(
          (rows) =>
            rows.length === 1 &&
            (rows[0]?.project as Record<string, unknown> | undefined)?.name === "Updated project",
        );
      },
      10_000,
      "Included project updates should reach every tab without cardinality drift",
    );
  });

  it("rehydrates exact multi-tab state after the SharedWorker restarts", async () => {
    const remoteDbId = trackRemoteBrowserDb(uniqueDbName("worker-restart-cardinality"));
    await createRemoteBrowserDb({
      id: remoteDbId,
      appId: "test-app",
      dbName: uniqueDbName("worker-restart-cardinality-store"),
      table: "todos",
      schemaJson: JSON.stringify(app.wasmSchema),
      tabCount: 2,
      initialize: true,
    });
    const expected = Array.from({ length: 12 }, (_, index) => ({
      title: `before-worker-restart-${index}`,
      done: index % 2 === 0,
    }));
    await Promise.all(
      expected.map((row, index) =>
        Promise.race([
          insertRemoteBrowserDbRow(remoteDbId, index % 2, row),
          new Promise<never>((_, reject) =>
            setTimeout(
              () => reject(new Error(`local settlement timed out for write ${index}`)),
              8000,
            ),
          ),
        ]),
      ),
    );
    await waitForCondition(
      async () => (await queryRemoteBrowserDbRows(remoteDbId, 0)).length === expected.length,
      8000,
      "Seed writes should converge before terminating the worker",
    );

    await restartRemoteBrowserDb(remoteDbId);

    for (let tabIndex = 0; tabIndex < 2; tabIndex += 1) {
      const snapshot = await queryRemoteBrowserDbRows(remoteDbId, tabIndex);
      expect(snapshot).toHaveLength(expected.length);
      expect(new Set(snapshot.map((row) => row.title))).toEqual(
        new Set(expected.map((row) => row.title)),
      );
    }
  });

  it("syncs a tab opened after the shared runtime is already ready", async () => {
    const dbName = uniqueDbName("late-tab-route");
    const first = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );

    first.insert(todos, { title: "Created before second tab", done: false });
    await waitForCondition(
      async () => {
        const rows = await first.all(allTodos, { tier: "local" });
        return rows.some((row) => row.title === "Created before second tab");
      },
      8000,
      "First tab should persist the initial row before opening the second",
    );

    const second = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );
    const secondRows = await withTimeout(
      second.all(allTodos, { tier: "local" }),
      8000,
      "Late tab initial query should hydrate through the shared runtime",
    );
    expect(secondRows.some((row) => row.title === "Created before second tab")).toBe(true);
  });

  it("hydrates a late tab subscription through the shared runtime", async () => {
    const dbName = uniqueDbName("late-tab-subscribe");
    const first = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );

    const title = "Persisted before second-tab subscription";
    first.insert(todos, { title, done: false });
    await waitForCondition(
      async () => {
        const rows = await first.all(allTodos, { tier: "local" });
        return rows.some((row) => row.title === title);
      },
      8000,
      "First tab should persist the seed row before opening the second",
    );

    const second = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );
    const snapshots: Todo[][] = [];
    const unsubscribe = trackSubscription(
      second.subscribe(allTodos, (rows) => {
        snapshots.push(rows);
      }),
    );

    await waitForCondition(
      async () => snapshots.some((rows) => rows.some((row) => row.title === title)),
      8000,
      "Late tab subscription should hydrate the persisted row",
    );

    unsubscribe();
  });

  it("surfaces schema mismatch errors and recovers after the pinning tab closes", async () => {
    const dbName = uniqueDbName("schema-mismatch-recovery");
    // Both versions must already be admitted with their lineage lens. Merely
    // supplying a new schema at reopen is not a catalogue publication.
    const server = await publishCatalogueSchemaFamily("schema-mismatch-recovery");
    const nextApp = catalogueAppV2;
    const oldTab = track(
      await createDb({
        appId: server.appId,
        serverUrl: server.serverUrl,
        driver: { type: "persistent", dbName },
      }),
    );
    await withTimeout(
      oldTab
        .insert(catalogueAppV1.todos, { title: "Old schema row", completed: false })
        .wait({ tier: "global" }),
      8000,
      "Old tab should receive the published catalogue before pinning its schema",
    );
    await waitForCondition(
      async () => {
        const rows = await oldTab.all(catalogueAppV1.todos, { tier: "local" });
        return rows.some((row) => row.title === "Old schema row");
      },
      8000,
      "Old tab should be durable-ready with the original schema",
    );

    const newTab = track(
      await createDb({
        appId: server.appId,
        serverUrl: server.serverUrl,
        driver: { type: "persistent", dbName },
      }),
    );
    await expect(
      withTimeout(
        newTab.all(nextApp.todos, { tier: "local" }),
        8000,
        "Schema-blocked tab query should reject instead of hanging",
      ),
    ).rejects.toThrow("incompatible persistent browser configuration");

    // Each later call gets one fresh admission attempt, and must still fail
    // visibly while the incompatible worker remains pinned.
    for (let attempt = 0; attempt < 2; attempt += 1) {
      await expect(
        withTimeout(
          newTab.all(nextApp.todos, { tier: "local" }),
          8000,
          "Repeated schema-blocked query should reject instead of hanging",
        ),
      ).rejects.toThrow("incompatible persistent browser configuration");
    }

    const failedTab = track(
      await createDb({
        appId: server.appId,
        serverUrl: server.serverUrl,
        driver: { type: "persistent", dbName },
      }),
    );
    await expect(failedTab.all(nextApp.todos, { tier: "local" })).rejects.toThrow(
      "incompatible persistent browser configuration",
    );
    await failedTab.shutdown();

    await oldTab.shutdown();
    const rows = await withTimeout(
      newTab.all(nextApp.todos, { tier: "local" }),
      8000,
      "Recovered tab should be able to query with its own schema",
    );
    expect(Array.isArray(rows)).toBe(true);
    expect(rows.some((row) => row.title === "Old schema row")).toBe(true);
  });

  it("keeps explicit-name account caches separate, shared per scope, and destroys only the selected scope", async () => {
    const appId = uniqueDbName("explicit-browser-owner-app");
    const dbName = uniqueDbName("shared-device-cache");
    const aliceSecret = generateAuthSecret();
    const bobSecret = generateAuthSecret();
    const aliceConfig = {
      appId,
      secret: aliceSecret,
      driver: { type: "persistent" as const, dbName },
    };
    const bobConfig = { appId, secret: bobSecret, driver: { type: "persistent" as const, dbName } };

    let alice: Db | null = track(await createDb(aliceConfig));
    const alicePhysicalName = resolveDefaultPersistentDbName(alice.config);
    expect(alicePhysicalName).toMatch(new RegExp(`^${dbName}::jazz-browser-v1::`));
    expect(alicePhysicalName).not.toContain(aliceSecret);

    let aliceSecondTab: Db | null = null;
    let bob: Db | null = null;
    let aliceReopened: Db | null = null;
    let bobReopened: Db | null = null;
    try {
      alice.insert(todos, { title: "Alice durable row", done: false });
      await waitForCondition(
        async () => (await alice.all(allTodos, { tier: "local" })).length === 1,
        8_000,
        "Alice should persist into her scoped root",
      );

      // A second tab for the same canonical scope joins Alice's same worker
      // and physical root, rather than creating a second cache.
      aliceSecondTab = track(await createDb(aliceConfig));
      expect(
        (await aliceSecondTab.all(allTodos, { tier: "local" })).map((row) => row.title),
      ).toEqual(["Alice durable row"]);
      bob = track(await createDb(bobConfig));
      const bobPhysicalName = resolveDefaultPersistentDbName(bob.config);
      expect(alicePhysicalName).not.toBe(bobPhysicalName);
      expect(bobPhysicalName).not.toContain(bobSecret);
      await expect(bob.all(allTodos, { tier: "local" })).resolves.toEqual([]);
      bob.insert(todos, { title: "Bob durable row", done: false });
      await waitForTodos(
        bob,
        (rows) => rows.some((row) => row.title === "Bob durable row"),
        "Bob should use his own scoped root",
      );

      // Destruction is deliberately per physical scope. Bob's explicit reset
      // cannot transfer or erase Alice's coexisting cache.
      await bob.deleteClientStorage();
      await bob.shutdown();
      untrack(bob);
      bob = null;

      await aliceSecondTab.shutdown();
      untrack(aliceSecondTab);
      aliceSecondTab = null;
      await alice.shutdown();
      untrack(alice);
      alice = null;

      aliceReopened = track(await createDb(aliceConfig));
      expect(
        (await aliceReopened.all(allTodos, { tier: "local" })).map((row) => row.title),
      ).toEqual(["Alice durable row"]);
      bobReopened = track(await createDb(bobConfig));
      await expect(bobReopened.all(allTodos, { tier: "local" })).resolves.toEqual([]);
    } finally {
      for (const db of [bobReopened, aliceReopened, bob, aliceSecondTab, alice]) {
        await db?.shutdown().catch(() => undefined);
        if (db) untrack(db);
      }
    }
  });

  it("fans out auth loss and accepts same-principal refresh from either tab", async () => {
    const { appId, serverUrl } = await publishSyncServerSchemaAndPermissions("auth-fanout");
    const dbName = uniqueDbName("auth-fanout");
    const userId = "00000000-0000-0000-0000-00000000fa01";
    const validJwt = await getJazzServerJwtForUser(userId, undefined, appId);
    const invalidJwt = makeStructurallyValidJwt(userId);

    const dbA = track(
      await createDb({
        appId,
        serverUrl,
        jwtToken: validJwt,
        registerJwt: true,
        driver: { type: "persistent", dbName },
      }),
    );
    const dbB = track(
      await createDb({
        appId,
        serverUrl,
        jwtToken: validJwt,
        registerJwt: true,
        driver: { type: "persistent", dbName },
      }),
    );
    dbA.insert(todos, { title: "first-tab-init", done: false });
    await withTimeout(
      dbA.all(allTodos, { tier: "local" }),
      15000,
      "First tab bridge init did not complete",
    );
    dbB.insert(todos, { title: "second-tab-init", done: false });
    await withTimeout(
      dbB.all(allTodos, { tier: "local" }),
      15000,
      "Second tab bridge init did not complete",
    );

    expect(dbA.getAuthState().error).toBeUndefined();
    expect(dbB.getAuthState().error).toBeUndefined();

    dbA.updateAuthToken(invalidJwt);

    await waitForCondition(
      async () => dbA.getAuthState().error === "invalid",
      20000,
      "First tab should turn unauthenticated when the server rejects its JWT",
    );
    await waitForCondition(
      async () => dbB.getAuthState().error === "invalid",
      20000,
      "Second tab should turn unauthenticated through the worker auth fan-out",
    );

    dbB.updateAuthToken(validJwt);

    await waitForCondition(
      async () => dbB.getAuthState().error === undefined,
      20000,
      "Second tab should recover after submitting a same-principal token refresh",
    );
    await waitForCondition(
      async () => dbA.getAuthState().error === undefined,
      20000,
      "First tab should receive the refreshed auth state from the shared worker",
    );
  }, 60000);

  it("returns an existing worker row to a fresh local follower after a rejected principal change", async () => {
    const { appId, serverUrl } = await publishSyncServerSchemaAndPermissions(
      "cold-local-follower-principal-guard",
    );
    const dbName = uniqueDbName("cold-local-follower-principal-guard");
    const aliceJwt = await getJazzServerJwtForUser(
      "00000000-0000-0000-0000-00000000ca11",
      undefined,
      appId,
    );
    const bobJwt = await getJazzServerJwtForUser(
      "00000000-0000-0000-0000-00000000cb22",
      undefined,
      appId,
    );
    const config = {
      appId,
      serverUrl,
      jwtToken: aliceJwt,
      registerJwt: true,
      driver: { type: "persistent" as const, dbName },
    };
    const owner = track(await createDb(config));
    let follower: Db | null = null;
    try {
      const knownOwnerRow = await owner
        .insert(todos, { title: "owner row before follower opens", done: false })
        .wait({ tier: "global" });

      // The follower has not inserted or queried this table. Its first local
      // attachment must wait for the persistent owner's existing snapshot,
      // rather than returning the follower's initially empty replica.
      const freshFollower = track(await createDb(config));
      follower = freshFollower;
      await expect(
        withTimeout(
          freshFollower.all(allTodos, { tier: "local" }),
          3_000,
          "Fresh follower local read did not receive the persistent owner row",
        ),
      ).resolves.toEqual([knownOwnerRow]);

      const aliceState = freshFollower.getAuthState();
      expect(aliceState.session?.user).toBeDefined();
      const followerRuntime = (
        freshFollower as unknown as {
          getClient(schema: typeof todos._schema): {
            getRuntime(): { notifyPeerTransportActivity(): void };
          };
        }
      )
        .getClient(todos._schema)
        .getRuntime();
      // Model the relevant terminal condition: Bob's rejected update cannot
      // yield a future worker acknowledgement. The local attachment still
      // receives the persistent owner's already covered row.
      const suppressFuturePeerActivity = vi
        .spyOn(followerRuntime, "notifyPeerTransportActivity")
        .mockImplementation(() => undefined);
      try {
        expect(() => freshFollower.updateAuthToken(bobJwt)).toThrow(
          "Changing auth principal on a live client is not supported. Recreate the Db.",
        );
        expect(freshFollower.getAuthState()).toEqual(aliceState);

        await expect(
          withTimeout(
            freshFollower.all(allTodos, { tier: "local" }),
            3_000,
            "Local read waited for a peer frame after principal rejection",
          ),
        ).resolves.toEqual([knownOwnerRow]);
      } finally {
        suppressFuturePeerActivity.mockRestore();
      }
    } finally {
      await follower?.shutdown().catch(() => undefined);
      if (follower) untrack(follower);
      await owner.shutdown();
      untrack(owner);
    }
  }, 60_000);

  it("rejects a principal-changing live auth update before local or worker state changes", async () => {
    const { appId, serverUrl } =
      await publishSyncServerSchemaAndPermissions("live-auth-owner-guard");
    const dbName = uniqueDbName("live-auth-owner-guard");
    const aliceJwt = await getJazzServerJwtForUser(
      "00000000-0000-0000-0000-00000000aa11",
      undefined,
      appId,
    );
    const bobJwt = await getJazzServerJwtForUser(
      "00000000-0000-0000-0000-00000000bb22",
      undefined,
      appId,
    );
    const db = track(
      await createDb({
        appId,
        serverUrl,
        jwtToken: aliceJwt,
        registerJwt: true,
        driver: { type: "persistent", dbName },
      }),
    );
    try {
      const knownAliceRow = await db
        .insert(todos, { title: "known Alice local row", done: false })
        .wait({ tier: "global" });
      // Establish default local follower coverage while the worker still owns
      // Alice's principal. The rejected Bob update below must not require a
      // new worker frame before returning this already covered local row.
      await expect(db.all(allTodos, { tier: "local" })).resolves.toEqual([knownAliceRow]);
      const aliceState = db.getAuthState();
      expect(aliceState.session?.user).toBeDefined();

      // Planted positive: applying Bob before BrowserConnectionManager checks
      // ownership would mutate this state and forward Bob's claims into the
      // durable Alice worker before the guard could reject it.
      expect(() => db.updateAuthToken(bobJwt)).toThrow(
        "Changing auth principal on a live client is not supported. Recreate the Db.",
      );
      expect(db.getAuthState()).toEqual(aliceState);
      await expect(db.all(allTodos, { tier: "local" })).resolves.toEqual([knownAliceRow]);
    } finally {
      await db.shutdown();
      untrack(db);
    }
  }, 60000);

  it("can update an optional row field to null", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions(
      "null-update-repro",
      nullablePermissions,
      nullableApp.wasmSchema,
    );
    const sharedLocalAuthToken = generateAuthSecret();
    const db = await createSyncedDb(
      ctx,
      "sync-null-update-repro",
      sharedLocalAuthToken,
      syncServer,
    );

    const inserted = db.insert(nullableApp.todos, {
      title: "nullable-description-repro",
      done: false,
      description: "server-original",
    });
    const insertedTodo = inserted.value;
    await inserted.wait({ tier: "local" });

    const updateResult = db.update(nullableApp.todos, insertedTodo.id, {
      description: null,
    });
    await updateResult.wait({ tier: "local" });

    const rowAfterNullUpdate = await db.one(nullableApp.todos.where({ id: insertedTodo.id }), {
      tier: "local",
    });
    expect(rowAfterNullUpdate).not.toBeNull();
    expect(rowAfterNullUpdate?.description ?? null).toBeNull();
  }, 60000);
});
