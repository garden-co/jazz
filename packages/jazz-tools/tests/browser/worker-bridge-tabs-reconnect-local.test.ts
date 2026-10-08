/// <reference types="vite/client" />

/**
 * Worker sync reconnection and local-tier delivery across isolated browser contexts.
 */

import { describe, it, expect } from "vitest";
import {
  createBrowserTestDb as createDb,
  createSyncedDb,
  sleep,
  uniqueDbName,
  waitForCondition,
  withTimeout,
} from "./support.js";

import { createInspectorLocalQueryOptions as inspectorLocalQueryOptions } from "../../src/internal/inspector-query.js";
import { generateAuthSecret } from "../../src/runtime/auth-secret-store.js";
import { blockJazzServerNetwork, unblockJazzServerNetwork } from "./testing-server.js";
import { createRemoteBrowserDb } from "./remote-browser-db.js";
import {
  app,
  todos,
  Todo,
  allTodos,
  waitForTodos,
  publishSyncServerSchemaAndPermissions,
  stopOwnedJazzServer,
  useSharedWorkerBridgeHarness,
} from "./worker-bridge-harness.js";

describe("SharedWorker bridge with IndexedDB", () => {
  const { ctx, trackRemoteBrowserDb, waitForRemoteTodoTitle, track, trackSubscription, untrack } =
    useSharedWorkerBridgeHarness();
  it("recovers sync after browser-side network loss with B in a separate context", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions("sync-recover");
    const sharedLocalAuthToken = generateAuthSecret();
    const { appId, serverUrl } = syncServer;
    const dbA = await createSyncedDb(ctx, "sync-recover-a", sharedLocalAuthToken, syncServer);
    const remoteDbId = trackRemoteBrowserDb(uniqueDbName("sync-recover-remote"));
    await createRemoteBrowserDb({
      id: remoteDbId,
      appId,
      dbName: uniqueDbName("sync-recover-b"),
      table: "todos",
      schemaJson: JSON.stringify(app.wasmSchema),
      serverUrl,
      localFirstSecret: sharedLocalAuthToken,
    });

    const baselineTitle = `baseline-network-recover-${Date.now()}`;
    await withTimeout(
      dbA.insert(todos, { title: baselineTitle, done: false }).wait({ tier: "local" }),
      10000,
      "Baseline insert(worker) did not resolve",
    );

    await waitForRemoteTodoTitle(
      remoteDbId,
      baselineTitle,
      "B sees baseline row before browser-side network block",
      20000,
    );

    await blockJazzServerNetwork(serverUrl);
    await sleep(500);
    await unblockJazzServerNetwork(serverUrl);
    await sleep(250);

    const recoveredTitle = `network-recovered-${Date.now()}`;
    await withTimeout(
      dbA.insert(todos, { title: recoveredTitle, done: false }).wait({ tier: "local" }),
      10000,
      "Recovered insert(worker) did not resolve",
    );

    const rowsOnB = await waitForRemoteTodoTitle(
      remoteDbId,
      recoveredTitle,
      "B sees row written after browser-side network recovery",
      20000,
    );
    expect(rowsOnB.some((row) => row.title === recoveredTitle)).toBe(true);
  }, 60000);

  it("keeps a local subscription live after an unexpected server shutdown", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions("local-after-server-shutdown");
    const db = await createSyncedDb(
      ctx,
      "local-after-server-shutdown",
      generateAuthSecret(),
      syncServer,
    );
    const snapshots: Todo[][] = [];
    trackSubscription(db.subscribe(allTodos, (rows) => snapshots.push(rows), { tier: "local" }));
    await waitForCondition(
      async () => snapshots.length > 0,
      5000,
      "local subscription did not publish its opening snapshot",
    );

    // Exercise loss of an established connection, not a race with its first Hello.
    await db.all(allTodos, { tier: "global" });
    await stopOwnedJazzServer(syncServer.serverUrl);
    const globalReadError = await withTimeout(
      db.all(allTodos, { tier: "global" }),
      // An established link reports the outage after 7.5s of failed
      // reconnects (and keeps retrying). Leave room for worker delivery too.
      15000,
      "global read did not observe the stopped server",
    ).then(
      () => null,
      (error: unknown) => error,
    );
    expect(globalReadError).toBeInstanceOf(Error);
    expect((globalReadError as Error).message).not.toContain(
      "global read did not observe the stopped server",
    );

    const title = `local-after-server-shutdown-${Date.now()}`;
    await withTimeout(
      db.insert(todos, { title, done: false }).wait({ tier: "local" }),
      5000,
      "offline insert did not become locally durable",
    );
    await waitForCondition(
      async () => snapshots.some((rows) => rows.some((row) => row.title === title)),
      5000,
      "local subscription did not publish the offline insert",
    );
  });

  /**
   *   writer ──baseline write──► server
   *   fresh probe starts while server traffic is blocked
   *   probe ──global query pending──X server
   *   network unblocks
   *   expected: the first fresh global query completes without needing a second client recreate
   */
  it("replays a fresh global query once upstream attaches after init", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions("edge-late-attach");
    const sharedLocalAuthToken = generateAuthSecret();
    const { serverUrl } = syncServer;
    const dbWriter = await createSyncedDb(
      ctx,
      "edge-late-attach-writer",
      sharedLocalAuthToken,
      syncServer,
    );

    try {
      const baselineTitle = `edge-late-baseline-${Date.now()}`;
      await withTimeout(
        dbWriter.insert(todos, { title: baselineTitle, done: false }).wait({ tier: "local" }),
        10000,
        "Baseline insert(worker) did not resolve",
      );

      await waitForTodos(
        dbWriter,
        (rows) => rows.some((row) => row.title === baselineTitle),
        "Writer sees baseline row at global tier before blocking",
        20000,
        "global",
      );

      await blockJazzServerNetwork(serverUrl);
      await sleep(250);

      const dbProbe = await createSyncedDb(
        ctx,
        "edge-late-attach-probe",
        sharedLocalAuthToken,
        syncServer,
      );
      const probeRowsPromise = waitForTodos(
        dbProbe,
        (rows) => rows.some((row) => row.title === baselineTitle),
        "Fresh global query resolves after upstream attach",
        20000,
        "global",
      );

      await sleep(500);
      await unblockJazzServerNetwork(serverUrl);

      const rowsOnProbe = await probeRowsPromise;
      expect(rowsOnProbe.some((row) => row.title === baselineTitle)).toBe(true);
    } finally {
      await unblockJazzServerNetwork(serverUrl);
    }
  }, 60000);

  /**
   *   A ──baseline write──► server ◄── B sees baseline
   *   browser blocks Jazz server traffic without reloading the page
   *   A ──offline write(worker)──X server
   *   A ──new online write──► server ◄── B sees control write
   *   expected: the earlier offline worker write also promotes to B + a fresh client
   */
  it("promotes offline worker rows after reconnect while the worker stays alive", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions("sync-offline");
    const sharedLocalAuthToken = generateAuthSecret();
    const { appId, serverUrl } = syncServer;
    const dbA = await createSyncedDb(ctx, "sync-offline-a", sharedLocalAuthToken, syncServer);
    const remoteDbId = trackRemoteBrowserDb(uniqueDbName("sync-offline-remote"));
    await createRemoteBrowserDb({
      id: remoteDbId,
      appId,
      dbName: uniqueDbName("sync-offline-b"),
      table: "todos",
      schemaJson: JSON.stringify(app.wasmSchema),
      serverUrl,
      localFirstSecret: sharedLocalAuthToken,
    });

    const baselineTitle = `baseline-before-offline-${Date.now()}`;
    await withTimeout(
      dbA.insert(todos, { title: baselineTitle, done: false }).wait({ tier: "local" }),
      10000,
      "Baseline insert(worker) did not resolve",
    );

    await waitForRemoteTodoTitle(
      remoteDbId,
      baselineTitle,
      "B sees baseline row before disconnect",
      20000,
    );

    await blockJazzServerNetwork(serverUrl);
    // Disconnect the WS transport so the block takes effect immediately.
    // Playwright route blocking only intercepts new connections; the existing
    // WebSocket must be closed explicitly for the offline simulation to hold.
    await dbA.disconnect();

    const offlineTitle = `offline-worker-row-${Date.now()}`;
    await withTimeout(
      dbA.insert(todos, { title: offlineTitle, done: true }).wait({ tier: "local" }),
      10000,
      "Offline insert(worker) did not resolve",
    );

    await waitForTodos(
      dbA,
      (rows) => rows.some((row) => row.title === offlineTitle),
      "A sees offline worker row locally",
      10000,
      "local",
    );

    await expect(
      waitForRemoteTodoTitle(
        remoteDbId,
        offlineTitle,
        "B should not see offline row while A is disconnected",
        2500,
      ),
    ).rejects.toThrow();

    await unblockJazzServerNetwork(serverUrl);
    // Re-establish the worker's upstream WebSocket now that the network is live again.
    await dbA.reconnect();

    const postReconnectTitle = `post-reconnect-control-${Date.now()}`;
    await withTimeout(
      dbA.insert(todos, { title: postReconnectTitle, done: false }).wait({ tier: "local" }),
      10000,
      "Post-reconnect control insert(worker) did not resolve",
    );

    await waitForTodos(
      dbA,
      (rows) => rows.some((row) => row.title === postReconnectTitle),
      "A sees control row locally after reconnect",
      10000,
      "local",
    );
    await waitForRemoteTodoTitle(
      remoteDbId,
      postReconnectTitle,
      "B sees control row written after reconnect",
      20000,
    );

    const rowsOnB = await waitForRemoteTodoTitle(
      remoteDbId,
      offlineTitle,
      "B sees offline worker row after reconnect",
      20000,
    );
    expect(rowsOnB.some((row) => row.title === offlineTitle)).toBe(true);
    try {
      const dbProbe = await createSyncedDb(
        ctx,
        "sync-offline-probe",
        sharedLocalAuthToken,
        syncServer,
      );
      const rowsOnProbe = await waitForTodos(
        dbProbe,
        (rows) => rows.some((row) => row.title === offlineTitle),
        "Fresh client sees offline worker row at global tier after reconnect",
        20000,
        "global",
      );
      expect(rowsOnProbe.some((row) => row.title === offlineTitle)).toBe(true);
    } finally {
    }
  }, 120000);

  it("local-only subscriptions receive rows from IndexedDB", async () => {
    const dbName = uniqueDbName("sync-local-only");
    const dbA = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );

    const snapshots: Todo[][] = [];
    const unsub = trackSubscription(
      dbA.subscribe(
        allTodos,
        (rows) => {
          snapshots.push(rows);
        },
        inspectorLocalQueryOptions(),
      ),
    );

    await dbA.insert(todos, { title: "local-only-local-1", done: true }).wait({ tier: "local" });

    // Wait for initial local-only snapshot.
    await waitForCondition(
      async () => snapshots.length > 0,
      5000,
      "local-only subscription should receive in-memory insert",
    );

    unsub();

    // Simulate a page refresh: close first instance, then reopen same namespace.
    await dbA.shutdown();
    untrack(dbA);

    const dbB = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );

    await waitForCondition(
      async () => {
        const rows = await dbB.all(allTodos, inspectorLocalQueryOptions());
        return rows.some((row) => row.title === "local-only-local-1");
      },
      8000,
      "local-only query should retrieve persisted IndexedDB rows after reopen",
    );

    const snapshotsB = await dbB.all(allTodos, inspectorLocalQueryOptions());
    expect(snapshotsB.length).toBe(1);
    expect(snapshotsB[0].title).toBe("local-only-local-1");
  }, 60000);

  it("local-only subscriptions do not receive rows from sync server", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions("sync-local-only");
    const sharedLocalAuthToken = generateAuthSecret();
    const dbA = await createSyncedDb(ctx, "sync-local-only-a", sharedLocalAuthToken, syncServer);
    const dbB = await createSyncedDb(ctx, "sync-local-only-b", sharedLocalAuthToken, syncServer);

    const snapshots: Todo[][] = [];
    const unsub = trackSubscription(
      dbB.subscribe(
        allTodos,
        (rows) => {
          snapshots.push(rows);
        },
        inspectorLocalQueryOptions(),
      ),
    );

    // Wait for initial local-only snapshot.
    await waitForCondition(
      async () => snapshots.length > 0,
      5000,
      "local-only subscription should produce an initial snapshot",
    );

    const remoteTitle = `remote-for-local-only-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    await withTimeout(
      dbA.insert(todos, { title: remoteTitle, done: false }).wait({ tier: "local" }),
      10000,
      "A insert(worker) did not resolve",
    );

    // Give sync enough time; local-only must still not see remote data.
    await sleep(3000);
    const latestAfterRemote = snapshots[snapshots.length - 1] ?? [];
    expect(latestAfterRemote.some((row) => row.title === remoteTitle)).toBe(false);

    const localTitle = `local-only-local-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    dbB.insert(todos, { title: localTitle, done: true });

    await waitForCondition(
      async () => {
        const latest = snapshots[snapshots.length - 1] ?? [];
        return latest.some((row) => row.title === localTitle);
      },
      8000,
      "local-only subscription should still include local inserts",
    );

    const latest = snapshots[snapshots.length - 1] ?? [];
    expect(latest.some((row) => row.title === localTitle)).toBe(true);
    expect(latest.some((row) => row.title === remoteTitle)).toBe(false);

    unsub();
  }, 60000);
});
