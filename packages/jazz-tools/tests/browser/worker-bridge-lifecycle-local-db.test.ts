/// <reference types="vite/client" />

/**
 * Public worker shutdown, persistent local data and repeated ownership lifecycles.
 */

import { describe, it, expect, vi } from "vitest";
import {
  createBrowserTestDb as createDb,
  sleep,
  uniqueDbName,
  waitForCondition,
  withTimeout,
} from "./support.js";
import {
  Db,
  getDbSubscriptionSource,
  resolveDefaultPersistentDbName,
} from "../../src/runtime/db.js";
import { generateAuthSecret } from "../../src/runtime/auth-secret-store.js";
import { INDEXEDDB_STORAGE_MANIFEST } from "../../src/runtime/indexeddb-page-store.js";

import { setBrowserFollowerProbeTimingForTest } from "../../src/runtime/native-runtime/browser-follower-connection.js";

import { getJazzServerJwtForUser } from "./testing-server.js";
import {
  createRemoteBrowserDb,
  deleteRemoteBrowserIndexedDbAndWaitForReload,
  insertRemoteBrowserDbRow,
  queryRemoteBrowserDbRows,
  restartRemoteBrowserDb,
} from "./remote-browser-db.js";
import { type BrowserInspectorControlRequest } from "../../src/runtime/native-runtime/browser-worker-protocol.js";
import {
  workerFaultBundleUrl,
  listWorkerLifecycle,
  LIVENESS_TEST_PROBE_TIMING,
  LIVENESS_TEST_SIGNAL_MS,
  app,
  todos,
  Todo,
  allTodos,
  catalogueTodos,
  waitForCatalogueTodos,
  publishCatalogueSchemaFamily,
  publishSyncServerSchemaAndPermissions,
  replaceStorageManifest,
  rawStorageRecords,
  useSharedWorkerBridgeHarness,
} from "./worker-bridge-harness.js";

declare const __JAZZ_BROWSER_SOAK__: string;

describe("SharedWorker bridge with IndexedDB", () => {
  const {
    errorListeners,
    trackRemoteBrowserDb,
    track,
    trackSubscription,
    untrack,
    shutdownDbAndWorker,
  } = useSharedWorkerBridgeHarness();
  it("creates Db with worker in browser environment", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent" },
      }),
    );
    expect(db).toBeDefined();
    expect(db).toBeInstanceOf(Db);
  });

  it("keeps public shutdown alive with pongs then rejects after silent worker death", async () => {
    const capability = uniqueDbName("follower-fault");
    setBrowserFollowerProbeTimingForTest(LIVENESS_TEST_PROBE_TIMING);
    const workerUrl = new URL(await workerFaultBundleUrl(), globalThis.location.href);
    workerUrl.searchParams.set("followerFault", capability);
    const control = new BroadcastChannel(capability);
    const receive = (type: string) =>
      new Promise<void>((resolve) => {
        const listener = (event: MessageEvent<{ type: string }>) => {
          if (event.data.type !== type) return;
          control.removeEventListener("message", listener);
          resolve();
        };
        control.addEventListener("message", listener);
      });
    let db: Db | undefined;
    try {
      db = track(
        await createDb({
          appId: "follower-silent-death",
          driver: { type: "persistent", dbName: uniqueDbName("follower-silent-death") },
          schema: app,
          runtimeSources: {
            brokerWorkerUrl: workerUrl.href,
            wasmVersion: "follower-liveness-test",
          },
        }),
      );
      await db.all(allTodos, { tier: "local" });
      const armed = receive("holding-close");
      control.postMessage({ type: "hold-close" });
      await withTimeout(armed, 5_000, "test worker did not arm the pending control hold");
      const held = receive("close-held");
      let outcome: "pending" | "resolved" | "rejected" = "pending";
      const shutdown = db.shutdown().then(
        () => {
          outcome = "resolved";
          return undefined;
        },
        (error: unknown) => {
          outcome = "rejected";
          return error;
        },
      );
      // Ordinary shutdown returns its foreground lease before sending close.
      // Killing here leaves no unrelated lease-return RPC in test cleanup.
      await withTimeout(held, 5_000, "public shutdown did not reach the real worker close");
      // Three real broker replies span more than the entire silent-death
      // bound. Neither the public operation nor its failure is fabricated.
      for (let index = 0; index < 3; index++) {
        await withTimeout(
          receive("pong-sent"),
          LIVENESS_TEST_SIGNAL_MS,
          "real worker did not answer its liveness probe",
        );
        expect(outcome).toBe("pending");
      }
      control.postMessage({ type: "die" });
      const error = await withTimeout(
        shutdown,
        LIVENESS_TEST_SIGNAL_MS,
        "silent worker death left public shutdown pending",
      );
      expect(outcome).toBe("rejected");
      if (!(error instanceof Error)) throw new Error("Expected public shutdown to reject");
      expect(error.message).toMatch(/outcomes are unknown.*not retried/);
    } finally {
      // Preserve setup/assertion failures without leaving this deliberately
      // killed fixture in the shared afterEach's unbounded shutdown loop.
      if (db) {
        untrack(db);
        void db.shutdown().catch(() => undefined);
      }
      control.postMessage({ type: "die" });
      control.close();
      setBrowserFollowerProbeTimingForTest();
    }
  }, 60_000);

  it("settles public shutdown after a pending follower operation rejects on silent worker death", async () => {
    const capability = uniqueDbName("follower-death-cleanup");
    setBrowserFollowerProbeTimingForTest(LIVENESS_TEST_PROBE_TIMING);
    const workerUrl = new URL(await workerFaultBundleUrl(), globalThis.location.href);
    workerUrl.searchParams.set("followerFault", capability);
    const control = new BroadcastChannel(capability);
    const receive = (type: string) =>
      new Promise<void>((resolve) => {
        const listener = (event: MessageEvent<{ type: string }>) => {
          if (event.data.type !== type) return;
          control.removeEventListener("message", listener);
          resolve();
        };
        control.addEventListener("message", listener);
      });
    let db: Db | undefined;
    try {
      db = track(
        await createDb({
          appId: "follower-death-cleanup",
          driver: { type: "persistent", dbName: uniqueDbName("follower-death-cleanup") },
          schema: app,
          runtimeSources: {
            brokerWorkerUrl: workerUrl.href,
            wasmVersion: "follower-liveness-test",
          },
        }),
      );
      await db.all(allTodos, { tier: "local" });
      const armed = receive("holding-pending-writes");
      control.postMessage({ type: "hold-pending-writes" });
      await withTimeout(armed, 5_000, "test worker did not arm the pending control hold");
      const held = receive("pending-writes-held");
      const pending = db.shutdown({ waitForSync: true }).then(
        () => undefined,
        (error: unknown) => error,
      );
      await withTimeout(held, 5_000, "public graceful shutdown did not reach the worker barrier");
      control.postMessage({ type: "die" });
      const error = await withTimeout(
        pending,
        LIVENESS_TEST_SIGNAL_MS,
        "silent worker death left the follower operation pending",
      );
      if (!(error instanceof Error) || !(error.cause instanceof Error)) {
        throw new Error("Expected graceful shutdown to retain the follower failure cause");
      }
      expect(error.cause.message).toMatch(/outcomes are unknown.*not retried/);

      // A terminal local cleanup failure is honest; an unanswered companion
      // lease-return RPC must not keep the public Db alive indefinitely.
      // Both observers precede the guard so rejection is not confused with a
      // harness timeout and does not become an unhandled rejection.
      const cleanup = db.shutdown().catch(() => undefined);
      await withTimeout(
        cleanup,
        5_000,
        "Db.shutdown remained pending after the follower connection had already failed",
      );
    } finally {
      // Setup failures and the bounded cleanup assertion above remain the
      // failure signal; never repeat a hanging shutdown in shared afterEach.
      if (db) {
        untrack(db);
        void db.shutdown().catch(() => undefined);
      }
      control.postMessage({ type: "die" });
      control.close();
      setBrowserFollowerProbeTimingForTest();
    }
  }, 45_000);

  it("exposes a bounded redacted worker lifecycle ledger to the owning inspector", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions("worker-lifecycle-ledger");
    const db = track(
      await createDb({
        appId: syncServer.appId,
        serverUrl: syncServer.serverUrl,
        secret: generateAuthSecret(),
        driver: { type: "persistent", dbName: uniqueDbName("worker-lifecycle-ledger") },
        logLevel: "trace",
        schema: app,
      }),
    );
    // `createDb` resolves after the foreground runtime is available; the
    // worker follower is installed on the first public read.
    await db.all(allTodos, { tier: "local" });
    const inspector = await db.openInspectorControlPort();
    inspector.start();
    try {
      const entries = await listWorkerLifecycle(inspector);
      expect(entries.map((entry) => entry.event)).toEqual(
        expect.arrayContaining(["bootstrap-start", "peer-attached"]),
      );
      expect(entries).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            sequence: expect.any(Number),
            peerCount: expect.any(Number),
            pendingBootstraps: expect.any(Number),
            activeLeases: expect.any(Number),
          }),
        ]),
      );
    } finally {
      inspector.postMessage({ type: "close" } satisfies BrowserInspectorControlRequest);
    }
  });

  it("registers concurrent local subscriptions before worker admission while withholding openings", async () => {
    const db = track(
      await createDb({
        appId: "concurrent-local-subscription-admission",
        secret: generateAuthSecret(),
        driver: { type: "persistent", dbName: uniqueDbName("concurrent-local-subscription") },
      }),
    );
    // Selecting the schema begins the worker handshake but cannot complete it
    // in this same call stack. The registration spy distinguishes the required
    // native ordering from the old workaround that waited before subscribing.
    const client = (
      db as unknown as {
        getClient(schema: typeof todos._schema): {
          subscribeInternal: (...args: never[]) => number;
        };
      }
    ).getClient(todos._schema);
    const nativeSubscribe = vi.spyOn(client, "subscribeInternal");
    const source = getDbSubscriptionSource(db);
    const firstDeltas: unknown[] = [];
    const secondDeltas: unknown[] = [];
    const first = source.subscribeDelta(todos, (delta) => firstDeltas.push(delta), {
      tier: "local",
    });
    const second = source.subscribeDelta(todos, (delta) => secondDeltas.push(delta), {
      tier: "local",
    });
    try {
      expect(first.ready).toBeDefined();
      expect(second.ready).toBeDefined();
      expect(nativeSubscribe).toHaveBeenCalledTimes(2);
      expect(firstDeltas).toEqual([]);
      expect(secondDeltas).toEqual([]);
      await expect(Promise.all([first.ready, second.ready])).resolves.toEqual([
        undefined,
        undefined,
      ]);
    } finally {
      first();
      second();
    }
  });

  it("rejects createDb operation-scoped when its foreground lease cannot open durable storage", async () => {
    const ambientErrors: string[] = [];
    const unhandledRejections: string[] = [];
    const recordAmbientError = (event: ErrorEvent) => {
      ambientErrors.push(event.error instanceof Error ? event.error.message : event.message);
    };
    const recordUnhandledRejection = (event: PromiseRejectionEvent) => {
      event.preventDefault();
      unhandledRejections.push(
        event.reason instanceof Error ? event.reason.message : String(event.reason),
      );
    };
    globalThis.addEventListener("error", recordAmbientError);
    globalThis.addEventListener("unhandledrejection", recordUnhandledRejection);
    errorListeners.add(recordAmbientError);
    const dbName = uniqueDbName("corrupt-storage-open");
    const secret = generateAuthSecret();
    const config = { appId: "test-app", secret, driver: { type: "persistent" as const, dbName } };
    try {
      const initial = track(await createDb(config));
      await initial
        .insert(todos, { title: "durable sentinel", done: false })
        .wait({ tier: "local" });
      await initial.shutdown();
      untrack(initial);
      // The last follower releases its worker context after the short idle
      // window. Without this, a cached worker runtime never reopens the raw
      // IndexedDB namespace and cannot observe the corruption below.
      await sleep(100);

      // Local-first caller credentials are normalized to a canonical session
      // during `createDb`, so the actual physical root must be derived from
      // the resolved Db config rather than the pre-normalization input.
      const physicalDbName = resolveDefaultPersistentDbName(initial.config);

      await replaceStorageManifest(physicalDbName, {
        ...INDEXEDDB_STORAGE_MANIFEST,
        storageEpoch: 2,
      });
      const recordsBeforeRead = await rawStorageRecords(physicalDbName);

      // Persistent create must acquire a durable foreground-node lease before
      // any synchronous mutation can mint a transaction identity. Storage
      // readiness therefore belongs to createDb, while schema selection stays
      // lazy. The original structured worker error must reject that operation
      // directly instead of collapsing to a message-only main-thread Error.
      let openFailure: unknown;
      try {
        await createDb(config);
      } catch (error) {
        openFailure = error;
      }
      expect(openFailure).toBeInstanceOf(Error);
      if (!(openFailure instanceof Error)) throw new Error("Expected browser worker open to fail");
      expect(openFailure).toMatchObject({
        name: "Error",
        message: "Missing or invalid IndexedDB storage epoch manifest",
        stack: expect.stringContaining("Missing or invalid IndexedDB storage epoch manifest"),
      });
      expect(openFailure.cause).toBeUndefined();
      await sleep(0);
      expect(ambientErrors).toEqual([]);
      expect(unhandledRejections).toEqual([]);
      expect(await rawStorageRecords(physicalDbName)).toEqual(recordsBeforeRead);
    } finally {
      globalThis.removeEventListener("error", recordAmbientError);
      globalThis.removeEventListener("unhandledrejection", recordUnhandledRejection);
      errorListeners.delete(recordAmbientError);
    }
  });

  // -------------------------------------------------------------------------
  // 2. Insert + local query through worker bridge
  // -------------------------------------------------------------------------

  it("inserts a row and queries it back", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("insert-query") },
      }),
    );

    // Insert (sync — runs on main-thread in-memory runtime)
    const {
      value: { id },
    } = db.insert(todos, { title: "Buy milk", done: false });
    expect(id).toBeTruthy();
    expect(typeof id).toBe("string");

    // Query (async — runs on main-thread runtime)
    const results = await db.all(allTodos);
    expect(results.length).toBe(1);
    expect(results[0].id).toBe(id);
    expect(results[0].title).toBe("Buy milk");
    expect(results[0].done).toBe(false);
  });

  it("inserts multiple rows and queries all", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("multi-insert") },
      }),
    );

    db.insert(todos, { title: "Task A", done: false });
    db.insert(todos, { title: "Task B", done: true });
    db.insert(todos, { title: "Task C", done: false });

    const results = await db.all(allTodos);
    expect(results.length).toBe(3);

    const titles = results.map((r) => r.title).sort();
    expect(titles).toEqual(["Task A", "Task B", "Task C"]);
  });

  it("sync insert before bridge init is persisted after init completes", async () => {
    const dbName = uniqueDbName("sync-insert-before-bridge-ready");
    const db1 = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );

    // First I/O operation, bridge hasn't been initialized yet.
    const {
      value: { id },
    } = db1.insert(todos, { title: "Test", done: false });

    await waitForCondition(
      async () => {
        const row = await db1.one(allTodos, { tier: "local" });
        return row?.id === id;
      },
      8_000,
      "sync insert should be forwarded to worker after bridge init",
    );

    await db1.shutdown();
    untrack(db1);

    const db2 = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );

    const persistedRow = await db2.one(allTodos, { tier: "local" });
    expect(persistedRow?.id).toBe(id);
  });

  // -------------------------------------------------------------------------
  // 3. Update + delete through worker bridge
  // -------------------------------------------------------------------------

  it("updates a row", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("update") },
      }),
    );

    const { value: inserted } = db.insert(todos, {
      title: "Original",
      done: false,
    });
    const { id } = inserted;
    const result = db.update(todos, id, { done: true });
    expect(result).toMatchObject({
      wait: expect.any(Function),
    });

    const results = await db.all(allTodos);
    expect(results.length).toBe(1);
    expect(results[0].title).toBe("Original");
    expect(results[0].done).toBe(true);
  });

  it("updates a row durably", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("update-durable") },
      }),
    );

    const { id } = await db
      .insert(todos, { title: "Original", done: false })
      .wait({ tier: "local" });

    const updateHandle = db.update(todos, id, { done: true });
    await updateHandle.wait({ tier: "local" });

    const results = await db.all(allTodos, { tier: "local" });
    expect(results.length).toBe(1);
    expect(results[0].done).toBe(true);
  });

  it("deletes a row", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("delete") },
      }),
    );

    const { value: inserted } = db.insert(todos, {
      title: "Ephemeral",
      done: false,
    });
    const { id } = inserted;
    expect((await db.all(allTodos)).length).toBe(1);

    const result = db.delete(todos, id);
    expect(result).toMatchObject({
      wait: expect.any(Function),
    });
    const results = await db.all(allTodos);
    expect(results.length).toBe(0);
  });

  it("deletes a row durably", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("delete-durable") },
      }),
    );

    const { id } = await db
      .insert(todos, { title: "Ephemeral", done: false })
      .wait({ tier: "local" });
    expect((await db.all(allTodos, { tier: "local" })).length).toBe(1);

    const deleteHandle = db.delete(todos, id);
    await deleteHandle.wait({ tier: "local" });

    const results = await db.all(allTodos, { tier: "local" });
    expect(results.length).toBe(0);
  });

  // -------------------------------------------------------------------------
  // 4. IndexedDB persistence across shutdown + re-open
  // -------------------------------------------------------------------------

  it("persists data across shutdown and re-create", async () => {
    const dbName = uniqueDbName("persistence");

    const db1 = await createDb({
      appId: "test-app",
      driver: { type: "persistent", dbName },
    });
    db1.insert(todos, { title: "Survive reload", done: true });
    const before = await db1.all(allTodos);
    expect(before.length).toBe(1);
    await db1.shutdown();

    // A new Db with the same namespace reopens the IndexedDB tree.
    const db2 = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );
    const after = await db2.all(allTodos, { tier: "local" });
    expect(after.length).toBe(1);
    expect(after[0].title).toBe("Survive reload");
    expect(after[0].done).toBe(true);
  });

  it("first local subscription snapshot returns no rows for an empty store", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("empty-snapshot") },
      }),
    );
    const firstSnapshot = new Promise<Todo[]>((resolve) => {
      trackSubscription(db.subscribe(allTodos, resolve, { tier: "local" }));
    });
    await expect(firstSnapshot).resolves.toEqual([]);
  });

  it("first local subscription snapshot contains persisted data", async () => {
    const config = {
      appId: "test-app",
      secret: generateAuthSecret(),
      driver: { type: "persistent" as const, dbName: uniqueDbName("first-snapshot-reopen") },
    };
    const seeded = track(await createDb(config));
    const expected: { id: string; title: string; done: boolean }[] = [];
    for (let index = 0; index < 3; index++) {
      const inserted = seeded.insert(todos, { title: `Persisted ${index}`, done: false });
      const row = await inserted.wait({ tier: "local" });
      expected.push({ id: row.id, title: row.title, done: row.done });
    }
    await seeded.all(allTodos, { tier: "local" });
    await shutdownDbAndWorker(seeded);

    const reopened = track(await createDb(config));
    const snapshots: (typeof expected)[] = [];
    // Subscribe before any read or readiness wait can warm the reopened
    // runtime. An empty callback followed by the stored rows must fail.
    trackSubscription(
      reopened.subscribe(
        todos.orderBy("title"),
        (rows) => snapshots.push(rows.map(({ id, title, done }) => ({ id, title, done }))),
        { tier: "local" },
      ),
    );
    await waitForCondition(
      async () => snapshots.length > 0,
      5000,
      "The initial local snapshot should contain the persisted rows",
    );
    expect(snapshots[0]).toEqual(expected);
  }, 15_000);

  it("deletes IndexedDB storage for the current namespace and keeps the same Db usable", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("delete-storage") },
      }),
    );

    await db.insert(todos, { title: "Should be deleted", done: false }).wait({ tier: "local" });
    const before = await db.all(allTodos, { tier: "local" });
    expect(before.length).toBe(1);
    expect(before[0].title).toBe("Should be deleted");

    await db.deleteClientStorage();

    const afterDelete = await db.all(allTodos, { tier: "local" });
    expect(afterDelete).toEqual([]);

    const {
      value: { id },
    } = db.insert(todos, { title: "Fresh after delete", done: true });
    const afterReinsert = await db.all(allTodos, { tier: "local" });
    expect(afterReinsert).toHaveLength(1);
    expect(afterReinsert[0].id).toBe(id);
    expect(afterReinsert[0].title).toBe("Fresh after delete");
    expect(afterReinsert[0].done).toBe(true);
  });

  it("shuts down immediately after a storage reset and reopens the cleared root", async () => {
    const dbName = uniqueDbName("delete-storage-shutdown");
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );

    await db.insert(todos, { title: "Before reset", done: false }).wait({ tier: "local" });
    await db.deleteClientStorage();

    await withTimeout(
      db.shutdown(),
      5_000,
      "Db shutdown did not settle immediately after resetting SharedWorker storage",
    );
    untrack(db);

    const reopened = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );
    expect(await reopened.all(allTodos, { tier: "local" })).toEqual([]);

    await reopened
      .insert(todos, { title: "Fresh after reset and reopen", done: true })
      .wait({ tier: "local" });
    expect(await reopened.all(allTodos, { tier: "local" })).toMatchObject([
      { title: "Fresh after reset and reopen", done: true },
    ]);

    await withTimeout(
      reopened.shutdown(),
      5_000,
      "Reopened Db shutdown did not settle after resetting SharedWorker storage",
    );
    untrack(reopened);
  });

  it("resolves a storage reset requested before any schema use", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("delete-storage-fresh") },
      }),
    );

    // No table/query has run yet: no client exists anywhere in the namespace.
    await db.deleteClientStorage();

    // The same Db must create a fresh shared runtime on first schema use.
    await db
      .insert(todos, { title: "first row after fresh wipe", done: false })
      .wait({ tier: "local" });
    expect(await db.all(allTodos, { tier: "local" })).toHaveLength(1);
  });

  it("resolves a fresh-namespace storage reset while a second fresh tab is open", async () => {
    const dbName = uniqueDbName("delete-storage-fresh-two-tabs");
    const dbA = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );
    const dbB = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );

    // Neither tab has used the schema; both join the reset as participants.
    await dbB.deleteClientStorage();

    // First schema use after the wipe creates the shared runtime; the other
    // fresh tab must attach and observe the write.
    await dbA
      .insert(todos, { title: "row after two-tab fresh wipe", done: false })
      .wait({ tier: "local" });
    await waitForCondition(
      async () => (await dbB.all(allTodos, { tier: "local" })).length === 1,
      8000,
      "Second fresh tab should observe the row written after the wipe",
    );
  });

  it("deletes IndexedDB storage across two tabs when requested by either tab", async () => {
    const dbName = uniqueDbName("delete-storage-two-tabs");
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
    await dbA
      .insert(todos, { title: "First tab data before wipe", done: false })
      .wait({ tier: "local" });
    await dbB
      .insert(todos, {
        title: "Second tab data before wipe",
        done: true,
      })
      .wait({ tier: "local" });

    await waitForCondition(
      async () => {
        const firstRows = await dbA.all(allTodos, { tier: "local" });
        const secondRows = await dbB.all(allTodos, { tier: "local" });
        return firstRows.length === 2 && secondRows.length === 2;
      },
      8000,
      "Both tabs should observe pre-wipe rows",
    );

    await dbB.deleteClientStorage();

    await waitForCondition(
      async () => {
        const firstRows = await dbA.all(allTodos, { tier: "local" });
        const secondRows = await dbB.all(allTodos, { tier: "local" });
        return firstRows.length === 0 && secondRows.length === 0;
      },
      12000,
      "A storage wipe should clear both tabs",
    );

    const marker = `fresh-after-two-tab-wipe-${Date.now()}`;
    await dbA.insert(todos, { title: marker, done: false }).wait({ tier: "local" });

    await waitForCondition(
      async () => {
        const firstRows = await dbA.all(allTodos, { tier: "local" });
        const secondRows = await dbB.all(allTodos, { tier: "local" });
        const firstHas = firstRows.some((row) => row.title === marker);
        const secondHas = secondRows.some((row) => row.title === marker);
        return firstHas && secondHas;
      },
      12000,
      "Both tabs should recover cleanly after two-tab storage wipe",
    );
  });

  it("reloads every attached tab when IndexedDB is externally deleted with dirty writes", async () => {
    const dbName = uniqueDbName("external-indexeddb-delete");
    const remoteDbId = trackRemoteBrowserDb(uniqueDbName("external-indexeddb-delete-page"));
    await createRemoteBrowserDb({
      id: remoteDbId,
      appId: "test-app",
      dbName,
      table: "todos",
      schemaJson: JSON.stringify(app.wasmSchema),
      initialize: true,
      tabCount: 2,
      initialRow: { title: "dirty before external deletion", done: false },
    });

    await deleteRemoteBrowserIndexedDbAndWaitForReload(
      remoteDbId,
      resolveDefaultPersistentDbName({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );
  });

  it("logout with wipeData clears browser storage before the next session opens", async () => {
    const dbName = uniqueDbName("logout-wipe");
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );

    await db
      .insert(todos, { title: "Should be wiped on logout", done: false })
      .wait({ tier: "local" });
    expect((await db.all(allTodos, { tier: "local" })).length).toBe(1);

    await db.logout({ wipeData: true });
    untrack(db);

    const reopened = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );
    const rows = await reopened.all(allTodos, { tier: "local" });
    expect(rows).toEqual([]);
  });

  it("rehydrates current catalogue schema and lens state after persistent worker reopen", async () => {
    const protocolErrors: string[] = [];
    const recordProtocolError = (event: ErrorEvent) => {
      const message = event.error instanceof Error ? event.error.message : event.message;
      if (message.includes("invalid catalogue update")) {
        protocolErrors.push(message);
      }
    };
    globalThis.addEventListener("error", recordProtocolError);
    errorListeners.add(recordProtocolError);

    const dbName = uniqueDbName("catalogue-current-schema-rehydrate");
    const testingServer = await publishCatalogueSchemaFamily("catalogue-current-schema-rehydrate");
    const jwtToken = await getJazzServerJwtForUser(
      "catalogue-current-schema-rehydrate",
      undefined,
      testingServer.appId,
    );

    const seeded = track(
      await createDb({
        appId: testingServer.appId,
        serverUrl: testingServer.serverUrl,
        jwtToken,
        registerJwt: true,
        driver: { type: "persistent", dbName },
      }),
    );

    const marker = `catalogue-current-schema-rehydrate-${Date.now()}`;
    await seeded
      .insert(catalogueTodos, {
        title: marker,
        completed: false,
        description: "written with the current schema",
      })
      .wait({ tier: "global" });

    await waitForCatalogueTodos(
      seeded,
      (rows) => rows.some((row) => row.title === marker && row.description?.includes("current")),
      "initial current-schema query should read the persisted row",
      15_000,
      "local",
    );

    await seeded.shutdown();
    untrack(seeded);

    const reopened = track(
      await createDb({
        appId: testingServer.appId,
        serverUrl: testingServer.serverUrl,
        jwtToken,
        registerJwt: true,
        driver: { type: "persistent", dbName },
      }),
    );

    const rowsAfterReopen = await waitForCatalogueTodos(
      reopened,
      (rows) => rows.some((row) => row.title === marker && row.description?.includes("current")),
      "reopened persistent worker should rehydrate current schema and lenses before querying",
      15_000,
      "local",
    );
    expect(rowsAfterReopen.find((row) => row.title === marker)?.completed).toBe(false);

    const remote = track(
      await createDb({
        appId: testingServer.appId,
        serverUrl: testingServer.serverUrl,
        jwtToken,
        registerJwt: true,
        driver: { type: "persistent", dbName: uniqueDbName("catalogue-remote-authority") },
      }),
    );
    const remoteMarker = `catalogue-remote-authority-${Date.now()}`;
    await remote
      .insert(catalogueTodos, {
        title: remoteMarker,
        completed: true,
        description: "written by an independent server-connected client",
      })
      .wait({ tier: "global" });

    const authoritativeRows = await waitForCatalogueTodos(
      reopened,
      (rows) => rows.some((row) => row.title === remoteMarker && row.completed),
      "reopened worker should receive authoritative current-schema rows from the server",
      15_000,
      "global",
    );
    expect(authoritativeRows.find((row) => row.title === remoteMarker)?.description).toContain(
      "independent",
    );

    await sleep(100);
    expect(protocolErrors).toEqual([]);
    globalThis.removeEventListener("error", recordProtocolError);
    errorListeners.delete(recordProtocolError);
  }, 60_000);

  it.runIf(__JAZZ_BROWSER_SOAK__ === "1")(
    "survives repeated durable writes across fresh SharedWorker lifecycles",
    async () => {
      for (let round = 0; round < 24; round += 1) {
        const db = track(
          await createDb({
            appId: "test-app",
            driver: {
              type: "persistent",
              dbName: uniqueDbName(`durable-lifecycle-soak-${round}`),
            },
          }),
        );
        const inserted = await db
          .insert(todos, { title: `durable-${round}`, done: false })
          .wait({ tier: "local" });
        await db.update(todos, inserted.id, { done: true }).wait({ tier: "local" });
        expect(await db.all(allTodos, { tier: "local" })).toEqual([{ ...inserted, done: true }]);
        await db.shutdown();
        untrack(db);
      }
    },
    180_000,
  );

  it.runIf(__JAZZ_BROWSER_SOAK__ === "1")(
    "survives randomized concurrent writes and SharedWorker restarts without cardinality drift",
    async () => {
      const seed = 0x5eed_1703;
      let randomState = seed;
      const random = () => {
        randomState += 0x6d2b_79f5;
        let value = randomState;
        value = Math.imul(value ^ (value >>> 15), value | 1);
        value ^= value + Math.imul(value ^ (value >>> 7), value | 61);
        return ((value ^ (value >>> 14)) >>> 0) / 4_294_967_296;
      };
      const remoteDbId = trackRemoteBrowserDb(uniqueDbName("worker-restart-soak"));
      await withTimeout(
        createRemoteBrowserDb({
          id: remoteDbId,
          appId: "test-app",
          dbName: uniqueDbName("worker-restart-soak-store"),
          table: "todos",
          schemaJson: JSON.stringify(app.wasmSchema),
          tabCount: 3,
          initialize: true,
        }),
        20_000,
        "Soak initial three-tab open timed out",
      );
      const expectedTitles = new Set<string>();
      for (let round = 0; round < 12; round += 1) {
        const writes = Array.from({ length: 3 + Math.floor(random() * 7) }, (_, index) => ({
          row: {
            title: `soak-${seed.toString(16)}-${round}-${index}-${Math.floor(random() * 1e9)}`,
            done: random() < 0.5,
          },
          tabIndex: Math.floor(random() * 3),
        }));
        writes.forEach(({ row }) => expectedTitles.add(row.title));
        await withTimeout(
          Promise.all(
            writes.map(({ row, tabIndex }) => insertRemoteBrowserDbRow(remoteDbId, tabIndex, row)),
          ),
          20_000,
          `Soak round ${round} writes timed out`,
        );
        await waitForCondition(
          async () =>
            (await queryRemoteBrowserDbRows(remoteDbId, round % 3)).length === expectedTitles.size,
          10_000,
          `Soak round ${round} should converge before restart`,
        );
        try {
          await withTimeout(
            restartRemoteBrowserDb(remoteDbId),
            20_000,
            `Soak round ${round} worker restart timed out`,
          );
        } catch (error) {
          throw new Error(`Soak restart failed in round ${round}`, { cause: error });
        }
        const snapshots = await withTimeout(
          Promise.all([0, 1, 2].map((tabIndex) => queryRemoteBrowserDbRows(remoteDbId, tabIndex))),
          20_000,
          `Soak round ${round} snapshots timed out`,
        );
        for (const snapshot of snapshots) {
          expect(snapshot).toHaveLength(expectedTitles.size);
          expect(new Set(snapshot.map((row) => row.title))).toEqual(expectedTitles);
        }
      }
    },
    180_000,
  );
});
