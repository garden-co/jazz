/**
 * Browser integration tests for Worker Bridge + OPFS persistence.
 *
 * Runs in a real Chromium browser via @vitest/browser + playwright.
 * Uses real jazz-wasm, real dedicated Workers, real OPFS storage.
 *
 * Server sync tests use a real jazz-tools server spawned by global-setup.
 */

import { describe, it, expect, afterEach } from "vitest";
import { createDb, Db, type QueryBuilder, type TableProxy } from "../../src/runtime/db.js";
import type { WasmSchema } from "../../src/drivers/types.js";
import { TEST_PORT, ADMIN_SECRET, APP_ID } from "./test-constants.js";

interface DebugLensEdgeState {
  sourceHash: string;
  targetHash: string;
}

interface DebugSchemaState {
  currentSchemaHash: string;
  liveSchemaHashes: string[];
  knownSchemaHashes: string[];
  pendingSchemaHashes: string[];
  lensEdges: DebugLensEdgeState[];
}

// ---------------------------------------------------------------------------
// Test schema — a simple "todos" table
// ---------------------------------------------------------------------------

const schema: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
      { name: "project", column_type: { type: "Uuid" }, nullable: true, references: "projects" },
      {
        name: "tags",
        column_type: { type: "Array", element: { type: "Text" } },
        nullable: true,
      },
    ],
  },
  projects: {
    columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
  },
};

interface Todo {
  id: string;
  title: string;
  done: boolean;
  project?: string;
  tags?: string[];
}

interface TodoInit {
  title: string;
  done: boolean;
  project?: string;
  tags?: string[];
}

interface Project {
  id: string;
  name: string;
}

interface ProjectInit {
  name: string;
}

const todos: TableProxy<Todo, TodoInit> = {
  _table: "todos",
  _schema: schema,
  _rowType: {} as Todo,
  _initType: {} as TodoInit,
};

const projects: TableProxy<Project, ProjectInit> = {
  _table: "projects",
  _schema: schema,
  _rowType: {} as Project,
  _initType: {} as ProjectInit,
};

/** QueryBuilder that selects all todos. */
const allTodos: QueryBuilder<Todo> = {
  _table: "todos",
  _schema: schema,
  _rowType: {} as Todo,
  _build() {
    return JSON.stringify({
      table: "todos",
      conditions: [],
      includes: {},
      orderBy: [],
    });
  },
};

/** QueryBuilder that selects all todos by project. */
function todosByProject(projectId: string): QueryBuilder<Todo> {
  return {
    _table: "todos",
    _schema: schema,
    _rowType: {} as Todo,
    _build() {
      return JSON.stringify({
        table: "todos",
        conditions: [{ column: "project", op: "eq", value: projectId }],
        includes: {},
        orderBy: [],
      });
    },
  };
}

// Fixture schema family pushed by global-setup (`examples/todo-server-rs/schema`), v2.
const catalogueSchemaV1: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "completed", column_type: { type: "Boolean" }, nullable: false },
    ],
  },
};

const catalogueSchemaV2: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "completed", column_type: { type: "Boolean" }, nullable: false },
      { name: "description", column_type: { type: "Text" }, nullable: true },
    ],
  },
};

interface CatalogueTodo {
  id: string;
  title: string;
  completed: boolean;
  description?: string;
}

const allCatalogueTodos: QueryBuilder<CatalogueTodo> = {
  _table: "todos",
  _schema: catalogueSchemaV2,
  _rowType: {} as CatalogueTodo,
  _build() {
    return JSON.stringify({
      table: "todos",
      conditions: [],
      includes: {},
      orderBy: [],
    });
  },
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Generate a unique dbName to isolate OPFS state between tests. */
function uniqueDbName(label: string): string {
  return `test-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("Worker Bridge with OPFS", () => {
  const dbs: Db[] = [];
  const subscriptions: Array<() => void> = [];

  /** Track dbs for cleanup. */
  function track(db: Db): Db {
    dbs.push(db);
    return db;
  }

  /** Track subscriptions so they are always cleaned up, even on assertion failures. */
  function trackSubscription(unsubscribe: () => void): () => void {
    subscriptions.push(unsubscribe);
    return () => {
      try {
        unsubscribe();
      } finally {
        const index = subscriptions.indexOf(unsubscribe);
        if (index >= 0) {
          subscriptions.splice(index, 1);
        }
      }
    };
  }

  async function createSyncedDb(label: string, localAuthToken: string): Promise<Db> {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;
    return track(
      await createDb({
        appId: APP_ID,
        driver: { type: "persistent", dbName: uniqueDbName(label) },
        serverUrl,
        localAuthMode: "anonymous",
        localAuthToken,
        adminSecret: ADMIN_SECRET,
      }),
    );
  }

  function untrack(db: Db): void {
    const index = dbs.indexOf(db);
    if (index >= 0) {
      dbs.splice(index, 1);
    }
  }

  function getTabRole(db: Db): "leader" | "follower" | null {
    const role = (db as any).tabRole;
    if (role === "leader" || role === "follower") {
      return role;
    }
    return null;
  }

  async function waitForLeaderAndFollower(a: Db, b: Db): Promise<{ leader: Db; follower: Db }> {
    await waitForCondition(
      async () => {
        const roleA = getTabRole(a);
        const roleB = getTabRole(b);
        return roleA === "leader" && roleB === "follower";
      },
      12000,
      "Expected one elected leader and one follower",
    ).catch(async () => {
      await waitForCondition(
        async () => {
          const roleA = getTabRole(a);
          const roleB = getTabRole(b);
          return roleA === "follower" && roleB === "leader";
        },
        12000,
        "Expected one elected leader and one follower",
      );
    });

    const roleA = getTabRole(a);
    const roleB = getTabRole(b);
    if (roleA === "leader" && roleB === "follower") {
      return { leader: a, follower: b };
    }
    if (roleA === "follower" && roleB === "leader") {
      return { leader: b, follower: a };
    }
    throw new Error("Unable to determine leader/follower roles");
  }

  async function waitForSingleLeader(tabs: Db[]): Promise<Db> {
    await waitForCondition(
      async () => {
        let leaders = 0;
        let knownRoles = 0;
        for (const tab of tabs) {
          const role = getTabRole(tab);
          if (!role) continue;
          knownRoles += 1;
          if (role === "leader") leaders += 1;
        }
        return knownRoles === tabs.length && leaders === 1;
      },
      12000,
      "Expected exactly one elected leader across tabs",
    );

    const leader = tabs.find((tab) => getTabRole(tab) === "leader");
    if (!leader) {
      throw new Error("Expected one leader after convergence");
    }
    return leader;
  }

  afterEach(async () => {
    for (const unsubscribe of subscriptions.splice(0)) {
      try {
        unsubscribe();
      } catch {
        // Best effort
      }
    }

    for (const db of dbs.splice(0).reverse()) {
      try {
        await db.shutdown();
      } catch {
        // Best effort
      }
    }
  });

  // -------------------------------------------------------------------------
  // 1. Worker initialization
  // -------------------------------------------------------------------------

  it("creates Db with worker in browser environment", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("init") },
      }),
    );
    expect(db).toBeDefined();
    expect(db).toBeInstanceOf(Db);
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
    const { id } = await db.insert(todos, { title: "Buy milk", done: false });
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

    await db.insert(todos, { title: "Task A", done: false });
    await db.insert(todos, { title: "Task B", done: true });
    await db.insert(todos, { title: "Task C", done: false });

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
    const { id } = db1.insert(todos, { title: "Test", done: false });

    await waitForCondition(
      async () => {
        const row = await db1.one(allTodos, { tier: "worker" });
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

    const persistedRow = await db2.one(allTodos, { tier: "worker" });
    expect(persistedRow?.id).toBe(id);
  });

  it("sync insert is not persisted if bridge fails to init", async () => {
    const dbName = uniqueDbName("sync-insert-bridge-init-failure");
    const db1 = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );

    // @ts-expect-error - worker is private
    const worker = db1.worker as Worker;
    const originalPostMessage = worker.postMessage.bind(worker);
    worker.postMessage = ((message: unknown, transfer?: Transferable[]) => {
      const typed = message as { type?: string } | undefined;
      if (typed?.type === "init") {
        queueMicrotask(() => {
          worker.dispatchEvent(
            new MessageEvent("message", {
              data: { type: "error", message: "forced bridge init failure for test" },
            }),
          );
        });
        return;
      }
      return originalPostMessage(message, { transfer });
    }) as Worker["postMessage"];

    const { id } = db1.insert(todos, { title: "Test", done: false });
    expect(id).toBeDefined();

    worker.postMessage = originalPostMessage;
    // Shutdown fails to ensure bridge is ready, but steps down as leader before that
    await expect(db1.shutdown()).rejects.toThrow(
      "Worker init failed: forced bridge init failure for test",
    );

    untrack(db1);

    const db2 = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );

    const persistedRows = await db2.all(allTodos, { tier: "worker" });
    expect(persistedRows.length).toEqual(0);
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

    const { id } = db.insert(todos, { title: "Original", done: false });
    const result = db.update(todos, id, { done: true });
    expect(result).toBeUndefined();

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

    const { id } = await db.insertDurable(
      todos,
      { title: "Original", done: false },
      { tier: "worker" },
    );
    const pending = db.updateDurable(todos, id, { done: true }, { tier: "worker" });
    expect(pending).toBeInstanceOf(Promise);
    await pending;

    const results = await db.all(allTodos, { tier: "worker" });
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

    const { id } = db.insert(todos, { title: "Ephemeral", done: false });
    expect((await db.all(allTodos)).length).toBe(1);

    const result = db.delete(todos, id);
    expect(result).toBeUndefined();
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

    const { id } = await db.insertDurable(
      todos,
      { title: "Ephemeral", done: false },
      { tier: "worker" },
    );
    expect((await db.all(allTodos, { tier: "worker" })).length).toBe(1);

    const pending = db.deleteDurable(todos, id, { tier: "worker" });
    expect(pending).toBeInstanceOf(Promise);
    await pending;

    const results = await db.all(allTodos, { tier: "worker" });
    expect(results.length).toBe(0);
  });

  // -------------------------------------------------------------------------
  // 4. OPFS persistence across shutdown + re-open
  // -------------------------------------------------------------------------

  it("persists data across shutdown and re-create (OPFS)", async () => {
    const dbName = uniqueDbName("persistence");

    const db1 = await createDb({ appId: "test-app", driver: { type: "persistent", dbName } });
    await db1.insert(todos, { title: "Survive reload", done: true });
    const before = await db1.all(allTodos);
    expect(before.length).toBe(1);
    await db1.shutdown();

    // New Db with same dbName — worker reopens OPFS, main thread starts empty.
    // Using "worker" settled tier makes the query wait for the worker's
    // QuerySettled response, ensuring OPFS data arrives before resolving.
    const db2 = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );
    const after = await db2.all(allTodos, { tier: "worker" });
    expect(after.length).toBe(1);
    expect(after[0].title).toBe("Survive reload");
    expect(after[0].done).toBe(true);
  });

  it("recovers data from WAL after crash (no snapshot flush)", async () => {
    const dbName = uniqueDbName("crash-recovery");

    const db1 = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );

    // insert({ tier: "worker" }) ensures data is in OPFS WAL before we crash
    await db1.insertDurable(todos, { title: "Crash-proof", done: false }, { tier: "worker" });
    await db1.insertDurable(todos, { title: "Also survives", done: true }, { tier: "worker" });

    // Simulate crash: release OPFS handles WITHOUT flushing snapshot.
    // WAL has the data, but snapshot is stale. Recovery must replay WAL.
    // (Real worker.terminate() doesn't reliably release OPFS exclusive
    // locks within the same page session — only a full page reload does.)
    await (db1 as any).ensureBridgeReady();
    const worker = (db1 as any).worker as Worker;
    worker.postMessage({ type: "simulate-crash" });
    await waitForWorkerMessageType(worker, "shutdown-ok", 5000, "simulate-crash");
    worker.terminate();
    // Null out dead worker bridge so Db shutdown only frees client-side resources.
    (db1 as any).worker = null;
    (db1 as any).workerBridge = null;
    await db1.shutdown();

    // New Db with same dbName — worker must recover from OPFS WAL
    const db2 = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );
    const after = await db2.all(allTodos, { tier: "worker" });
    expect(after.length).toBe(2);

    const titles = after.map((r) => r.title).sort();
    expect(titles).toEqual(["Also survives", "Crash-proof"]);
  });

  it("deletes OPFS storage for the current namespace and keeps the same Db usable", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("delete-storage") },
      }),
    );

    await db.insertDurable(todos, { title: "Should be deleted", done: false }, { tier: "worker" });
    const before = await db.all(allTodos, { tier: "worker" });
    expect(before.length).toBe(1);
    expect(before[0].title).toBe("Should be deleted");

    await db.deleteClientStorage();

    const afterDelete = await db.all(allTodos, { tier: "worker" });
    expect(afterDelete).toEqual([]);

    const { id } = await db.insert(todos, { title: "Fresh after delete", done: true });
    const afterReinsert = await db.all(allTodos, { tier: "worker" });
    expect(afterReinsert).toHaveLength(1);
    expect(afterReinsert[0].id).toBe(id);
    expect(afterReinsert[0].title).toBe("Fresh after delete");
    expect(afterReinsert[0].done).toBe(true);
  });

  it("rehydrates worker catalogue schemas/lenses and restores them on main thread", async () => {
    const dbName = uniqueDbName("catalogue-schema-lens-rehydrate");
    const seeded = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );

    // Initialize worker/main runtimes with schema v2 from client context.
    await seeded.all(allCatalogueTodos, { tier: "worker" });

    // Seed historical v1 schema + auto lens v1->v2 directly into worker OPFS.
    await seedWorkerLiveSchema(seeded, catalogueSchemaV1);

    await waitForCondition(
      async () => {
        const state = await getWorkerDebugSchemaState(seeded);
        return hasRestoredCatalogueState(state);
      },
      12_000,
      "Seeded worker should hold schema/lens state beyond client context",
    );

    await seeded.shutdown();
    untrack(seeded);

    const offline = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );
    await offline.all(allCatalogueTodos, { tier: "worker" });

    await waitForCondition(
      async () => {
        const state = await getWorkerDebugSchemaState(offline);
        return hasRestoredCatalogueState(state);
      },
      12_000,
      "Offline worker should rehydrate schema/lens state from OPFS manifest",
    );

    await waitForCondition(
      async () => {
        await offline.all(allCatalogueTodos, { tier: "worker" });
        const mainState = getMainDebugSchemaState(offline, catalogueSchemaV2);
        return hasRestoredCatalogueState(mainState);
      },
      12_000,
      "Main thread should restore schema/lens state via worker catalogue sync",
    );
  }, 90_000);

  // -------------------------------------------------------------------------
  // 5. Durable insert resolves at worker tier
  // -------------------------------------------------------------------------

  it("insert resolves when worker acks", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("with-ack") },
      }),
    );

    // insert("worker") should resolve once the worker's OPFS has it
    const { id } = await db.insertDurable(
      todos,
      { title: "Durable", done: false },
      { tier: "worker" },
    );
    expect(id).toBeTruthy();
    expect(typeof id).toBe("string");

    // Data should be visible locally
    const results = await db.all(allTodos);
    expect(results.length).toBe(1);
    expect(results[0].id).toBe(id);
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
      db.subscribeAll(allTodos, (delta) => {
        received.push([...delta.all]);
      }),
    );

    await db.insert(todos, { title: "Observed", done: false });

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

    const { id: projectId } = await db.insert(projects, { name: "Observed Project" });
    const unsub = trackSubscription(
      db.subscribeAll(todosByProject(projectId), (delta) => {
        received.push([...delta.all]);
      }),
    );

    await db.insert(todos, { title: "Observed", done: false, project: projectId });
    const { id: anotherProjectId } = await db.insert(projects, { name: "Ignored Project" });
    await db.insert(todos, { title: "Not observed", done: false, project: anotherProjectId });

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

  it("forwards page lifecycle hints from main thread to worker bridge", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("lifecycle") },
      }),
    );

    await db.insert(todos, { title: "Prime bridge", done: false });
    await (db as any).ensureBridgeReady();

    const bridge = (db as any).workerBridge;
    expect(bridge).toBeTruthy();

    const seenEvents: string[] = [];
    const originalSendLifecycleHint = bridge.sendLifecycleHint.bind(bridge);
    bridge.sendLifecycleHint = (event: string) => {
      seenEvents.push(event);
      originalSendLifecycleHint(event);
    };

    (db as any).onPageHide();
    (db as any).onPageFreeze();
    (db as any).onPageResume();

    expect(seenEvents).toEqual(["pagehide", "freeze", "resume"]);
  });

  // -------------------------------------------------------------------------
  // 7. Server sync through worker
  // -------------------------------------------------------------------------

  it("propagates synced row from client A to client B", async () => {
    const sharedLocalAuthToken = `sync-token-a-to-b-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const dbA = await createSyncedDb("sync-a", sharedLocalAuthToken);
    const dbB = await createSyncedDb("sync-b", sharedLocalAuthToken);

    const title = `sync-a-to-b-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    await withTimeout(
      dbA.insertDurable(todos, { title, done: false }, { tier: "worker" }),
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
    const sharedLocalAuthToken = `sync-token-b-to-a-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const dbA = await createSyncedDb("sync-a-reverse", sharedLocalAuthToken);
    const dbB = await createSyncedDb("sync-b-reverse", sharedLocalAuthToken);

    const title = `sync-b-to-a-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    await withTimeout(
      dbB.insertDurable(todos, { title, done: true }, { tier: "worker" }),
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

  it("local-only subscriptions receive rows from opfs", async () => {
    const dbName = uniqueDbName("sync-local-only");
    const dbA = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );

    const snapshots: Todo[][] = [];
    const unsub = trackSubscription(
      dbA.subscribeAll(
        allTodos,
        (delta) => {
          snapshots.push([...delta.all]);
        },
        { propagation: "local-only" },
      ),
    );

    await dbA.insertDurable(todos, { title: "local-only-local-1", done: true }, { tier: "worker" });

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
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );

    await waitForCondition(
      async () => {
        const rows = await dbB.all(allTodos, { propagation: "local-only" });
        return rows.some((row) => row.title === "local-only-local-1");
      },
      8000,
      "local-only query should retrieve persisted OPFS rows after reopen",
    );

    const snapshotsB = await dbB.all(allTodos, { propagation: "local-only" });
    expect(snapshotsB.length).toBe(1);
    expect(snapshotsB[0].title).toBe("local-only-local-1");
  }, 60000);

  it("local-only subscriptions do not receive rows from sync server", async () => {
    const sharedLocalAuthToken = `sync-local-only-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const dbA = await createSyncedDb("sync-local-only-a", sharedLocalAuthToken);
    const dbB = await createSyncedDb("sync-local-only-b", sharedLocalAuthToken);

    const snapshots: Todo[][] = [];
    const unsub = trackSubscription(
      dbB.subscribeAll(
        allTodos,
        (delta) => {
          snapshots.push([...delta.all]);
        },
        { propagation: "local-only" },
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
      dbA.insertDurable(todos, { title: remoteTitle, done: false }, { tier: "worker" }),
      10000,
      "A insert(worker) did not resolve",
    );

    // Give sync enough time; local-only must still not see remote data.
    await sleep(3000);
    const latestAfterRemote = snapshots[snapshots.length - 1] ?? [];
    expect(latestAfterRemote.some((row) => row.title === remoteTitle)).toBe(false);

    const localTitle = `local-only-local-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    await dbB.insert(todos, { title: localTitle, done: true });

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

  // -------------------------------------------------------------------------
  // 8. Leader election + cross-tab peer routing
  // -------------------------------------------------------------------------

  it("routes follower writes through the elected leader", async () => {
    const dbName = uniqueDbName("leader-route");
    const dbA = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );
    const dbB = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );
    const { leader, follower } = await waitForLeaderAndFollower(dbA, dbB);

    const receivedByLeader: string[] = [];
    const unsubscribe = leader.subscribeAll(
      allTodos as QueryBuilder<Todo & { id: string }>,
      (delta) => {
        for (const todo of delta.all) {
          receivedByLeader.push(todo.title);
        }
      },
    );

    await follower.insert(todos, { title: "Routed via leader", done: false });

    await waitForCondition(
      async () => receivedByLeader.includes("Routed via leader"),
      8000,
      "Leader should receive follower write through peer routing",
    );

    await waitForCondition(
      async () => {
        const leaderRows = await leader.all(allTodos, { tier: "worker" });
        const followerRows = await follower.all(allTodos, { tier: "worker" });
        const leaderHas = leaderRows.some((row) => row.title === "Routed via leader");
        const followerHas = followerRows.some((row) => row.title === "Routed via leader");
        return leaderHas && followerHas;
      },
      8000,
      "Both leader and follower should observe routed write",
    );

    unsubscribe();
  });

  it("fails over to follower after leader shutdown", async () => {
    const dbName = uniqueDbName("leader-failover");
    const dbA = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );
    const dbB = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );
    const { leader, follower } = await waitForLeaderAndFollower(dbA, dbB);

    await leader.shutdown();
    untrack(leader);

    await waitForCondition(
      async () => getTabRole(follower) === "leader",
      12000,
      "Follower should be promoted to leader after shutdown",
    );

    const { id } = await follower.insert(todos, { title: "Post-failover", done: true });
    await waitForCondition(
      async () => {
        const rows = await follower.all(allTodos, { tier: "worker" });
        return rows.some((row) => row.id === id && row.title === "Post-failover");
      },
      8000,
      "New leader should continue processing writes after failover",
    );
  });

  it("re-elects cleanly when a closed leader tab is reopened", async () => {
    const dbName = uniqueDbName("leader-reopen");
    const dbA = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );
    const dbB = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );
    const { leader: initialLeader, follower: survivor } = await waitForLeaderAndFollower(dbA, dbB);

    await initialLeader.shutdown();
    untrack(initialLeader);

    await waitForCondition(
      async () => getTabRole(survivor) === "leader",
      12000,
      "Surviving tab should become leader after leader closes",
    );

    const reopened = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );
    const currentLeader = await waitForSingleLeader([survivor, reopened]);
    const currentFollower = currentLeader === survivor ? reopened : survivor;
    await currentLeader.all(allTodos, { tier: "worker" });

    const marker = `reopen-${Date.now()}`;
    await withTimeout(
      currentFollower.insertDurable(todos, { title: marker, done: false }, { tier: "worker" }),
      10000,
      "Follower insert during reopen re-election did not resolve",
    );

    await waitForCondition(
      async () => {
        const leaderRows = await currentLeader.all(allTodos, { tier: "worker" });
        const followerRows = await currentFollower.all(allTodos, { tier: "worker" });
        const leaderHas = leaderRows.some((row) => row.title === marker);
        const followerHas = followerRows.some((row) => row.title === marker);
        return leaderHas && followerHas;
      },
      8000,
      "Reopened tab and current leader should converge after re-election",
    );
  });
});

// ---------------------------------------------------------------------------
// Polling helper
// ---------------------------------------------------------------------------

async function waitForCondition(
  check: () => Promise<boolean>,
  timeoutMs: number,
  message: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown = undefined;
  while (Date.now() < deadline) {
    try {
      if (await check()) return;
    } catch (error) {
      lastError = error;
    }
    await sleep(50);
  }

  const lastErrorMessage =
    lastError instanceof Error ? lastError.message : lastError ? String(lastError) : "none";
  throw new Error(`Timeout after ${timeoutMs}ms: ${message}; lastError=${lastErrorMessage}`);
}

async function waitForWorkerMessageType(
  worker: Worker,
  expectedType: string,
  timeoutMs: number,
  label: string,
): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error(`${label}: no ${expectedType} worker message within ${timeoutMs}ms`));
    }, timeoutMs);

    const handler = (event: MessageEvent) => {
      const data = event.data as { type?: string } | undefined;
      if (data?.type === expectedType) {
        cleanup();
        resolve();
      }
    };

    const cleanup = () => {
      clearTimeout(timeout);
      worker.removeEventListener("message", handler);
    };

    worker.addEventListener("message", handler);
  });
}

async function waitForTodos(
  db: Db,
  predicate: (rows: Todo[]) => boolean,
  label: string,
  timeoutMs = 15000,
  tier: "worker" | "edge" | undefined = undefined,
): Promise<Todo[]> {
  const deadline = Date.now() + timeoutMs;
  let lastRows: Todo[] = [];
  let lastError: unknown = undefined;

  while (Date.now() < deadline) {
    try {
      const rows = await db.all(allTodos, { tier });
      if (predicate(rows)) {
        return rows;
      }
      lastRows = rows;
    } catch (error) {
      lastError = error;
    }

    await sleep(150);
  }

  const rowPreview = JSON.stringify(
    lastRows.slice(0, 10).map((row) => ({ id: row.id, title: row.title, done: row.done })),
  );
  const lastErrorMessage =
    lastError instanceof Error ? lastError.message : lastError ? String(lastError) : "none";
  throw new Error(
    `${label}: timed out after ${timeoutMs}ms (tier=${tier ?? "default"}); ` +
      `lastRowsCount=${lastRows.length}; lastRows=${rowPreview}; lastError=${lastErrorMessage}`,
  );
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  let timeoutId: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_, reject) => {
        timeoutId = setTimeout(() => {
          reject(new Error(`${label} after ${timeoutMs}ms`));
        }, timeoutMs);
      }),
    ]);
  } finally {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function hasRestoredCatalogueState(state: DebugSchemaState): boolean {
  return state.liveSchemaHashes.length > 1 && state.lensEdges.length > 0;
}

function getMainDebugSchemaState(db: Db, schemaForClient: WasmSchema): DebugSchemaState {
  const client = (db as any).getClient(schemaForClient);
  const runtime = client.getRuntime() as { __debugSchemaState?: () => DebugSchemaState };
  if (typeof runtime.__debugSchemaState !== "function") {
    throw new Error("Expected runtime.__debugSchemaState to be available");
  }
  return runtime.__debugSchemaState();
}

async function getWorkerDebugSchemaState(db: Db, timeoutMs = 5000): Promise<DebugSchemaState> {
  await (db as any).ensureBridgeReady();
  const worker = (db as any).worker as Worker | null;
  if (!worker) {
    throw new Error("Expected worker instance to exist");
  }

  return new Promise<DebugSchemaState>((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error(`debug-schema-state: no response within ${timeoutMs}ms`));
    }, timeoutMs);

    const handler = (event: MessageEvent) => {
      const data = event.data as
        | { type?: string; state?: DebugSchemaState; message?: string }
        | undefined;
      if (!data?.type) return;

      if (data.type === "debug-schema-state-ok" && data.state) {
        cleanup();
        resolve(data.state);
        return;
      }

      if (
        data.type === "error" &&
        typeof data.message === "string" &&
        data.message.includes("debug-schema-state")
      ) {
        cleanup();
        reject(new Error(data.message));
      }
    };

    const cleanup = () => {
      clearTimeout(timeout);
      worker.removeEventListener("message", handler);
    };

    worker.addEventListener("message", handler);
    worker.postMessage({ type: "debug-schema-state" });
  });
}

async function seedWorkerLiveSchema(db: Db, schema: WasmSchema, timeoutMs = 5000): Promise<void> {
  await (db as any).ensureBridgeReady();
  const worker = (db as any).worker as Worker | null;
  if (!worker) {
    throw new Error("Expected worker instance to exist");
  }

  const schemaJson = JSON.stringify(schema);

  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error(`debug-seed-live-schema: no response within ${timeoutMs}ms`));
    }, timeoutMs);

    const handler = (event: MessageEvent) => {
      const data = event.data as { type?: string; message?: string } | undefined;
      if (!data?.type) return;

      if (data.type === "debug-seed-live-schema-ok") {
        cleanup();
        resolve();
        return;
      }

      if (
        data.type === "error" &&
        typeof data.message === "string" &&
        data.message.includes("debug-seed-live-schema")
      ) {
        cleanup();
        reject(new Error(data.message));
      }
    };

    const cleanup = () => {
      clearTimeout(timeout);
      worker.removeEventListener("message", handler);
    };

    worker.addEventListener("message", handler);
    worker.postMessage({ type: "debug-seed-live-schema", schemaJson });
  });
}
