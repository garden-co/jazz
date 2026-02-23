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

// ---------------------------------------------------------------------------
// Test schema — a simple "todos" table
// ---------------------------------------------------------------------------

const schema: WasmSchema = {
  tables: {
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

const todos: TableProxy<Todo, TodoInit> = {
  _table: "todos",
  _schema: schema,
  _rowType: {} as Todo,
  _initType: {} as TodoInit,
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
        dbName: uniqueDbName(label),
        serverUrl,
        localAuthMode: "anonymous",
        localAuthToken,
        adminSecret: ADMIN_SECRET,
      }),
    );
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
    const db = track(await createDb({ appId: "test-app", dbName: uniqueDbName("init") }));
    expect(db).toBeDefined();
    expect(db).toBeInstanceOf(Db);
  });

  // -------------------------------------------------------------------------
  // 2. Insert + local query through worker bridge
  // -------------------------------------------------------------------------

  it("inserts a row and queries it back", async () => {
    const db = track(await createDb({ appId: "test-app", dbName: uniqueDbName("insert-query") }));

    // Insert (sync — runs on main-thread in-memory runtime)
    const id = db.insert(todos, { title: "Buy milk", done: false });
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
    const db = track(await createDb({ appId: "test-app", dbName: uniqueDbName("multi-insert") }));

    db.insert(todos, { title: "Task A", done: false });
    db.insert(todos, { title: "Task B", done: true });
    db.insert(todos, { title: "Task C", done: false });

    const results = await db.all(allTodos);
    expect(results.length).toBe(3);

    const titles = results.map((r) => r.title).sort();
    expect(titles).toEqual(["Task A", "Task B", "Task C"]);
  });

  // -------------------------------------------------------------------------
  // 3. Update + delete through worker bridge
  // -------------------------------------------------------------------------

  it("updates a row", async () => {
    const db = track(await createDb({ appId: "test-app", dbName: uniqueDbName("update") }));

    const id = db.insert(todos, { title: "Original", done: false });
    db.update(todos, id, { done: true });

    const results = await db.all(allTodos);
    expect(results.length).toBe(1);
    expect(results[0].title).toBe("Original");
    expect(results[0].done).toBe(true);
  });

  it("deletes a row", async () => {
    const db = track(await createDb({ appId: "test-app", dbName: uniqueDbName("delete") }));

    const id = db.insert(todos, { title: "Ephemeral", done: false });
    expect((await db.all(allTodos)).length).toBe(1);

    db.deleteFrom(todos, id);
    const results = await db.all(allTodos);
    expect(results.length).toBe(0);
  });

  // -------------------------------------------------------------------------
  // 4. OPFS persistence across shutdown + re-open
  // -------------------------------------------------------------------------

  it("persists data across shutdown and re-create (OPFS)", async () => {
    const dbName = uniqueDbName("persistence");

    const db1 = await createDb({ appId: "test-app", dbName });
    db1.insert(todos, { title: "Survive reload", done: true });
    const before = await db1.all(allTodos);
    expect(before.length).toBe(1);
    await db1.shutdown();

    // New Db with same dbName — worker reopens OPFS, main thread starts empty.
    // Using "worker" settled tier makes the query wait for the worker's
    // QuerySettled response, ensuring OPFS data arrives before resolving.
    const db2 = track(await createDb({ appId: "test-app", dbName }));
    const after = await db2.all(allTodos, "worker");
    expect(after.length).toBe(1);
    expect(after[0].title).toBe("Survive reload");
    expect(after[0].done).toBe(true);
  });

  it("recovers data from WAL after crash (no snapshot flush)", async () => {
    const dbName = uniqueDbName("crash-recovery");

    const db1 = track(await createDb({ appId: "test-app", dbName }));

    // insertWithAck ensures data is in OPFS WAL before we crash
    await db1.insertWithAck(todos, { title: "Crash-proof", done: false }, "worker");
    await db1.insertWithAck(todos, { title: "Also survives", done: true }, "worker");

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

    // New Db with same dbName — worker must recover from OPFS WAL
    const db2 = track(await createDb({ appId: "test-app", dbName }));
    const after = await db2.all(allTodos, "worker");
    expect(after.length).toBe(2);

    const titles = after.map((r) => r.title).sort();
    expect(titles).toEqual(["Also survives", "Crash-proof"]);
  });

  // -------------------------------------------------------------------------
  // 5. Acknowledged insert resolves at worker tier
  // -------------------------------------------------------------------------

  it("insertWithAck resolves when worker acks", async () => {
    const db = track(await createDb({ appId: "test-app", dbName: uniqueDbName("with-ack") }));

    // insertWithAck("worker") should resolve once the worker's OPFS has it
    const id = await db.insertWithAck(todos, { title: "Durable", done: false }, "worker");
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
    const db = track(await createDb({ appId: "test-app", dbName: uniqueDbName("subscribe") }));

    const received: Todo[][] = [];

    const unsub = trackSubscription(
      db.subscribeAll(allTodos, (delta) => {
        received.push([...delta.all]);
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
    const db = track(await createDb({ appId: "test-app", dbName: uniqueDbName("subscribe") }));

    const received: Todo[][] = [];

    const projectId = "00000000-0000-0000-0000-000000000123";
    const unsub = trackSubscription(
      db.subscribeAll(todosByProject(projectId), (delta) => {
        received.push([...delta.all]);
      }),
    );

    db.insert(todos, { title: "Observed", done: false, project: projectId });
    const anotherProjectId = "00000000-0000-0000-0000-000000000456";
    db.insert(todos, { title: "Not observed", done: false, project: anotherProjectId });

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

  // -------------------------------------------------------------------------
  // 7. Server sync through worker
  // -------------------------------------------------------------------------

  it("propagates synced row from client A to client B", async () => {
    const sharedLocalAuthToken = `sync-token-a-to-b-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const dbA = await createSyncedDb("sync-a", sharedLocalAuthToken);
    const dbB = await createSyncedDb("sync-b", sharedLocalAuthToken);

    const title = `sync-a-to-b-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    await withTimeout(
      dbA.insertWithAck(todos, { title, done: false }, "worker"),
      10000,
      "A insertWithAck(worker) did not resolve",
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
      dbB.insertWithAck(todos, { title, done: true }, "worker"),
      10000,
      "B insertWithAck(worker) did not resolve",
    );

    const rowsOnA = await waitForTodos(
      dbA,
      (rows) => rows.some((row) => row.title === title),
      "B -> A propagation",
      20000,
    );
    expect(rowsOnA.some((row) => row.title === title)).toBe(true);
  }, 60000);
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
  settledTier: "worker" | "edge" | undefined = undefined,
): Promise<Todo[]> {
  const deadline = Date.now() + timeoutMs;
  let lastRows: Todo[] = [];
  let lastError: unknown = undefined;

  while (Date.now() < deadline) {
    try {
      const rows = await db.all(allTodos, settledTier);
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
    `${label}: timed out after ${timeoutMs}ms (tier=${settledTier ?? "default"}); ` +
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
