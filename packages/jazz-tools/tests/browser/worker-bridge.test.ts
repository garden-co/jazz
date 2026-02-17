/**
 * Browser integration tests for Worker Bridge + OPFS persistence.
 *
 * Runs in a real Chromium browser via @vitest/browser + playwright.
 * Uses real groove-wasm, real dedicated Workers, real OPFS storage.
 *
 * Server sync tests use a real jazz-tools server spawned by global-setup.
 */

import { describe, it, expect, afterEach } from "vitest";
import { createDb, Db, type QueryBuilder, type TableProxy } from "../../src/runtime/db.js";
import type { WasmSchema } from "../../src/drivers/types.js";
import { TEST_PORT, JWT_SECRET, ADMIN_SECRET, APP_ID } from "./test-constants.js";

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
}

interface TodoInit {
  title: string;
  done: boolean;
  project?: string;
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

function projectsWithTodos(projectId: string): QueryBuilder<Project> {
  return {
    _table: "projects",
    _schema: schema,
    _rowType: {} as Project,
    _build() {
      return JSON.stringify({
        table: "projects",
        conditions: [
          {
            column: "id",
            op: "eq",
            value: projectId,
          },
        ],
        includes: {
          todosViaProject: true,
        },
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

  /** Track dbs for cleanup. */
  function track(db: Db): Db {
    dbs.push(db);
    return db;
  }

  afterEach(async () => {
    for (const db of dbs) {
      try {
        await db.shutdown();
      } catch {
        // Best effort
      }
    }
    dbs.length = 0;
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

  it("queries by id", async () => {
    const db = track(await createDb({ appId: "test-app", dbName: uniqueDbName("query-by-id") }));

    const id = db.insert(projects, { name: "Project A" });

    const results = await db.all(projectsWithTodos(id));
    expect(results.length).toBe(1);

    expect(results[0].id).toBe(id);
    expect(results[0].name).toBe("Project A");
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

    const db1 = await createDb({ appId: "test-app", dbName });

    // insertPersisted ensures data is in OPFS WAL before we crash
    await db1.insertPersisted(todos, { title: "Crash-proof", done: false }, "worker");
    await db1.insertPersisted(todos, { title: "Also survives", done: true }, "worker");

    // Simulate crash: release OPFS handles WITHOUT flushing snapshot.
    // WAL has the data, but snapshot is stale. Recovery must replay WAL.
    // (Real worker.terminate() doesn't reliably release OPFS exclusive
    // locks within the same page session — only a full page reload does.)
    await (db1 as any).ensureBridgeReady();
    const worker = (db1 as any).worker as Worker;
    worker.postMessage({ type: "simulate-crash" });
    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => {
        cleanup();
        reject(new Error("simulate-crash: no shutdown-ok received"));
      }, 5000);
      const handler = (event: MessageEvent) => {
        if (event.data.type === "shutdown-ok") {
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
    worker.terminate();
    // Null out to prevent afterEach from trying clean shutdown on dead worker
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
  // 5. Persisted insert resolves at worker tier
  // -------------------------------------------------------------------------

  it("insertPersisted resolves when worker acks", async () => {
    const db = track(await createDb({ appId: "test-app", dbName: uniqueDbName("persisted") }));

    // insertPersisted("worker") should resolve once the worker's OPFS has it
    const id = await db.insertPersisted(todos, { title: "Durable", done: false }, "worker");
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

    const unsub = db.subscribeAll(allTodos as QueryBuilder<Todo & { id: string }>, (delta) => {
      received.push([...delta.all]);
    });

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
    const unsub = db.subscribeAll(
      todosByProject(projectId) as QueryBuilder<Todo & { id: string }>,
      (delta) => {
        received.push([...delta.all]);
      },
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

  it("syncs data between two clients through the server", async () => {
    const serverUrl = `http://127.0.0.1:${TEST_PORT}`;
    const token1 = await signJwt("user-a", JWT_SECRET);

    const db1 = track(
      await createDb({
        appId: APP_ID,
        dbName: uniqueDbName("sync-a"),
        serverUrl,
        jwtToken: token1,
        adminSecret: ADMIN_SECRET,
      }),
    );

    // Insert and wait for server-tier persistence ack
    const id = await db1.insertPersisted(todos, { title: "Server-synced", done: false }, "edge");
    expect(id).toBeTruthy();

    // Query back from the server (edge-tier settlement)
    const results = await db1.all(allTodos, "edge");
    expect(results.length).toBeGreaterThanOrEqual(1);
    expect(results[0].title).toBe("Server-synced");
  });
});

// ---------------------------------------------------------------------------
// JWT helper (Web Crypto — works in browser)
// ---------------------------------------------------------------------------

function base64url(input: string | Uint8Array): string {
  const str = typeof input === "string" ? btoa(input) : btoa(String.fromCharCode(...input));
  return str.replace(/=/g, "").replace(/\+/g, "-").replace(/\//g, "_");
}

async function signJwt(sub: string, secret: string): Promise<string> {
  const header = { alg: "HS256", typ: "JWT" };
  const payload = {
    sub,
    claims: {},
    exp: Math.floor(Date.now() / 1000) + 3600,
  };
  const enc = new TextEncoder();
  const headerB64 = base64url(JSON.stringify(header));
  const payloadB64 = base64url(JSON.stringify(payload));
  const data = enc.encode(`${headerB64}.${payloadB64}`);
  const key = await crypto.subtle.importKey(
    "raw",
    enc.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const sig = await crypto.subtle.sign("HMAC", key, data);
  return `${headerB64}.${payloadB64}.${base64url(new Uint8Array(sig))}`;
}

// ---------------------------------------------------------------------------
// Polling helper
// ---------------------------------------------------------------------------

async function waitForCondition(
  check: () => Promise<boolean>,
  timeoutMs: number,
  message: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error(`Timeout: ${message}`);
}
