/**
 * Browser integration tests for the SharedWorker + IndexedDB runtime.
 *
 * Runs in a real Chromium browser via @vitest/browser + playwright.
 * Uses real jazz-wasm, a real SharedWorker, and real IndexedDB storage.
 *
 * Server sync tests use a real jazz-tools server spawned by global-setup.
 */

import { describe, it, expect, afterEach, vi } from "vitest";
import { createDb, Db, type QueryBuilder } from "../../src/runtime/db.js";
import type { Schema } from "../../src/drivers/types.js";
import { generateAuthSecret } from "../../src/runtime/auth-secret-store.js";
import {
  TestCleanup,
  createSyncedDb,
  sleep,
  uniqueDbName,
  waitForCondition,
  waitForQuery,
  withTimeout,
} from "./support.js";
import {
  blockJazzServerNetwork,
  getJazzServerInfo,
  getJazzServerJwtForUser,
  type JazzServerInfo,
  unblockJazzServerNetwork,
} from "./testing-server.js";
import {
  closeRemoteBrowserDb,
  createRemoteBrowserDb,
  deleteRemoteBrowserIndexedDbAndWaitForReload,
  insertRemoteBrowserDbRow,
  queryRemoteBrowserDbRows,
  updateRemoteBrowserDbRow,
  restartRemoteBrowserDb,
  waitForRemoteBrowserDbTitle,
} from "./remote-browser-db.js";
import { CompiledPermissions, schema as s } from "../../src/";
import { deploy } from "../../src/dev/catalogue.js";
import type {
  BrowserInspectorContext,
  BrowserInspectorControlEvent,
  BrowserInspectorControlRequest,
} from "../../src/runtime/native-runtime/browser-worker-protocol.js";

declare const __JAZZ_BROWSER_SOAK__: string;

let nextInspectorRequestId = 1;

async function listWorkerContexts(port: MessagePort): Promise<BrowserInspectorContext[]> {
  const id = nextInspectorRequestId++;
  return new Promise((resolve) => {
    const onMessage = (event: MessageEvent<BrowserInspectorControlEvent>) => {
      if (event.data.type !== "contexts" || event.data.id !== id) return;
      port.removeEventListener("message", onMessage);
      resolve(event.data.contexts);
    };
    port.addEventListener("message", onMessage);
    port.postMessage({ type: "list-contexts", id } satisfies BrowserInspectorControlRequest);
  });
}

async function waitForWorkerContextRelease(port: MessagePort, dbName: string): Promise<void> {
  await waitForCondition(
    async () => !(await listWorkerContexts(port)).some((context) => context.dbName === dbName),
    5000,
    `SharedWorker context ${dbName} should be destroyed before restart`,
  );
}

async function terminateWorker(port: MessagePort): Promise<void> {
  const id = nextInspectorRequestId++;
  await new Promise<void>((resolve, reject) => {
    const onMessage = (event: MessageEvent<BrowserInspectorControlEvent>) => {
      if (event.data.type !== "result" || event.data.id !== id) return;
      port.removeEventListener("message", onMessage);
      if (event.data.error) reject(new Error(event.data.error));
      else resolve();
    };
    port.addEventListener("message", onMessage);
    port.postMessage({ type: "terminate-worker", id } satisfies BrowserInspectorControlRequest);
  });
}

// ---------------------------------------------------------------------------
// Test schema — a simple "todos" table
// ---------------------------------------------------------------------------

const schema = {
  projects: s.table({
    name: s.string(),
  }),
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
    projectId: s.ref("projects").optional(),
    tags: s.array(s.string()).optional(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
const app: s.App<AppSchema> = s.defineApp(schema);
const { projects, todos } = app;
type Todo = s.RowOf<typeof todos>;

const readOnlyPermissions = s.definePermissions(app, ({ policy }) => [
  policy.projects.allowRead.always(),
  policy.projects.allowInsert.never(),
  policy.projects.allowUpdate.never(),
  policy.projects.allowDelete.never(),
  policy.todos.allowRead.always(),
  policy.todos.allowInsert.never(),
  policy.todos.allowUpdate.never(),
  policy.todos.allowDelete.never(),
]);

const noUpdatePermissions = s.definePermissions(app, ({ policy }) => [
  policy.projects.allowRead.always(),
  policy.projects.allowInsert.always(),
  policy.projects.allowUpdate.never(),
  policy.projects.allowDelete.always(),
  policy.todos.allowRead.always(),
  policy.todos.allowInsert.always(),
  policy.todos.allowUpdate.never(),
  policy.todos.allowDelete.always(),
]);

const noDeletePermissions = s.definePermissions(app, ({ policy }) => [
  policy.projects.allowRead.always(),
  policy.projects.allowInsert.always(),
  policy.projects.allowUpdate.always(),
  policy.projects.allowDelete.never(),
  policy.todos.allowRead.always(),
  policy.todos.allowInsert.always(),
  policy.todos.allowUpdate.always(),
  policy.todos.allowDelete.never(),
]);

const nullableSchema = {
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
    description: s.string().optional(),
  }),
};

type NullableSchema = s.Schema<typeof nullableSchema>;
const nullableApp: s.App<NullableSchema> = s.defineApp(nullableSchema);
const nullablePermissions = s.definePermissions(nullableApp, ({ policy }) => [
  policy.todos.allowRead.always(),
  policy.todos.allowInsert.always(),
  policy.todos.allowUpdate.always(),
  policy.todos.allowDelete.always(),
]);

/** QueryBuilder that selects all todos. */
const allTodos: QueryBuilder<Todo> = app.todos;

// A small published schema family used to prove that the persistent worker
// rehydrates catalogue state, including its migration lens, before a current
// client issues its first query after reopening.
const catalogueSchemaV1 = {
  todos: s.table({
    title: s.string(),
    completed: s.boolean(),
  }),
};

const catalogueSchemaV2 = {
  todos: s.table({
    title: s.string(),
    completed: s.boolean(),
    description: s.string().optional(),
  }),
};

const catalogueAppV1 = s.defineApp(catalogueSchemaV1);
const catalogueAppV2 = s.defineApp(catalogueSchemaV2);
const { todos: catalogueTodos } = catalogueAppV2;
type CatalogueTodo = s.RowOf<typeof catalogueTodos>;
const allCatalogueTodos: QueryBuilder<CatalogueTodo> = catalogueAppV2.todos;

const cataloguePermissionsV1 = s.definePermissions(catalogueAppV1, ({ policy }) => [
  policy.todos.allowRead.always(),
  policy.todos.allowInsert.always(),
  policy.todos.allowUpdate.always(),
  policy.todos.allowDelete.always(),
]);

const cataloguePermissionsV2 = s.definePermissions(catalogueAppV2, ({ policy }) => [
  policy.todos.allowRead.always(),
  policy.todos.allowInsert.always(),
  policy.todos.allowUpdate.always(),
  policy.todos.allowDelete.always(),
]);

/**
 * Structurally valid JWT with a deliberately invalid signature: parses fine on
 * the client (sub/exp claims) but the testing server rejects it at handshake.
 */
function makeStructurallyValidJwt(userId: string): string {
  const encode = (value: unknown) =>
    btoa(JSON.stringify(value)).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
  const header = encode({ alg: "HS256", typ: "JWT" });
  const payload = encode({
    // Match TestJwtIssuer's ordinary external identity so this remains a
    // same-principal refresh after the server rejects the signature.
    iss: "urn:jazz:test",
    sub: userId,
    exp: Math.floor(Date.now() / 1000) + 3600,
  });
  return `${header}.${payload}.invalid-signature`;
}

/** QueryBuilder that selects all todos by project. */
function todosByProject(projectId: string): QueryBuilder<Todo> {
  return app.todos.where({ projectId });
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("SharedWorker bridge with IndexedDB", () => {
  const ctx = new TestCleanup();
  const remoteBrowserDbIds = new Set<string>();
  const errorListeners = new Set<(event: ErrorEvent) => void>();

  function trackRemoteBrowserDb(id: string): string {
    remoteBrowserDbIds.add(id);
    return id;
  }

  async function waitForRemoteTodoTitle(
    id: string,
    title: string,
    label: string,
    timeoutMs: number,
    tier?: "local" | "edge",
  ): Promise<Record<string, unknown>[]> {
    try {
      return await waitForRemoteBrowserDbTitle({ id, title, timeoutMs, tier });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      throw new Error(`${label}: ${message}`);
    }
  }

  /** Shorthand: track a Db for cleanup. */
  function track(db: Db): Db {
    return ctx.track(db);
  }

  /** Shorthand: track a subscription for cleanup. */
  function trackSubscription(unsubscribe: () => void): () => void {
    return ctx.trackSubscription(unsubscribe);
  }

  function untrack(db: Db): void {
    ctx.untrack(db);
  }

  afterEach(async () => {
    for (const listener of errorListeners) {
      globalThis.removeEventListener("error", listener);
    }
    errorListeners.clear();
    for (const id of remoteBrowserDbIds) {
      try {
        await closeRemoteBrowserDb(id);
      } catch {
        // Best effort
      }
    }
    remoteBrowserDbIds.clear();
    await ctx.cleanup();
  });

  // -------------------------------------------------------------------------
  // 1. Worker initialization
  // -------------------------------------------------------------------------

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

    await deleteRemoteBrowserIndexedDbAndWaitForReload(remoteDbId, dbName);
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

    const seeded = track(
      await createDb({
        appId: testingServer.appId,
        serverUrl: testingServer.serverUrl,
        adminSecret: testingServer.adminSecret,
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
      .wait({ tier: "edge" });

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
        adminSecret: testingServer.adminSecret,
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
        adminSecret: testingServer.adminSecret,
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
      .wait({ tier: "edge" });

    const authoritativeRows = await waitForCatalogueTodos(
      reopened,
      (rows) => rows.some((row) => row.title === remoteMarker && row.completed),
      "reopened worker should receive authoritative current-schema rows from the server",
      15_000,
      "edge",
    );
    expect(authoritativeRows.find((row) => row.title === remoteMarker)?.description).toContain(
      "independent",
    );

    await sleep(100);
    expect(protocolErrors).toEqual([]);
    globalThis.removeEventListener("error", recordProtocolError);
    errorListeners.delete(recordProtocolError);
  }, 60_000);

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
        received.push([...rows]);
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
        received.push([...rows]);
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
          snapshots.push([...rows]);
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
        received.push([...rows]);
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
    const { appId, serverUrl, adminSecret } =
      await publishSyncServerSchemaAndPermissions("subscribe-initial-jwt");
    const db = track(
      await createDb({
        appId,
        driver: {
          type: "persistent",
          dbName: uniqueDbName("subscribe-initial-jwt"),
        },
        serverUrl,
        adminSecret,
        jwtToken: await getJazzServerJwtForUser("subscribe-initial-jwt", undefined, appId),
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
        received.push([...rows]);
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

  it("resolves insert wait at edge tier through the worker bridge", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions("sync-wait-edge");
    const sharedLocalAuthToken = generateAuthSecret();
    const db = await createSyncedDb(ctx, "sync-wait-edge", sharedLocalAuthToken, syncServer);

    const title = `wait-edge-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const inserted = db.insert(todos, { title, done: false });
    const { value: insertedTodo } = inserted;

    await withTimeout(inserted.wait({ tier: "edge" }), 10000, "insert wait(edge) did not resolve");

    expect(insertedTodo.id).toBeTruthy();
    expect(insertedTodo.title).toBe(title);

    const rowsAtEdge = await waitForTodos(
      db,
      (rows) => rows.some((row) => row.id === insertedTodo.id && row.title === title),
      "insert wait(edge) row becomes queryable at edge",
      20000,
      "edge",
    );
    expect(rowsAtEdge.some((row) => row.id === insertedTodo.id)).toBe(true);
  }, 60000);

  it("preserves admin write authority through the SharedWorker relay", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions(
      "sync-admin-write-authority",
      readOnlyPermissions,
    );
    const db = track(
      await createDb({
        appId: syncServer.appId,
        serverUrl: syncServer.serverUrl,
        adminSecret: syncServer.adminSecret,
        driver: {
          type: "persistent",
          dbName: uniqueDbName("sync-admin-write-authority"),
        },
        schema: app,
      }),
    );

    const inserted = db.insert(todos, {
      title: `admin-write-${Date.now()}`,
      done: false,
    });
    await withTimeout(inserted.wait({ tier: "edge" }), 10_000, "admin insert was rejected");

    const updatedTitle = `admin-update-${Date.now()}`;
    await withTimeout(
      db.update(todos, inserted.value.id, { title: updatedTitle }).wait({ tier: "edge" }),
      10_000,
      "admin update was rejected",
    );

    const rows = await waitForTodos(
      db,
      (current) =>
        current.some((row) => row.id === inserted.value.id && row.title === updatedTitle),
      "admin update should be authoritative",
      15_000,
      "edge",
    );
    expect(rows.find((row) => row.id === inserted.value.id)?.title).toBe(updatedTitle);
  });

  it("server permissions check rejects client optimistic insert - wait notification", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions(
      "sync-wait-edge",
      readOnlyPermissions,
    );

    const sharedLocalAuthToken = generateAuthSecret();
    const db = await createSyncedDb(ctx, "sync-wait-edge", sharedLocalAuthToken, syncServer);

    const insertResult = db.insert(todos, { title: "Rejected", done: false });
    const batchId = await insertResult.transactionId;
    await expect(insertResult.wait({ tier: "edge" })).rejects.toMatchObject({
      name: "PersistedWriteRejectedError",
      transactionId: batchId,
      code: "permission_denied",
    });

    const todosAfterRevert = await db.all(allTodos, { tier: "local" });
    expect(todosAfterRevert.length).toBe(0);
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
    const batchId = await insertResult.transactionId;
    await waitForCondition(
      async () => mutationErrorSpy.mock.calls.length > 0,
      5000,
      "onMutationError handler should be called",
    );
    expect(mutationErrorSpy).toHaveBeenCalledWith({
      code: "permission_denied",
      reason: "Write rejected by server authorization",
      transaction: {
        transactionId: batchId,
        kind: "mergeable",
        sealed: true,
        latestSettlement: {
          kind: "rejected",
          transactionId: batchId,
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
    await expect(insertResult.wait({ tier: "edge" })).rejects.toMatchObject({
      name: "PersistedWriteRejectedError",
      transactionId: insertResult.transactionId,
      code: "permission_denied",
    });
    expect(mutationErrorSpy).not.toHaveBeenCalled();
  });

  it("does not send a live rejection to a runtime attached after its originating peer closes", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions(
      "sync-on-mutation-error-restart",
    );

    const sharedLocalAuthToken = generateAuthSecret();
    const dbName = uniqueDbName("sync-on-mutation-error-restart");
    const createPersistentDb = (serverUrl?: string) =>
      createDb({
        appId: syncServer.appId,
        driver: { type: "persistent" as const, dbName },
        serverUrl,
        secret: sharedLocalAuthToken,
      });

    const dbBeforeRestart = track(await createPersistentDb(syncServer.serverUrl));
    const durableControl = dbBeforeRestart.insert(todos, {
      title: "Durable control across rejection restart",
      done: false,
    });
    await durableControl.wait({ tier: "edge" });
    await publishPermissionsForServer(syncServer, readOnlyPermissions);

    const mutationErrorSpy = vi.fn();
    dbBeforeRestart.onMutationError(mutationErrorSpy);

    const insertResult = dbBeforeRestart.insert(todos, {
      title: "Rejected across restart",
      done: false,
    });
    const batchId = await insertResult.transactionId;

    await waitForCondition(
      async () => mutationErrorSpy.mock.calls.length > 0,
      5000,
      "onMutationError handler should receive rejection before restart",
    );
    expect(mutationErrorSpy).toHaveBeenCalledWith({
      code: "permission_denied",
      reason: "Write rejected by server authorization",
      transaction: {
        transactionId: batchId,
        kind: "mergeable",
        sealed: true,
        latestSettlement: {
          kind: "rejected",
          transactionId: batchId,
          code: "permission_denied",
          reason: "Write rejected by server authorization",
        },
      },
    });

    const inspectorControl = await dbBeforeRestart.openInspectorControlPort();
    inspectorControl.start();
    const [initialContext] = (await listWorkerContexts(inspectorControl)).filter(
      (context) => context.dbName === dbName,
    );
    expect(initialContext).toBeDefined();
    try {
      await dbBeforeRestart.shutdown();
      untrack(dbBeforeRestart);
      await waitForWorkerContextRelease(inspectorControl, dbName);
      await terminateWorker(inspectorControl);

      const dbAfterAcknowledgement = track(await createPersistentDb(undefined));
      const replayAfterAckSpy = vi.fn();
      dbAfterAcknowledgement.onMutationError(replayAfterAckSpy);
      expect(await dbAfterAcknowledgement.all(allTodos, { tier: "local" })).toEqual([
        durableControl.value,
      ]);
      const secondInspectorControl = await dbAfterAcknowledgement.openInspectorControlPort();
      secondInspectorControl.start();
      const [secondContext] = (await listWorkerContexts(secondInspectorControl)).filter(
        (context) => context.dbName === dbName,
      );
      expect(secondContext?.workerRealmId).not.toBe(initialContext?.workerRealmId);
      // The destroyed worker context rehydrated the settled local view, but the
      // original tab's application notification is not a backlog for a later tab.
      await sleep(500);
      expect(replayAfterAckSpy).not.toHaveBeenCalled();

      await dbAfterAcknowledgement.shutdown();
      untrack(dbAfterAcknowledgement);
      await waitForWorkerContextRelease(secondInspectorControl, dbName);
      await terminateWorker(secondInspectorControl);

      const dbAfterSecondRestart = track(await createPersistentDb(undefined));
      expect(await dbAfterSecondRestart.all(allTodos, { tier: "local" })).toEqual([
        durableControl.value,
      ]);
      const thirdInspectorControl = await dbAfterSecondRestart.openInspectorControlPort();
      thirdInspectorControl.start();
      const [thirdContext] = (await listWorkerContexts(thirdInspectorControl)).filter(
        (context) => context.dbName === dbName,
      );
      expect(thirdContext?.workerRealmId).not.toBe(secondContext?.workerRealmId);
      thirdInspectorControl.postMessage({
        type: "close",
      } satisfies BrowserInspectorControlRequest);
    } finally {
      inspectorControl.postMessage({ type: "close" } satisfies BrowserInspectorControlRequest);
    }
  });

  it("rehydrates rejected worker batches without replaying an absent runtime's notification", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions(
      "sync-on-mutation-error-undelivered-restart",
      readOnlyPermissions,
    );

    const sharedLocalAuthToken = generateAuthSecret();
    const dbName = uniqueDbName("sync-on-mutation-error-undelivered-restart");
    const createPersistentDb = (serverUrl?: string) =>
      createDb({
        appId: syncServer.appId,
        driver: { type: "persistent" as const, dbName },
        serverUrl,
        secret: sharedLocalAuthToken,
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
    const [contextBeforeRestart] = (await listWorkerContexts(inspectorBeforeRestart)).filter(
      (context) => context.dbName === dbName,
    );
    await dbBeforeRestart.shutdown();
    untrack(dbBeforeRestart);
    await waitForWorkerContextRelease(inspectorBeforeRestart, dbName);
    await terminateWorker(inspectorBeforeRestart);

    const dbAfterRestart = track(await createPersistentDb(syncServer.serverUrl));
    const replayAfterRestartSpy = vi.fn();
    dbAfterRestart.onMutationError(replayAfterRestartSpy);

    // Run a query to set up the runtime
    await dbAfterRestart.all(allTodos, { tier: "edge" });
    const inspectorAfterRestart = await dbAfterRestart.openInspectorControlPort();
    inspectorAfterRestart.start();
    const [contextAfterRestart] = (await listWorkerContexts(inspectorAfterRestart)).filter(
      (context) => context.dbName === dbName,
    );
    expect(contextAfterRestart?.workerRealmId).not.toBe(contextBeforeRestart?.workerRealmId);

    await waitForCondition(
      async () => (await dbAfterRestart.all(allTodos, { tier: "local" })).length === 0,
      5000,
      "rejected transaction should not rehydrate into the restarted local view",
    );
    await sleep(500);
    expect(replayAfterRestartSpy).not.toHaveBeenCalled();

    await dbAfterRestart.shutdown();
    untrack(dbAfterRestart);
    await waitForWorkerContextRelease(inspectorAfterRestart, dbName);
    await terminateWorker(inspectorAfterRestart);

    const dbAfterSecondRestart = track(await createPersistentDb(undefined));
    expect(await dbAfterSecondRestart.all(allTodos, { tier: "local" })).toEqual([]);
    const inspectorAfterSecondRestart = await dbAfterSecondRestart.openInspectorControlPort();
    inspectorAfterSecondRestart.start();
    const [contextAfterSecondRestart] = (
      await listWorkerContexts(inspectorAfterSecondRestart)
    ).filter((context) => context.dbName === dbName);
    expect(contextAfterSecondRestart?.workerRealmId).not.toBe(contextAfterRestart?.workerRealmId);
    inspectorAfterSecondRestart.postMessage({
      type: "close",
    } satisfies BrowserInspectorControlRequest);
  });

  describe("optimistic writes are reverted on server rejection", () => {
    it("insert", async () => {
      const syncServer = await publishSyncServerSchemaAndPermissions(
        "sync-wait-edge",
        readOnlyPermissions,
      );

      const sharedLocalAuthToken = generateAuthSecret();
      const db = await createSyncedDb(ctx, "sync-wait-edge", sharedLocalAuthToken, syncServer);

      const insertResult = db.insert(todos, { title: "Rejected", done: false });
      await expect(insertResult.wait({ tier: "edge" })).rejects.toMatchObject({
        name: "PersistedWriteRejectedError",
        transactionId: insertResult.transactionId,
        code: "permission_denied",
      });

      const todosAfterRevert = await db.all(allTodos, { tier: "local" });
      expect(todosAfterRevert.length).toBe(0);
    });

    it("update", async () => {
      const syncServer = await publishSyncServerSchemaAndPermissions(
        "sync-wait-edge",
        noUpdatePermissions,
      );

      const sharedLocalAuthToken = generateAuthSecret();
      const db = await createSyncedDb(ctx, "sync-wait-edge", sharedLocalAuthToken, syncServer);

      const insertResult = db.insert(todos, {
        title: "Initial task",
        done: false,
      });
      const todo = await insertResult.wait({ tier: "edge" });

      const updateResult = db.update(todos, todo.id, { title: "Updated task" });
      await expect(updateResult.wait({ tier: "edge" })).rejects.toMatchObject({
        name: "PersistedWriteRejectedError",
        transactionId: updateResult.transactionId,
        code: "permission_denied",
      });

      const todosAfterRevert = await db.all(allTodos, { tier: "local" });
      expect(todosAfterRevert).toEqual([todo]);
    });

    it("delete", async () => {
      const syncServer = await publishSyncServerSchemaAndPermissions(
        "sync-wait-edge",
        noDeletePermissions,
      );

      const sharedLocalAuthToken = generateAuthSecret();
      const db = await createSyncedDb(ctx, "sync-wait-edge", sharedLocalAuthToken, syncServer);

      const insertResult = db.insert(todos, {
        title: "Initial task",
        done: false,
      });
      const todo = await insertResult.wait({ tier: "edge" });

      const deleteResult = db.delete(todos, todo.id);
      await expect(deleteResult.wait({ tier: "edge" })).rejects.toMatchObject({
        name: "PersistedWriteRejectedError",
        transactionId: deleteResult.transactionId,
        code: "permission_denied",
      });

      const todosAfterRevert = await db.all(allTodos, { tier: "local" });
      expect(todosAfterRevert).toEqual([todo]);
    });

    describe("also reverts after restart", () => {
      it("insert", async () => {
        const syncServer = await publishSyncServerSchemaAndPermissions(
          "sync-restart-revert-insert",
          readOnlyPermissions,
        );

        const sharedLocalAuthToken = generateAuthSecret();
        const dbName = uniqueDbName("sync-restart-revert-insert");
        const createPersistentDb = (serverUrl?: string) =>
          createDb({
            appId: syncServer.appId,
            driver: { type: "persistent" as const, dbName },
            serverUrl,
            secret: sharedLocalAuthToken,
          });

        const dbBeforeRestart = track(await createPersistentDb(undefined));
        const insertResult = dbBeforeRestart.insert(todos, {
          title: "Rejected after restart",
          done: false,
        });
        await insertResult.wait({ tier: "local" });

        const todosBeforeRestart = await dbBeforeRestart.all(allTodos, {
          tier: "local",
        });
        expect(todosBeforeRestart).toEqual([insertResult.value]);

        await dbBeforeRestart.shutdown();
        untrack(dbBeforeRestart);

        const dbAfterRestart = track(await createPersistentDb(syncServer.serverUrl));
        expect(await dbAfterRestart.all(allTodos, { tier: "edge" })).toEqual([]);
        await dbAfterRestart.shutdown();
        untrack(dbAfterRestart);

        // Reopen offline to prove the accepted server state crossed the public
        // runtime lifecycle boundary and was durably settled in the worker.
        const dbAfterSettlement = track(await createPersistentDb(undefined));
        expect(await dbAfterSettlement.all(allTodos, { tier: "local" })).toEqual([]);
      });

      it("update", async () => {
        const syncServer = await publishSyncServerSchemaAndPermissions(
          "sync-restart-revert-update",
        );

        const sharedLocalAuthToken = generateAuthSecret();
        const dbName = uniqueDbName("sync-restart-revert-update");
        const createPersistentDb = (serverUrl?: string) =>
          createDb({
            appId: syncServer.appId,
            driver: { type: "persistent" as const, dbName },
            serverUrl,
            secret: sharedLocalAuthToken,
          });

        const seeder = track(await createPersistentDb(syncServer.serverUrl));
        const insertResult = seeder.insert(todos, {
          title: "Initial task",
          done: false,
        });
        const todo = insertResult.value;
        await insertResult.wait({ tier: "edge" });
        await seeder.shutdown();
        untrack(seeder);

        await publishPermissionsForServer(syncServer, noUpdatePermissions);

        const dbBeforeRestart = track(await createPersistentDb(undefined));
        expect(await dbBeforeRestart.all(allTodos, { tier: "local" })).toEqual([todo]);

        const updateResult = dbBeforeRestart.update(todos, todo.id, {
          title: "Rejected update after restart",
        });
        await updateResult.wait({ tier: "local" });

        const todosBeforeRestart = await dbBeforeRestart.all(allTodos, {
          tier: "local",
        });
        expect(todosBeforeRestart).toEqual([{ ...todo, title: "Rejected update after restart" }]);

        await dbBeforeRestart.shutdown();
        untrack(dbBeforeRestart);

        const dbAfterRestart = track(await createPersistentDb(syncServer.serverUrl));
        expect(await dbAfterRestart.all(allTodos, { tier: "edge" })).toEqual([todo]);
        await dbAfterRestart.shutdown();
        untrack(dbAfterRestart);

        const dbAfterSettlement = track(await createPersistentDb(undefined));
        expect(await dbAfterSettlement.all(allTodos, { tier: "local" })).toEqual([todo]);
      });

      it("delete", async () => {
        const syncServer = await publishSyncServerSchemaAndPermissions(
          "sync-restart-revert-delete",
        );

        const sharedLocalAuthToken = generateAuthSecret();
        const dbName = uniqueDbName("sync-restart-revert-delete");
        const createPersistentDb = (serverUrl?: string) =>
          createDb({
            appId: syncServer.appId,
            driver: { type: "persistent" as const, dbName },
            serverUrl,
            secret: sharedLocalAuthToken,
          });

        const seeder = track(await createPersistentDb(syncServer.serverUrl));
        const insertResult = seeder.insert(todos, {
          title: "Initial task",
          done: false,
        });
        const todo = insertResult.value;
        await insertResult.wait({ tier: "edge" });
        await seeder.shutdown();
        untrack(seeder);

        await publishPermissionsForServer(syncServer, noDeletePermissions);

        const dbBeforeRestart = track(await createPersistentDb(undefined));
        expect(await dbBeforeRestart.all(allTodos, { tier: "local" })).toEqual([todo]);

        const deleteResult = dbBeforeRestart.delete(todos, todo.id);
        await deleteResult.wait({ tier: "local" });

        const todosBeforeRestart = await dbBeforeRestart.all(allTodos, {
          tier: "local",
        });
        expect(todosBeforeRestart).toEqual([]);

        await dbBeforeRestart.shutdown();
        untrack(dbBeforeRestart);

        const dbAfterRestart = track(await createPersistentDb(syncServer.serverUrl));
        expect(await dbAfterRestart.all(allTodos, { tier: "edge" })).toEqual([todo]);
        await dbAfterRestart.shutdown();
        untrack(dbAfterRestart);

        const dbAfterSettlement = track(await createPersistentDb(undefined));
        expect(await dbAfterSettlement.all(allTodos, { tier: "local" })).toEqual([todo]);
      });
    });
  });

  it("recovers sync after browser-side network loss with B in a separate context", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions("sync-recover");
    const sharedLocalAuthToken = generateAuthSecret();
    const { appId, serverUrl, adminSecret } = syncServer;
    const dbA = await createSyncedDb(ctx, "sync-recover-a", sharedLocalAuthToken, syncServer);
    const remoteDbId = trackRemoteBrowserDb(uniqueDbName("sync-recover-remote"));
    await createRemoteBrowserDb({
      id: remoteDbId,
      appId,
      dbName: uniqueDbName("sync-recover-b"),
      table: "todos",
      schemaJson: JSON.stringify(app.wasmSchema),
      serverUrl,
      adminSecret,
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

  /**
   *   writer ──baseline write──► server
   *   fresh probe starts while server traffic is blocked
   *   probe ──edge query pending──X server
   *   network unblocks
   *   expected: the first fresh edge query completes without needing a second client recreate
   */
  it("replays a fresh edge query once upstream attaches after init", async () => {
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
        "Writer sees baseline row at edge before blocking",
        20000,
        "edge",
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
        "Fresh edge query resolves after upstream attach",
        20000,
        "edge",
      );

      await sleep(500);
      await unblockJazzServerNetwork(serverUrl);
      await sleep(250);

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
   *   expected: the earlier offline worker write also promotes to B + fresh edge client
   */
  it("promotes offline worker rows after reconnect while the worker stays alive", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions("sync-offline");
    const sharedLocalAuthToken = generateAuthSecret();
    const { appId, serverUrl, adminSecret } = syncServer;
    const dbA = await createSyncedDb(ctx, "sync-offline-a", sharedLocalAuthToken, syncServer);
    const remoteDbId = trackRemoteBrowserDb(uniqueDbName("sync-offline-remote"));
    await createRemoteBrowserDb({
      id: remoteDbId,
      appId,
      dbName: uniqueDbName("sync-offline-b"),
      table: "todos",
      schemaJson: JSON.stringify(app.wasmSchema),
      serverUrl,
      adminSecret,
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
    await sleep(250);

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
    await sleep(250);

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
        "Fresh client sees offline worker row at edge after reconnect",
        20000,
        "edge",
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
          snapshots.push([...rows]);
        },
        { propagation: "local-only" },
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
        const rows = await dbB.all(allTodos, { propagation: "local-only" });
        return rows.some((row) => row.title === "local-only-local-1");
      },
      8000,
      "local-only query should retrieve persisted IndexedDB rows after reopen",
    );

    const snapshotsB = await dbB.all(allTodos, { propagation: "local-only" });
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
          snapshots.push([...rows]);
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
        snapshots.push([...rows]);
      }),
    );

    await waitForCondition(
      async () => snapshots.some((rows) => rows.some((row) => row.title === title)),
      8000,
      "Late tab subscription should hydrate the persisted row",
    );

    unsubscribe();
  });

  it.fails("surfaces schema mismatch errors and recovers after the pinning tab closes", async () => {
    const dbName = uniqueDbName("schema-mismatch-recovery");
    const oldTab = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );
    oldTab.insert(todos, { title: "Old schema row", done: false });
    await waitForCondition(
      async () => {
        const rows = await oldTab.all(allTodos, { tier: "local" });
        return rows.some((row) => row.title === "Old schema row");
      },
      8000,
      "Old tab should be durable-ready with the original schema",
    );

    const nextApp = s.defineApp({
      todos: s.table({
        title: s.string(),
        done: s.boolean(),
        priority: s.string().optional(),
      }),
    });

    const newTab = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );
    await expect(
      withTimeout(
        newTab.all(nextApp.todos, { tier: "local" }),
        8000,
        "Schema-blocked tab query should reject instead of hanging",
      ),
    ).rejects.toThrow("incompatible persistent browser schema");

    await oldTab.shutdown();
    const rows = await withTimeout(
      newTab.all(nextApp.todos, { tier: "local" }),
      8000,
      "Recovered tab should be able to query with its own schema",
    );
    expect(Array.isArray(rows)).toBe(true);
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
        driver: { type: "persistent", dbName },
      }),
    );
    const dbB = track(
      await createDb({
        appId,
        serverUrl,
        jwtToken: validJwt,
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
      localUpdates: "immediate",
    });
    expect(rowAfterNullUpdate).not.toBeNull();
    expect(rowAfterNullUpdate?.description ?? null).toBeNull();
  }, 60000);
});

// ---------------------------------------------------------------------------
// Local helpers (thin wrappers over support.ts using local schema types)
// ---------------------------------------------------------------------------

async function waitForTodos(
  db: Db,
  predicate: (rows: Todo[]) => boolean,
  label: string,
  timeoutMs = 15000,
  tier?: "local" | "edge",
): Promise<Todo[]> {
  return waitForQuery(db, allTodos, predicate, label, timeoutMs, tier);
}

async function waitForCatalogueTodos(
  db: Db,
  predicate: (rows: CatalogueTodo[]) => boolean,
  label: string,
  timeoutMs = 15_000,
  tier?: "local" | "edge",
): Promise<CatalogueTodo[]> {
  return waitForQuery(db, allCatalogueTodos, predicate, label, timeoutMs, tier);
}

async function publishCatalogueSchemaFamily(scope: string): Promise<JazzServerInfo> {
  const testingServer = await getJazzServerInfo(uniqueDbName(`worker-bridge-${scope}`));
  const { appId, serverUrl, adminSecret } = testingServer;

  const v1 = await deploy({
    appId,
    serverUrl,
    adminSecret,
    schema: catalogueAppV1.wasmSchema,
    permissions: cataloguePermissionsV1,
  });

  const v2 = await deploy({
    appId,
    serverUrl,
    adminSecret,
    schema: catalogueAppV2.wasmSchema,
  });

  const migration = s.defineMigration({
    fromHash: v1.schema.hash,
    toHash: v2.schema.hash,
    from: catalogueSchemaV1,
    to: catalogueSchemaV2,
    migrate: {
      todos: {
        description: s.add.string({ default: null }),
      },
    },
  });

  await deploy({
    appId,
    serverUrl,
    adminSecret,
    schema: catalogueAppV2.wasmSchema,
    permissions: cataloguePermissionsV2,
    migration,
  });

  return testingServer;
}

async function publishSyncServerSchemaAndPermissions(
  scope: string,
  permissions?: CompiledPermissions,
  schema?: Schema,
): Promise<JazzServerInfo> {
  const testingServer = await getJazzServerInfo(uniqueDbName(`worker-bridge-${scope}`));
  const permissionsToPublish = permissions ?? {
    todos: {
      select: { using: { type: "True" } },
      insert: { with_check: { type: "True" } },
      update: {
        using: { type: "True" },
        with_check: { type: "True" },
      },
      delete: { using: { type: "True" } },
    },
    projects: {
      select: { using: { type: "True" } },
      insert: { with_check: { type: "True" } },
      update: {
        using: { type: "True" },
        with_check: { type: "True" },
      },
      delete: { using: { type: "True" } },
    },
  };
  await publishPermissionsForServer(testingServer, permissionsToPublish, schema);
  return testingServer;
}

async function publishPermissionsForServer(
  testingServer: JazzServerInfo,
  permissions: CompiledPermissions,
  schema?: Schema,
): Promise<void> {
  const { appId, serverUrl, adminSecret } = testingServer;
  await deploy({
    appId,
    serverUrl,
    adminSecret,
    schema: schema ?? app.wasmSchema,
    permissions,
  });
}
