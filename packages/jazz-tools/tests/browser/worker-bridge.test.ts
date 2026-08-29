/**
 * Browser integration tests for the SharedWorker + IndexedDB runtime.
 *
 * Runs in a real Chromium browser via @vitest/browser + playwright.
 * Uses real jazz-wasm, a real SharedWorker, and real IndexedDB storage.
 *
 * Server sync tests use a real jazz-tools server spawned by global-setup.
 */

import { describe, it, expect, afterEach, vi } from "vitest";
import {
  createDb,
  Db,
  getDbSubscriptionSource,
  resolveDefaultPersistentDbName,
  type QueryBuilder,
} from "../../src/runtime/db.js";
import type { Schema } from "../../src/drivers/types.js";
import { generateAuthSecret } from "../../src/runtime/auth-secret-store.js";
import {
  INDEXEDDB_BTREE_METADATA_STORE,
  INDEXEDDB_BTREE_PAGES_STORE,
  INDEXEDDB_STORAGE_MANIFEST,
  INDEXEDDB_STORAGE_MANIFEST_KEY,
  INDEXEDDB_STORAGE_MANIFEST_STORE,
} from "../../src/runtime/indexeddb-page-store.js";
import { createBrowserSharedWorkerBaseName } from "../../src/runtime/native-runtime/browser-shared-worker-connection.js";
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
  stopJazzServer,
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

const transactionIdentitySchema = {
  projects: s.table({
    name: s.string(),
  }),
  documents: s
    .table({
      branch: s.string(),
      title: s.string(),
      projectId: s.ref("projects"),
      body: s.string(),
    })
    .branchBy("branch"),
};
const transactionIdentityApp = s.defineApp(transactionIdentitySchema);
const transactionIdentityPermissions = s.definePermissions(transactionIdentityApp, ({ policy }) => [
  policy.projects.allowRead.always(),
  policy.projects.allowInsert.always(),
  policy.projects.allowUpdate.always(),
  policy.projects.allowDelete.always(),
  policy.documents.allowRead.always(),
  policy.documents.allowInsert.always(),
  policy.documents.allowUpdate.always(),
  policy.documents.allowDelete.always(),
]);

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

// A single recovered worker restart must be able to settle two former
// foreground transactions independently: the ordinary todo is admitted,
// while the marked todo is rejected.  Keeping both outcomes in one authority
// policy makes the receipt independent of a mid-test policy redeploy.
const recoveryTerminalPermissions = s.definePermissions(app, ({ policy }) => [
  policy.projects.allowRead.always(),
  policy.projects.allowInsert.always(),
  policy.projects.allowUpdate.always(),
  policy.projects.allowDelete.always(),
  policy.todos.allowRead.always(),
  policy.todos.allowInsert.where({ done: false }),
  policy.todos.allowUpdate.always(),
  policy.todos.allowDelete.always(),
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
  it("fences a generation-advanced worker realm until its live predecessor releases the physical root", async () => {
    const appId = uniqueDbName("physical-worker-epoch-app");
    const dbName = uniqueDbName("physical-worker-epoch-root");
    const secret = generateAuthSecret();
    const first = track(await createDb({ appId, secret, driver: { type: "persistent", dbName } }));
    try {
      // Materialize both the foreground lease and worker runtime before
      // deliberately advancing the page-side generation key.
      await first.all(allTodos, { tier: "local" });
      const workerName = createBrowserSharedWorkerBaseName(undefined, dbName);
      localStorage.setItem(`jazz:shared-worker-generation:${workerName}`, "1");

      // Planted overlap: generation one names a distinct SharedWorker even
      // though generation zero is live. It must fail before it can recover
      // generation zero's foreground lease pool.
      await expect(
        createDb({ appId, secret, driver: { type: "persistent", dbName } }),
      ).rejects.toThrow("active in another Jazz SharedWorker realm");

      await first.shutdown();
      untrack(first);
      await sleep(100);

      const successor = track(
        await createDb({ appId, secret, driver: { type: "persistent", dbName } }),
      );
      try {
        await expect(successor.all(allTodos, { tier: "local" })).resolves.toEqual([]);
      } finally {
        await successor.shutdown();
        untrack(successor);
      }
    } finally {
      await first.shutdown().catch(() => undefined);
      untrack(first);
    }
  });

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
        getClient(schema: typeof todos._schema): { subscribe: (...args: never[]) => number };
      }
    ).getClient(todos._schema);
    const nativeSubscribe = vi.spyOn(client, "subscribe");
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
    try {
      const initial = track(
        await createDb({
          appId: "test-app",
          secret,
          driver: { type: "persistent", dbName },
        }),
      );
      await initial
        .insert(todos, { title: "durable sentinel", done: false })
        .wait({ tier: "local" });
      await initial.shutdown();
      untrack(initial);
      // The last follower releases its worker context after the short idle
      // window. Without this, a cached worker runtime never reopens the raw
      // IndexedDB namespace and cannot observe the corruption below.
      await sleep(100);

      await replaceStorageManifest(dbName, {
        ...INDEXEDDB_STORAGE_MANIFEST,
        storageEpoch: 2,
      });
      const recordsBeforeRead = await rawStorageRecords(dbName);

      // Persistent create must acquire a durable foreground-node lease before
      // any synchronous mutation can mint a transaction identity. Storage
      // readiness therefore belongs to createDb, while schema selection stays
      // lazy. The original cause must reject that operation directly.
      await expect(
        createDb({
          appId: "test-app",
          secret,
          driver: { type: "persistent", dbName },
        }),
      ).rejects.toThrow("Missing or invalid IndexedDB storage epoch manifest");
      await sleep(0);
      expect(ambientErrors).toEqual([]);
      expect(unhandledRejections).toEqual([]);
      expect(await rawStorageRecords(dbName)).toEqual(recordsBeforeRead);
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
        jwtToken,
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

  it("rejects backend credentials through the SharedWorker relay", async () => {
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

    // A browser worker is a persistent client runtime, never a trusted
    // backend. Keeping backend credentials out of it avoids handing a
    // privileged capability to browser storage or worker ports.
    expect(() => db.insert(todos, { title: `backend-write-${Date.now()}`, done: false })).toThrow(
      "Persistent browser workers require a verified client session",
    );
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
    await expect(insertResult.wait({ tier: "edge" })).rejects.toMatchObject({
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
      appPeer.all(allTodos, { tier: "edge" }),
      writerPeer.all(allTodos, { tier: "edge" }),
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
    await expect(rejected.wait({ tier: "edge" })).rejects.toMatchObject({
      name: "PersistedWriteRejectedError",
      code: "permission_denied",
    });
    expect(await writerPeer.all(allTodos, { tier: "local" })).toEqual([]);
    expect(await appPeer.all(allTodos, { tier: "edge" })).toEqual([]);
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
    await waitForCondition(
      async () => mutationErrorSpy.mock.calls.length > 0,
      5000,
      "onMutationError handler should be called",
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
      transactionId: insertResult.txId,
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

  it("delivers a rejection to a runtime attached while the worker rehydrates", async () => {
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
    // This runtime is already attached when the restored worker receives the
    // settlement, so it is a live notification rather than unsupported
    // cross-lifecycle toast continuity. A later runtime still only observes
    // the reconciled row state below.
    await waitForCondition(
      () => replayAfterRestartSpy.mock.calls.length === 1,
      5000,
      "attached runtime should receive the restored worker's live rejection",
    );

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
    const secret = generateAuthSecret();
    const dbName = uniqueDbName("sync-recovery-terminal-pair");
    const createPersistentDb = (serverUrl?: string) =>
      createDb({
        appId: syncServer.appId,
        driver: { type: "persistent" as const, dbName },
        serverUrl,
        secret,
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

    const firstInspector = await first.openInspectorControlPort();
    firstInspector.start();
    await first.shutdown();
    untrack(first);
    await waitForWorkerContextRelease(firstInspector, dbName);
    await terminateWorker(firstInspector);

    const successor = track(await createPersistentDb(syncServer.serverUrl));
    const mutationErrors = vi.fn();
    successor.onMutationError(mutationErrors);
    // `createDb` is intentionally lazy. Attach the foreground runtime before
    // opening the inspector so this receipt observes the same public startup
    // path as an application's first local query.
    await successor.all(allTodos, { tier: "local" });
    const successorInspector = await successor.openInspectorControlPort();
    successorInspector.start();

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

    await successor.all(allTodos, { tier: "edge" });
    await sleep(250);
    expect(mutationErrors).toHaveBeenCalledTimes(1);
    await expect(successor.all(allTodos, { tier: "local" })).resolves.toEqual([
      expect.objectContaining({ id: accepted.value.id, title: "accepted after worker restart" }),
    ]);

    await successor.shutdown();
    untrack(successor);
    await waitForWorkerContextRelease(successorInspector, dbName);
    await terminateWorker(successorInspector);

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
        transactionId: insertResult.txId,
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
        transactionId: updateResult.txId,
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
        transactionId: deleteResult.txId,
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

    await stopJazzServer(syncServer.serverUrl);
    const edgeError = await withTimeout(
      db.all(allTodos, { tier: "edge" }),
      5000,
      "edge read did not observe the stopped server",
    ).then(
      () => null,
      (error: unknown) => error,
    );
    expect(edgeError).toBeInstanceOf(Error);
    expect((edgeError as Error).message).not.toContain(
      "edge read did not observe the stopped server",
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
          snapshots.push(rows);
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
          snapshots.push(rows);
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

  it("keeps explicit-name account caches separate, shared per scope, and destroys only the selected scope", async () => {
    const appId = uniqueDbName("explicit-browser-owner-app");
    const dbName = uniqueDbName("shared-device-cache");
    const aliceJwt = makeStructurallyValidJwt("explicit-base-alice");
    const bobJwt = makeStructurallyValidJwt("explicit-base-bob");
    const aliceConfig = {
      appId,
      jwtToken: aliceJwt,
      driver: { type: "persistent" as const, dbName },
    };
    const bobConfig = { appId, jwtToken: bobJwt, driver: { type: "persistent" as const, dbName } };
    const alicePhysicalName = resolveDefaultPersistentDbName(aliceConfig);
    const bobPhysicalName = resolveDefaultPersistentDbName(bobConfig);
    expect(alicePhysicalName).toMatch(new RegExp(`^${dbName}::jazz-browser-v1::`));
    expect(alicePhysicalName).not.toBe(bobPhysicalName);
    expect(alicePhysicalName).not.toContain(aliceJwt);
    expect(bobPhysicalName).not.toContain(bobJwt);

    let alice: Db | null = track(await createDb(aliceConfig));
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

async function replaceStorageManifest(name: string, manifest: unknown): Promise<void> {
  const database = await requestResult(indexedDB.open(name));
  const transaction = database.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readwrite");
  transaction
    .objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE)
    .put(manifest, INDEXEDDB_STORAGE_MANIFEST_KEY);
  await transactionDone(transaction);
  database.close();
}

async function rawStorageRecords(name: string): Promise<Record<string, unknown>> {
  const database = await requestResult(indexedDB.open(name));
  const storeNames = [
    INDEXEDDB_BTREE_PAGES_STORE,
    INDEXEDDB_BTREE_METADATA_STORE,
    INDEXEDDB_STORAGE_MANIFEST_STORE,
  ];
  const transaction = database.transaction(storeNames, "readonly");
  const records = Object.fromEntries(
    await Promise.all(
      storeNames.map(async (storeName) => {
        const store = transaction.objectStore(storeName);
        return [storeName, await requestResult(store.getAll())] as const;
      }),
    ),
  );
  await transactionDone(transaction);
  database.close();
  return records;
}

function requestResult<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error("IndexedDB request failed"));
  });
}

function transactionDone(transaction: IDBTransaction): Promise<void> {
  return new Promise((resolve, reject) => {
    transaction.oncomplete = () => resolve();
    transaction.onerror = () =>
      reject(transaction.error ?? new Error("IndexedDB transaction failed"));
    transaction.onabort = () =>
      reject(transaction.error ?? new Error("IndexedDB transaction aborted"));
  });
}
