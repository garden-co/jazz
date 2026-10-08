/// <reference types="vite/client" />

/**
 * Shared schemas, case-owned authorities and cleanup for the worker-bridge
 * browser scenarios. Each test file retains independent client bookkeeping.
 */

import { expect, beforeEach, afterEach } from "vitest";
import { commands } from "vitest/browser";
import { Db, type QueryBuilder } from "../../src/runtime/db.js";
import { type Schema } from "../../src/drivers/types.js";
import {
  INDEXEDDB_BTREE_METADATA_STORE,
  INDEXEDDB_BTREE_PAGES_STORE,
  INDEXEDDB_STORAGE_MANIFEST_KEY,
  INDEXEDDB_STORAGE_MANIFEST_STORE,
} from "../../src/runtime/indexeddb-page-store.js";
import { setBrowserFollowerProbeTimingForTest } from "../../src/runtime/native-runtime/browser-follower-connection.js";
import { TestCleanup, uniqueDbName, waitForCondition, waitForQuery } from "./support.js";
import { getJazzServerInfo, stopJazzServer, type JazzServerInfo } from "./testing-server.js";
import { closeRemoteBrowserDb, waitForRemoteBrowserDbTitle } from "./remote-browser-db.js";
import { CompiledPermissions, schema as s, migration as m } from "../../src/";
import { computeSchemaHash, deploy } from "../../src/dev/catalogue.js";
import {
  deserializeBrowserRelayError,
  type BrowserInspectorContext,
  type BrowserInspectorControlEvent,
  type BrowserInspectorControlRequest,
  type BrowserRelayError,
} from "../../src/runtime/native-runtime/browser-worker-protocol.js";

export async function workerFaultBundleUrl(): Promise<string> {
  if (
    !("workerFaultBundleUrl" in commands) ||
    typeof commands.workerFaultBundleUrl !== "function"
  ) {
    throw new Error("Browser test project is missing the worker fault bundle command.");
  }
  const url: unknown = await commands.workerFaultBundleUrl();
  if (typeof url !== "string") throw new Error("Worker fault bundle command did not return a URL.");
  return url;
}

export let nextInspectorRequestId = 1;

export async function listWorkerContexts(port: MessagePort): Promise<BrowserInspectorContext[]> {
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

export async function listWorkerLifecycle(
  port: MessagePort,
): Promise<Extract<BrowserInspectorControlEvent, { type: "lifecycle-trace" }>["entries"]> {
  const id = nextInspectorRequestId++;
  return new Promise((resolve) => {
    const onMessage = (event: MessageEvent<BrowserInspectorControlEvent>) => {
      if (event.data.type !== "lifecycle-trace" || event.data.id !== id) return;
      port.removeEventListener("message", onMessage);
      resolve(event.data.entries);
    };
    port.addEventListener("message", onMessage);
    port.postMessage({ type: "lifecycle-trace", id } satisfies BrowserInspectorControlRequest);
  });
}

export async function waitForWorkerContextRelease(
  port: MessagePort,
  dbName: string,
): Promise<void> {
  await waitForCondition(
    async () => !(await listWorkerContexts(port)).some((context) => context.dbName === dbName),
    5000,
    `SharedWorker context ${dbName} should be destroyed before restart`,
  );
}

export async function terminateWorker(port: MessagePort): Promise<void> {
  const id = nextInspectorRequestId++;
  await new Promise<void>((resolve, reject) => {
    const onMessage = (event: MessageEvent<BrowserInspectorControlEvent>) => {
      if (event.data.type !== "result" || event.data.id !== id) return;
      port.removeEventListener("message", onMessage);
      if (event.data.error) reject(deserializeBrowserRelayError(event.data.error));
      else resolve();
    };
    port.addEventListener("message", onMessage);
    port.postMessage({ type: "terminate-worker", id } satisfies BrowserInspectorControlRequest);
  });
}

// ---------------------------------------------------------------------------
// Test schema — a simple "todos" table
// ---------------------------------------------------------------------------

// Liveness tests exercise the real worker, real probes and real pongs, but
// with the page's probe policy scaled from 30s + 30s down to 1s + 1s. The
// timer arithmetic itself is covered with fake timers in
// src/runtime/native-runtime/browser-follower-connection.test.ts.
export const LIVENESS_TEST_PROBE_TIMING = { intervalMs: 1_000, replyMs: 1_000 } as const;
// Comfortably above one interval plus one reply grace under CI load.
export const LIVENESS_TEST_SIGNAL_MS = 15_000;

export const schema = {
  projects: s.table(
    {
      name: s.string(),
    },
    { todosViaProject: s.reverse("todos", "project") },
  ),
  todos: s.table(
    {
      title: s.string(),
      done: s.boolean(),
      projectId: s.uuid().optional(),
      tags: s.array(s.string()).optional(),
    },
    { project: s.rel("projects", "projectId") },
  ),
};

export type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
export const { projects, todos } = app;
export type Todo = s.RowOf<typeof todos>;

// Keep the maintained-index hydration fixture separate from the broadly used
// worker-bridge schema: this test must exercise the same two-equality indexed
// source shape as the native receipt, without changing unrelated test schemas.
export const maintainedIndexedSchema = {
  projects: s.table(
    {
      name: s.string(),
    },
    {},
  ),
  todos: s
    .table(
      {
        title: s.string(),
        done: s.boolean(),
      },
      {},
    )
    .indexOnly(["title", "done"]),
};
export type MaintainedIndexedSchema = s.Schema<typeof maintainedIndexedSchema>;
export const maintainedIndexedApp: s.App<MaintainedIndexedSchema> =
  s.defineApp(maintainedIndexedSchema);
export const { todos: maintainedIndexedTodos } = maintainedIndexedApp;
export type MaintainedIndexedTodo = s.RowOf<typeof maintainedIndexedTodos>;

export const transactionIdentitySchema = {
  projects: s.table(
    {
      name: s.string(),
    },
    { documentsViaProject: s.reverse("documents", "project") },
  ),
  documents: s
    .table(
      {
        branch: s.string(),
        title: s.string(),
        projectId: s.uuid(),
        body: s.string(),
      },
      { project: s.rel("projects", "projectId") },
    )
    .branchBy("branch"),
};
export const transactionIdentityApp = s.defineApp(transactionIdentitySchema);
export const transactionIdentityPermissions = s.definePermissions(
  transactionIdentityApp,
  ({ policy }) => [
    policy.projects.allowRead.always(),
    policy.projects.allowInsert.always(),
    policy.projects.allowUpdate.always(),
    policy.projects.allowDelete.always(),
    policy.documents.allowRead.always(),
    policy.documents.allowInsert.always(),
    policy.documents.allowUpdate.always(),
    policy.documents.allowDelete.always(),
  ],
);

export const readOnlyPermissions = s.definePermissions(app, ({ policy }) => [
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
export const recoveryTerminalPermissions = s.definePermissions(app, ({ policy }) => [
  policy.projects.allowRead.always(),
  policy.projects.allowInsert.always(),
  policy.projects.allowUpdate.always(),
  policy.projects.allowDelete.always(),
  policy.todos.allowRead.always(),
  policy.todos.allowInsert.where({ done: false }),
  policy.todos.allowUpdate.always(),
  policy.todos.allowDelete.always(),
]);

export const noUpdatePermissions = s.definePermissions(app, ({ policy }) => [
  policy.projects.allowRead.always(),
  policy.projects.allowInsert.always(),
  policy.projects.allowUpdate.never(),
  policy.projects.allowDelete.always(),
  policy.todos.allowRead.always(),
  policy.todos.allowInsert.always(),
  policy.todos.allowUpdate.never(),
  policy.todos.allowDelete.always(),
]);

export const noDeletePermissions = s.definePermissions(app, ({ policy }) => [
  policy.projects.allowRead.always(),
  policy.projects.allowInsert.always(),
  policy.projects.allowUpdate.always(),
  policy.projects.allowDelete.never(),
  policy.todos.allowRead.always(),
  policy.todos.allowInsert.always(),
  policy.todos.allowUpdate.always(),
  policy.todos.allowDelete.never(),
]);

export const nullableSchema = {
  todos: s.table(
    {
      title: s.string(),
      done: s.boolean(),
      description: s.string().optional(),
    },
    {},
  ),
};

export type NullableSchema = s.Schema<typeof nullableSchema>;
export const nullableApp: s.App<NullableSchema> = s.defineApp(nullableSchema);
export const nullablePermissions = s.definePermissions(nullableApp, ({ policy }) => [
  policy.todos.allowRead.always(),
  policy.todos.allowInsert.always(),
  policy.todos.allowUpdate.always(),
  policy.todos.allowDelete.always(),
]);

/** QueryBuilder that selects all todos. */
export const allTodos: QueryBuilder<Todo> = app.todos;

// A small published schema family used to prove that the persistent worker
// rehydrates catalogue state, including its migration lens, before a current
// client issues its first query after reopening.
export const catalogueSchemaV1 = {
  todos: s.table(
    {
      title: s.string(),
      completed: s.boolean(),
    },
    {},
  ),
};

export const catalogueSchemaV2 = {
  todos: s.table(
    {
      title: s.string(),
      completed: s.boolean(),
      description: s.string().optional(),
    },
    {},
  ),
};

export const catalogueAppV1 = s.defineApp(catalogueSchemaV1);
export const catalogueAppV2 = s.defineApp(catalogueSchemaV2);
export const { todos: catalogueTodos } = catalogueAppV2;
export type CatalogueTodo = s.RowOf<typeof catalogueTodos>;
export const allCatalogueTodos: QueryBuilder<CatalogueTodo> = catalogueAppV2.todos;

export const cataloguePermissionsV1 = s.definePermissions(catalogueAppV1, ({ policy }) => [
  policy.todos.allowRead.always(),
  policy.todos.allowInsert.always(),
  policy.todos.allowUpdate.always(),
  policy.todos.allowDelete.always(),
]);

export const cataloguePermissionsV2 = s.definePermissions(catalogueAppV2, ({ policy }) => [
  policy.todos.allowRead.always(),
  policy.todos.allowInsert.always(),
  policy.todos.allowUpdate.always(),
  policy.todos.allowDelete.always(),
]);

/**
 * Structurally valid JWT with a deliberately invalid signature: parses fine on
 * the client (sub/exp claims) but the testing server rejects it at handshake.
 */
export function makeStructurallyValidJwt(userId: string): string {
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
export function todosByProject(projectId: string): QueryBuilder<Todo> {
  return app.todos.where({ projectId });
}

export type RawForegroundLease = {
  node: Uint8Array;
  returnWithHighWater(value: bigint): Promise<void>;
};

export function startRawForegroundLease(
  port: MessagePort,
  request: {
    dbName: string;
    storageOwner: string;
    testDelayBeforeLeaseAllocationMs?: number;
  },
): { queued: Promise<void>; ready: Promise<RawForegroundLease> } {
  let resolveQueued!: () => void;
  const queued = new Promise<void>((resolve) => {
    resolveQueued = resolve;
  });
  const ready = new Promise<RawForegroundLease>((resolve, reject) => {
    const onMessage = (
      event: MessageEvent<{
        type?: string;
        error?: BrowserRelayError;
        node?: Uint8Array;
        leaseId?: string;
      }>,
    ) => {
      if (event.data?.type === "foreground-node-lease-test-queued") {
        resolveQueued();
        return;
      }
      if (event.data?.type === "foreground-node-lease-error" && event.data.error) {
        port.removeEventListener("message", onMessage);
        reject(deserializeBrowserRelayError(event.data.error));
        return;
      }
      if (event.data?.type !== "foreground-node-lease-ready" || !event.data.node) return;
      const node = event.data.node.slice();
      resolve({
        node,
        returnWithHighWater(value) {
          return new Promise<void>((resolveReturn, rejectReturn) => {
            const onResult = (
              resultEvent: MessageEvent<{ type?: string; error?: BrowserRelayError }>,
            ) => {
              if (resultEvent.data?.type !== "foreground-node-lease-result") return;
              port.removeEventListener("message", onResult);
              port.removeEventListener("message", onMessage);
              port.close();
              if (resultEvent.data.error) {
                rejectReturn(deserializeBrowserRelayError(resultEvent.data.error));
              } else resolveReturn();
            };
            port.addEventListener("message", onResult);
            port.postMessage({
              type: "return-foreground-node-lease",
              confirmedTxTime: value.toString(),
            });
          });
        },
      });
    };
    port.addEventListener("message", onMessage);
    port.start();
    port.postMessage({ type: "acquire-foreground-node-lease", ...request });
  });
  return { queued, ready };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Local helpers (thin wrappers over support.ts using local schema types)
// ---------------------------------------------------------------------------

export async function waitForTodos(
  db: Db,
  predicate: (rows: Todo[]) => boolean,
  label: string,
  timeoutMs = 15000,
  tier?: "local" | "global",
): Promise<Todo[]> {
  return waitForQuery(db, allTodos, predicate, label, timeoutMs, tier);
}

export async function waitForCatalogueTodos(
  db: Db,
  predicate: (rows: CatalogueTodo[]) => boolean,
  label: string,
  timeoutMs = 15_000,
  tier?: "local" | "global",
): Promise<CatalogueTodo[]> {
  return waitForQuery(db, allCatalogueTodos, predicate, label, timeoutMs, tier);
}

// beforeAll/default authorities are not enlisted in a test's cleanup.
let testOwnedServerUrls: Set<string> | undefined;

export async function stopOwnedJazzServer(serverUrl: string): Promise<void> {
  await stopJazzServer(serverUrl);
  testOwnedServerUrls?.delete(serverUrl);
}

export async function publishCatalogueSchemaFamily(scope: string): Promise<JazzServerInfo> {
  const testingServer = await getJazzServerInfo(uniqueDbName(`worker-bridge-${scope}`));
  testOwnedServerUrls?.add(testingServer.serverUrl);
  const { appId, serverUrl, adminSecret } = testingServer;

  const v1 = await deploy({
    appId,
    serverUrl,
    adminSecret,
    schema: catalogueAppV1.wasmSchema,
    permissions: cataloguePermissionsV1,
  });

  const migration = m.defineMigration({
    fromHash: v1.schema.hash,
    toHash: await computeSchemaHash(catalogueAppV2.wasmSchema),
    from: catalogueSchemaV1,
    to: catalogueSchemaV2,
    migrate: {
      todos: {
        description: m.add.string({ default: null }),
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

export async function publishSyncServerSchemaAndPermissions(
  scope: string,
  permissions?: CompiledPermissions,
  schema?: Schema,
): Promise<JazzServerInfo> {
  const testingServer = await getJazzServerInfo(uniqueDbName(`worker-bridge-${scope}`));
  testOwnedServerUrls?.add(testingServer.serverUrl);
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

export async function publishPermissionsForServer(
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

export async function replaceStorageManifest(name: string, manifest: unknown): Promise<void> {
  const database = await requestResult(indexedDB.open(name));
  const transaction = database.transaction(INDEXEDDB_STORAGE_MANIFEST_STORE, "readwrite");
  transaction
    .objectStore(INDEXEDDB_STORAGE_MANIFEST_STORE)
    .put(manifest, INDEXEDDB_STORAGE_MANIFEST_KEY);
  await transactionDone(transaction);
  database.close();
}

export async function rawStorageRecords(name: string): Promise<Record<string, unknown>> {
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

export function requestResult<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error("IndexedDB request failed"));
  });
}

export function transactionDone(transaction: IDBTransaction): Promise<void> {
  return new Promise((resolve, reject) => {
    transaction.oncomplete = () => resolve();
    transaction.onerror = () =>
      reject(transaction.error ?? new Error("IndexedDB transaction failed"));
    transaction.onabort = () =>
      reject(transaction.error ?? new Error("IndexedDB transaction aborted"));
  });
}

/**
 * Per-describe cleanup and remote-tab bookkeeping shared by the SharedWorker
 * bridge test files. Call once inside the describe; it registers the afterEach.
 */
export function useSharedWorkerBridgeHarness() {
  const ctx = new TestCleanup();
  const remoteBrowserDbIds = new Set<string>();
  const errorListeners = new Set<(event: ErrorEvent) => void>();

  beforeEach(() => {
    testOwnedServerUrls = new Set<string>();
  });

  function trackRemoteBrowserDb(id: string): string {
    remoteBrowserDbIds.add(id);
    return id;
  }

  async function waitForRemoteTodoTitle(
    id: string,
    title: string,
    label: string,
    timeoutMs: number,
    tier?: "local" | "global",
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

  // Stops the current SharedWorker so the next createDb restores data from storage
  async function shutdownDbAndWorker(db: Db, inspector?: MessagePort): Promise<void> {
    const port = inspector ?? (await db.openInspectorControlPort());
    port.start();
    try {
      const [context] = await listWorkerContexts(port);
      expect(context).toBeDefined();
      await db.shutdown();
      untrack(db);
      await waitForWorkerContextRelease(port, context!.dbName);
      await terminateWorker(port);
    } finally {
      port.close();
    }
  }

  afterEach(async () => {
    const ownedServers = testOwnedServerUrls;
    testOwnedServerUrls = undefined;
    // A liveness test that throws before its own finally must not leave
    // later tests on the scaled probe policy.
    setBrowserFollowerProbeTimingForTest();
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
    if (ownedServers) {
      await Promise.all([...ownedServers].map((serverUrl) => stopJazzServer(serverUrl)));
    }
  });

  return {
    ctx,
    remoteBrowserDbIds,
    errorListeners,
    trackRemoteBrowserDb,
    waitForRemoteTodoTitle,
    track,
    trackSubscription,
    untrack,
    shutdownDbAndWorker,
  };
}
