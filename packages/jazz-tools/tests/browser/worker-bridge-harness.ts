/// <reference types="vite/client" />

/**
 * Shared schemas, fixtures and helpers for the worker-bridge*.test.ts files.
 * The bridge suite is split across files so the browser runner can schedule
 * its three parts on separate workers; test bodies are unchanged.
 */

import { expect, afterEach } from "vitest";
import { commands } from "vitest/browser";
import { Db, type QueryBuilder } from "../../src/runtime/db.js";
import {
  INDEXEDDB_BTREE_METADATA_STORE,
  INDEXEDDB_BTREE_PAGES_STORE,
  INDEXEDDB_STORAGE_MANIFEST_KEY,
  INDEXEDDB_STORAGE_MANIFEST_STORE,
} from "../../src/runtime/indexeddb-page-store.js";
import { setBrowserFollowerProbeTimingForTest } from "../../src/runtime/native-runtime/browser-follower-connection.js";
import { TestCleanup, uniqueDbName, waitForCondition, waitForQuery } from "./support.js";
import {
  deploy,
  getJazzServerInfo,
  jazzCore,
  startJazzApp,
  type JazzServerInfo,
} from "./testing-server.js";
import { closeRemoteBrowserDb, waitForRemoteBrowserDbTitle } from "./remote-browser-db.js";
import type { SchemaSourceInput } from "../../src/schema-source.js";
import type { TestPermissionsFile } from "../../src/testing/test-app.js";
import { app, type Todo } from "./apps/worker-bridge/jazz.generated.js";
import openPermissionsFile from "./apps/worker-bridge/jazz.permissions.json";
import readOnlyPermissionsFile from "./apps/worker-bridge/read-only.permissions.json";
import recoveryTerminalPermissionsFile from "./apps/worker-bridge/recovery-terminal.permissions.json";
import noUpdatePermissionsFile from "./apps/worker-bridge/no-update.permissions.json";
import noDeletePermissionsFile from "./apps/worker-bridge/no-delete.permissions.json";
import {
  app as maintainedIndexedAppFile,
  type Todo as MaintainedIndexedTodo,
} from "./apps/worker-bridge-indexed/jazz.generated.js";
import { app as transactionIdentityAppFile } from "./apps/worker-bridge-transaction-identity/jazz.generated.js";
import transactionIdentityPermissionsFile from "./apps/worker-bridge-transaction-identity/jazz.permissions.json";
import { app as nullableAppFile } from "./apps/worker-bridge-nullable/jazz.generated.js";
import nullablePermissionsFile from "./apps/worker-bridge-nullable/jazz.permissions.json";
import { app as catalogueAppV1File } from "./apps/worker-bridge-catalogue-v1/jazz.generated.js";
import cataloguePermissionsV1File from "./apps/worker-bridge-catalogue-v1/jazz.permissions.json";
import {
  app as catalogueAppV2File,
  type Todo as CatalogueTodo,
} from "./apps/worker-bridge-catalogue-v2/jazz.generated.js";
import cataloguePermissionsV2File from "./apps/worker-bridge-catalogue-v2/jazz.permissions.json";
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
// with the page's probe policy scaled from 30s + 30s down to 2s + 2s. The
// timer arithmetic itself is covered with fake timers in
// src/runtime/native-runtime/browser-follower-connection.test.ts.
export const LIVENESS_TEST_PROBE_TIMING = { intervalMs: 2_000, replyMs: 2_000 } as const;
// Comfortably above one interval plus one reply grace under CI load.
export const LIVENESS_TEST_SIGNAL_MS = 15_000;

// The apps live under ./apps/worker-bridge*/ (jazz.app.json, with their
// committed bindings); the permission variants are jazz.permissions.json
// files beside the main app.
export { app };
export type { Todo };
export const { projects, todos } = app;

// Keep the maintained-index hydration fixture separate from the broadly used
// worker-bridge schema: this test must exercise the same two-equality indexed
// source shape as the native receipt, without changing unrelated test schemas.
export const maintainedIndexedApp = maintainedIndexedAppFile;
export const { todos: maintainedIndexedTodos } = maintainedIndexedApp;
export type { MaintainedIndexedTodo };

export const transactionIdentityApp = transactionIdentityAppFile;
export const transactionIdentityPermissions: TestPermissionsFile =
  transactionIdentityPermissionsFile;

/** Every operation on projects and todos is allowed. */
export const openPermissions: TestPermissionsFile = openPermissionsFile;

export const readOnlyPermissions: TestPermissionsFile = readOnlyPermissionsFile;

// A single recovered worker restart must be able to settle two former
// foreground transactions independently: the ordinary todo is admitted,
// while the marked todo is rejected.  Keeping both outcomes in one authority
// policy makes the receipt independent of a mid-test policy redeploy.
export const recoveryTerminalPermissions: TestPermissionsFile = recoveryTerminalPermissionsFile;

export const noUpdatePermissions: TestPermissionsFile = noUpdatePermissionsFile;

export const noDeletePermissions: TestPermissionsFile = noDeletePermissionsFile;

export const nullableApp = nullableAppFile;
export const nullablePermissions: TestPermissionsFile = nullablePermissionsFile;

/** QueryBuilder that selects all todos. */
export const allTodos: QueryBuilder<Todo> = app.todos;

// A small published schema family used to prove that the persistent worker
// rehydrates catalogue state, including its migration lens, before a current
// client issues its first query after reopening. V2's jazz.app.json carries
// V1 as history and the lens adding `description` (default null).
export const catalogueAppV1 = catalogueAppV1File;
export const catalogueAppV2 = catalogueAppV2File;
export const { todos: catalogueTodos } = catalogueAppV2;
export type { CatalogueTodo };
export const allCatalogueTodos: QueryBuilder<CatalogueTodo> = catalogueAppV2.todos;

export const cataloguePermissionsV1: TestPermissionsFile = cataloguePermissionsV1File;
export const cataloguePermissionsV2: TestPermissionsFile = cataloguePermissionsV2File;

/**
 * Structurally valid JWT with a deliberately invalid signature: parses fine on
 * the client (sub/exp claims) but the testing server rejects it at handshake.
 */
export function makeStructurallyValidJwt(userId: string, issuer = "urn:jazz:test"): string {
  const encode = (value: unknown) =>
    btoa(JSON.stringify(value)).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
  const header = encode({ alg: "HS256", typ: "JWT" });
  const payload = encode({
    // Pass the signed-in user's issuer for a same-principal token the
    // server rejects for its signature.
    iss: issuer,
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
  tier?: "local-first" | "remote",
): Promise<Todo[]> {
  return waitForQuery(db, allTodos, predicate, label, timeoutMs, tier);
}

export async function waitForCatalogueTodos(
  db: Db,
  predicate: (rows: CatalogueTodo[]) => boolean,
  label: string,
  timeoutMs = 15_000,
  tier?: "local-first" | "remote",
): Promise<CatalogueTodo[]> {
  return waitForQuery(db, allCatalogueTodos, predicate, label, timeoutMs, tier);
}

export async function publishCatalogueSchemaFamily(scope: string): Promise<JazzServerInfo> {
  const testingServer = await getJazzServerInfo(uniqueDbName(`worker-bridge-${scope}`));
  const { appId, serverUrl, adminSecret } = testingServer;

  await deploy({
    appId,
    serverUrl,
    adminSecret,
    app: catalogueAppV1,
    permissions: cataloguePermissionsV1,
  });

  // V2 ships V1 as history with the lens between them, so this publishes the
  // V1 -> V2 migration with V2.
  await deploy({
    appId,
    serverUrl,
    adminSecret,
    app: catalogueAppV2,
    permissions: cataloguePermissionsV2,
  });

  return startJazzApp(testingServer, {
    schema: catalogueAppV2,
    permissions: cataloguePermissionsV2,
  });
}

export async function publishSyncServerSchemaAndPermissions(
  scope: string,
  permissions?: TestPermissionsFile,
  schema?: SchemaSourceInput,
): Promise<JazzServerInfo> {
  const testingServer = await getJazzServerInfo(uniqueDbName(`worker-bridge-${scope}`));
  return publishPermissionsForServer(testingServer, permissions ?? openPermissions, schema);
}

/**
 * Deploy `permissions` to the core behind `testingServer`, then (re)start the
 * app backend for them on the same origin, so clients reconnect to the new
 * bundle. Returns the app backend's info.
 */
export async function publishPermissionsForServer(
  testingServer: JazzServerInfo,
  permissions: TestPermissionsFile,
  schema?: SchemaSourceInput,
): Promise<JazzServerInfo> {
  const deployedSchema = schema ?? app;
  await deploy({
    ...jazzCore(testingServer),
    app: deployedSchema,
    permissions: permissions,
  });
  return startJazzApp(testingServer, { schema: deployedSchema, permissions });
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

  function trackRemoteBrowserDb(id: string): string {
    remoteBrowserDbIds.add(id);
    return id;
  }

  async function waitForRemoteTodoTitle(
    id: string,
    title: string,
    label: string,
    timeoutMs: number,
    tier?: "local-first" | "remote",
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
