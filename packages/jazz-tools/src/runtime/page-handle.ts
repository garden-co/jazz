// `window.__jazz`: the sync state of the page's Jazz databases, for an agent
// or a test driving the page through Playwright or a browser, without
// scraping the UI or reaching into app code:
//
//   await page.evaluate(() => window.__jazz.status())
//   await page.evaluate(() => window.__jazz.waitForSync({ timeoutMs: 10_000 }))
//
// It carries only sync state, never data or credentials, so it is there in
// production too: any script on the page can already reach the Db itself.

import type { SyncStatus } from "./sync-status.js";

/** What `window.__jazz` needs from a Db. */
export interface PageDb {
  getSyncStatus(): SyncStatus;
  waitForSync(options?: { timeoutMs?: number; signal?: AbortSignal }): Promise<void>;
}

export interface PageDbInfo {
  /** The app the Db belongs to, as configured (`appId`). */
  appId: string | null;
  status: SyncStatus;
}

/** `window.__jazz`. Version 1 promises the shape below and nothing more. */
export interface JazzPageHandle {
  readonly version: 1;
  /**
   * The sync status of the page's Db (see `db.getSyncStatus()`), or null
   * while none is open. With several open, the one opened last.
   */
  status(): SyncStatus | null;
  /** How many of this page's committed transactions the server hasn't accepted or rejected yet. */
  pending(): number;
  /** Every open Db, oldest first. */
  dbs(): PageDbInfo[];
  /**
   * Resolve once every open Db's writes so far are accepted by the server
   * (see `db.waitForSync()`): rejects with the first rejection, or a timeout.
   */
  waitForSync(options?: { timeoutMs?: number }): Promise<void>;
}

// Clearing and shutting down a page's persistent client storage, from
// window-client-storage.ts, shares the same object.
export type WindowJazzStorageDb = {
  deleteClientStorage(): Promise<void>;
  shutdown(): Promise<void>;
};

export type LiveStorageContext = {
  db: WindowJazzStorageDb;
  namespace: string;
};

export interface WindowJazzClientApi {
  clearStorage(namespace?: string): Promise<void>;
  listLiveStorageNamespaces(): string[];
  /**
   * Shut down live Jazz client storage contexts and await full cleanup
   * (runtime teardown and pending IndexedDB persistence). Pass a namespace to target
   * one context; with no argument, shuts down every live context.
   *
   * Resolves once all targets have completed shutdown.  Db.shutdown is
   * idempotent, so this is safe to call alongside framework-driven cleanup
   * (e.g. JazzProvider unmount).
   */
  shutdown(namespace?: string): Promise<void>;
}

declare global {
  interface Window {
    /** Present once a Jazz Db opened on the page. */
    __jazz?: WindowJazzClientApi & JazzPageHandle;
    __jazzWindowStorageContexts__?: Set<LiveStorageContext>;
  }
}

export function getLiveStorageContexts(currentWindow: Window): Set<LiveStorageContext> {
  if (!currentWindow.__jazzWindowStorageContexts__) {
    currentWindow.__jazzWindowStorageContexts__ = new Set();
  }

  return currentWindow.__jazzWindowStorageContexts__;
}

function listLiveStorageNamespaces(currentWindow: Window): string[] {
  return [
    ...new Set([...getLiveStorageContexts(currentWindow)].map((entry) => entry.namespace)),
  ].sort();
}

function formatAvailableNamespaces(namespaces: readonly string[]): string {
  if (namespaces.length === 0) {
    return "";
  }

  return ` Available namespaces: ${namespaces.join(", ")}.`;
}

function resolveStorageTarget(currentWindow: Window, namespace?: string): WindowJazzStorageDb {
  const liveContexts = [...getLiveStorageContexts(currentWindow)];
  const namespaces = listLiveStorageNamespaces(currentWindow);

  if (liveContexts.length === 0) {
    throw new Error("No live Jazz storage contexts are available on this page.");
  }

  const trimmedNamespace = namespace?.trim();
  if (!trimmedNamespace) {
    if (namespaces.length === 1) {
      const [onlyNamespace] = namespaces;
      return liveContexts.find((entry) => entry.namespace === onlyNamespace)!.db;
    }

    throw new Error(
      `Multiple live Jazz storage contexts are available. Call window.__jazz.clearStorage("<namespace>").${formatAvailableNamespaces(
        namespaces,
      )}`,
    );
  }

  const match = liveContexts.find((entry) => entry.namespace === trimmedNamespace);
  if (!match) {
    throw new Error(
      `No live Jazz storage context matches "${trimmedNamespace}".${formatAvailableNamespaces(
        namespaces,
      )}`,
    );
  }

  return match.db;
}

const open = new Map<PageDb, string | null>();

function handle(): JazzPageHandle {
  const latest = () => [...open.keys()].at(-1) ?? null;
  return {
    version: 1,
    status: () => latest()?.getSyncStatus() ?? null,
    pending: () => [...open.keys()].reduce((sum, db) => sum + db.getSyncStatus().pendingUploads, 0),
    dbs: () => [...open].map(([db, appId]) => ({ appId, status: db.getSyncStatus() })),
    waitForSync: async (options = {}) => {
      await Promise.all([...open.keys()].map((db) => db.waitForSync(options)));
    },
  };
}

/**
 * List `db` on `window.__jazz` until the returned function is called. Does
 * nothing outside a browser page (Node, workers, React Native).
 */
export function registerPageDb(db: PageDb, appId: string | null): () => void {
  const page = (globalThis as { window?: Window }).window;
  if (!page || typeof page.document === "undefined") return () => {};
  open.set(db, appId);
  ensureWindowJazz(page);
  return () => {
    open.delete(db);
  };
}

/** Install `window.__jazz` (storage and sync status), keeping what's already on it. */
export function ensureWindowJazz(page: Window): WindowJazzClientApi & JazzPageHandle {
  const existing = page.__jazz;
  const host =
    existing && typeof existing === "object"
      ? (existing as unknown as Record<string, unknown>)
      : {};
  const api = Object.assign(host, handle(), {
    async clearStorage(namespace?: string): Promise<void> {
      await resolveStorageTarget(page, namespace).deleteClientStorage();
    },
    listLiveStorageNamespaces(): string[] {
      return listLiveStorageNamespaces(page);
    },
    async shutdown(namespace?: string): Promise<void> {
      const contexts = [...getLiveStorageContexts(page)];
      if (contexts.length === 0) return;
      const trimmed = namespace?.trim();
      const targets = trimmed ? contexts.filter((entry) => entry.namespace === trimmed) : contexts;
      await Promise.all(targets.map((entry) => entry.db.shutdown()));
    },
  }) as WindowJazzClientApi & JazzPageHandle;
  page.__jazz = api;
  return api;
}
