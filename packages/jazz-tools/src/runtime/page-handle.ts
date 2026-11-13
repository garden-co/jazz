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

/**
 * Marks the `window.__jazz` Jazz installed, so a later install (another
 * client, another copy of jazz-tools on the page) extends it, and a value the
 * app put there is told apart. `Symbol.for`, so every copy agrees.
 */
const JAZZ_PAGE_HANDLE = Symbol.for("jazz-tools.window-jazz");

/**
 * `window.__jazz` is Jazz's name: the first Db opened on the page sets it. A
 * value the app (or another library) put there first is left alone, neither
 * overwritten nor extended (it may be sealed or frozen), and nothing fails:
 * Jazz says so once on the console and keeps its handle off the page.
 */
function windowJazzConflictMessage(existing: unknown): string {
  return `window.__jazz is Jazz's page handle (window.__jazz.status(), waitForSync(), clearStorage()), but this page already has a window.__jazz that Jazz didn't set (${
    existing === null ? "null" : typeof existing
  }). Jazz leaves it as it is and works without its page handle; rename your app's global to get the handle back.`;
}

/** Pages already told, so each hears it once. */
const warnedPages = new WeakSet<object>();

/**
 * Whether `page` has a `window.__jazz` Jazz didn't set; warns once per page
 * if so. A browser session checks this when it starts, so the warning shows
 * there rather than only when a Db first opens (on the thin client, which
 * doesn't install the handle, that is the upgrade to local-first).
 */
export function warnWindowJazzConflict(page: Window): boolean {
  const existing = page.__jazz as unknown;
  if (existing === undefined || isJazzPageHandle(existing)) return false;
  if (!warnedPages.has(page)) {
    warnedPages.add(page);
    console.warn(windowJazzConflictMessage(existing));
  }
  return true;
}

/**
 * Jazz's own `window.__jazz`: marked by this or a later copy of jazz-tools,
 * or, from a copy before the mark (alpha.60 and earlier), the version-1
 * handle shape, which it extends as before.
 */
function isJazzPageHandle(value: unknown): value is Record<string | symbol, unknown> {
  if (!value || typeof value !== "object") return false;
  if (JAZZ_PAGE_HANDLE in value) return true;
  const legacy = value as { version?: unknown; status?: unknown };
  return legacy.version === 1 && typeof legacy.status === "function";
}

/**
 * Install `window.__jazz` (storage and sync status), extending the one Jazz
 * installed. With an app's own `window.__jazz` there, the handle is returned
 * but not put on the page (see {@link warnWindowJazzConflict}).
 */
export function ensureWindowJazz(page: Window): WindowJazzClientApi & JazzPageHandle {
  const conflict = warnWindowJazzConflict(page);
  const existing = conflict ? undefined : (page.__jazz as unknown);
  const host = (existing as Record<string | symbol, unknown> | undefined) ?? {};
  // An older copy's handle gets the mark too, for copies after this one.
  host[JAZZ_PAGE_HANDLE] = true;
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
  if (!conflict) page.__jazz = api;
  return api;
}
