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

declare global {
  interface Window {
    __jazz?: JazzPageHandle;
  }
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
  // With two copies of jazz-tools on one page, the first one's handle stays.
  if (!Object.hasOwn(page, "__jazz")) {
    Object.defineProperty(page, "__jazz", {
      value: handle(),
      configurable: true,
      enumerable: false,
      writable: false,
    });
  }
  return () => {
    open.delete(db);
  };
}
