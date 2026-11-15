/**
 * The back/forward cache keeps a page whole while the user is on another
 * one, and shows it again on "back". Chrome caches a page with its Web Locks
 * still held, so to the worker and to other tabs the cached page looks open.
 * A Jazz page holds two locks for its whole life: its store's
 * (`jazz-replica-v1`, see `replica-ledger`) and its own liveness lock
 * (`jazz-tab-alive`, see the shared worker connection). So each holder lets
 * go when the page goes into the cache (`pagehide`, `persisted`) and takes
 * its lock again when it comes back (`pageshow`, `persisted`). A page that
 * unloads lets go of nothing itself: the browser drops what it held.
 *
 * (Firefox caches no page connected to a SharedWorker that holds a lock, as
 * a local-first tab's worker always does, so there "back" loads the page
 * again either way.)
 *
 * Holders return in the order they registered, each after the one before
 * it settled: the store's hold registers before the client that opens the
 * store, so the store is held again (or the page reloads, because another
 * tab wiped it meanwhile) before the client reconnects to its worker.
 */

export type PageCacheHolder = {
  /** Let go of what the page holds, now: the page freezes right after. */
  leave(): void;
  /**
   * Take it again: the page is back from the cache. Called also for a
   * holder that never let go (it registered while the page was away, or a
   * return stopped short), which keeps what it holds. `"stop"`
   * when the page reloads instead: the holders after it don't return.
   */
  return(): void | "stop" | Promise<void | "stop">;
};

const holders: PageCacheHolder[] = [];
let away = false;
let returning: Promise<void> = Promise.resolve();
let listening = false;

/**
 * Let `holder` go while this page is in the back/forward cache. Returns the
 * function that unregisters it. A holder that registers while the page is
 * away (a client opening mid-navigation) holds nothing yet to let go of.
 */
export function holdAcrossPageCache(holder: PageCacheHolder): () => void {
  if (typeof window === "undefined" || typeof window.addEventListener !== "function") {
    return () => {};
  }
  listen();
  holders.push(holder);
  return () => {
    const index = holders.indexOf(holder);
    if (index !== -1) holders.splice(index, 1);
  };
}

function listen(): void {
  if (listening) return;
  listening = true;
  // Only a page the browser keeps: one that unloads drops its locks with
  // it, and letting go early would only cut short what it still sends.
  window.addEventListener("pagehide", (event) => {
    if (event.persisted) leave();
  });
  window.addEventListener("pageshow", (event) => {
    if (event.persisted) comeBack();
  });
}

function leave(): void {
  if (away) return;
  away = true;
  for (const holder of [...holders]) {
    try {
      holder.leave();
    } catch (error) {
      console.warn("Jazz could not let go of a page lock for the back/forward cache.", error);
    }
  }
}

function comeBack(): void {
  if (!away) return;
  away = false;
  const order = [...holders];
  returning = returning.then(async () => {
    for (const holder of order) {
      // Left again before this holder's turn: it stays let go, and the
      // next return takes it again (holders return only what they let go).
      if (away) return;
      // Unregistered meanwhile (its client closed): nothing to take again.
      if (!holders.includes(holder)) continue;
      try {
        if ((await holder.return()) === "stop") return;
      } catch (error) {
        console.warn("Jazz could not take a page lock again after the back/forward cache.", error);
      }
    }
  });
}

/** @internal For tests: whether the page is away, and a reset between cases. */
export const pageCacheForTest = {
  isAway: () => away,
  returned: () => returning,
  reset(): void {
    holders.length = 0;
    away = false;
    returning = Promise.resolve();
  },
};
