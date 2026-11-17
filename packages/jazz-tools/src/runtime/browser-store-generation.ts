/**
 * Which incarnation of a browser store this is: a value in localStorage, per
 * physical store, that every tab-side path deleting or resetting the store
 * replaces. A tab that wasn't attached to the store's worker meanwhile (it
 * was in the back/forward cache, see `page-cache`) heard of no reset; it
 * compares the value it recorded with the one now, and a value gone (site
 * data cleared) counts as changed too.
 */

const key = (dbName: string) => `jazz:store-generation:${dbName}`;

/** Record a new incarnation of `dbName`'s store: it was deleted, reset or invalidated. */
export function renewBrowserStoreGeneration(dbName: string): void {
  try {
    globalThis.localStorage?.setItem(key(dbName), crypto.randomUUID());
  } catch {
    // No durable storage: a tab coming back can't tell, as before.
  }
}

/** `dbName`'s store incarnation now, recording one if there is none; `null` without localStorage. */
export function browserStoreGeneration(dbName: string): string | null {
  try {
    const storage = globalThis.localStorage;
    if (!storage) return null;
    const current = storage.getItem(key(dbName));
    if (current !== null) return current;
    const created = crypto.randomUUID();
    storage.setItem(key(dbName), created);
    return created;
  } catch {
    return null;
  }
}

/** Whether `dbName`'s store is another incarnation than `recorded` (from `browserStoreGeneration`). */
export function browserStoreGenerationChanged(dbName: string, recorded: string | null): boolean {
  if (recorded === null) return false;
  try {
    return globalThis.localStorage?.getItem(key(dbName)) !== recorded;
  } catch {
    // Storage that worked when the tab left and fails now: assume the worst.
    return true;
  }
}
