const RELOAD_MARKER = "jazz:indexeddb-invalidation-reload";
const RELOAD_LOOP_WINDOW_MS = 10_000;

export interface StorageInvalidationReloadContext {
  now?: () => number;
  reload?: () => void;
  storage?: Pick<Storage, "getItem" | "setItem">;
}

/** Reload once per tab, but fail closed instead of entering a reload loop. */
export function reloadAfterStorageInvalidation(
  context: StorageInvalidationReloadContext = {},
): boolean {
  const now = (context.now ?? Date.now)();
  try {
    const storage = context.storage ?? globalThis.sessionStorage;
    const previous = Number(storage.getItem(RELOAD_MARKER) ?? 0);
    if (Number.isFinite(previous) && now - previous < RELOAD_LOOP_WINDOW_MS) return false;
    storage.setItem(RELOAD_MARKER, String(now));
  } catch {
    // Reloading is still safer than retaining a runtime from a deleted
    // persistence epoch when sessionStorage itself is unavailable.
  }
  (context.reload ?? (() => globalThis.location.reload()))();
  return true;
}
