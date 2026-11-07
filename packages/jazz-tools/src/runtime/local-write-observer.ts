/**
 * @internal Development diagnostics only: observe that application code wrote
 * through a Db, without seeing what it wrote. Nothing in the write path waits
 * on these listeners, and a Db with no observer pays one WeakMap lookup.
 */
const observers = new WeakMap<object, Set<() => void>>();

/** @internal Call `listener` after each local write (or transaction start) on `db`. */
export function observeLocalWrites(db: object, listener: () => void): () => void {
  let listeners = observers.get(db);
  if (!listeners) observers.set(db, (listeners = new Set()));
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

/** @internal Db write entry points report each accepted local write here. */
export function notifyLocalWrite(db: object): void {
  const listeners = observers.get(db);
  if (!listeners?.size) return;
  for (const listener of [...listeners]) {
    try {
      listener();
    } catch (error) {
      console.error("Jazz local write observer failed", error);
    }
  }
}
