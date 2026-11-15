/**
 * @internal Which sessions are open, for `jazz-tools/testing/sessions`.
 * Nothing is tracked until that module turns tracking on, so an app that
 * never imports it holds no extra references to its sessions. The set
 * lives on a `Symbol.for` global, so every copy of jazz-tools agrees.
 */
const OPEN = Symbol.for("jazz-tools.testing.openSessions");

interface Closable {
  close(): Promise<void>;
}

type Global = { [OPEN]?: Set<Closable> };

/** @internal Start tracking sessions opened from now on. */
export function trackJazzSessions(): Set<Closable> {
  const global = globalThis as Global;
  return (global[OPEN] ??= new Set());
}

/** @internal Called by every session as it opens; returns its untrack. */
export function registerJazzSession(session: Closable): () => void {
  const open = (globalThis as Global)[OPEN];
  if (!open) return () => {};
  open.add(session);
  return () => open.delete(session);
}
