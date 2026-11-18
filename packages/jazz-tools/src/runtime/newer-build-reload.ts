/**
 * What a tab does when a newer build of the app wants its local-first store
 * (see `BrowserConnectionManager.yieldToNewerBuild`): reload, so it runs the
 * newer build too, unless the app registered its own `onNewerBuild` (a
 * `createJazzClient` option), which gets the reload to call when it suits it
 * (say, after a "new version" notice). One handler per page: reloading is
 * the page's.
 */

/** The `createJazzClient` option: call `reload()` to load the newer build. */
export type NewerBuildHandler = (reload: () => void) => void;

const RELOAD_MARKER = "jazz:newer-build-reload";
/** A page that reloads for a newer build and still finds itself older within this window (a CDN serving the old build) doesn't reload again. */
const RELOAD_LOOP_WINDOW_MS = 10_000;

let handler: NewerBuildHandler | undefined;

/** @internal Register the page's handler (`undefined`: reload). */
export function setNewerBuildHandler(next: NewerBuildHandler | undefined): void {
  handler = next;
}

type ReloadContext = {
  now?: () => number;
  storage?: Pick<Storage, "getItem" | "setItem">;
  reload?: () => void;
};

/**
 * Whether this page may reload for a newer build now: not when it already
 * did moments ago (it loaded an older build again, so reloading again would
 * loop). Without sessionStorage it may.
 */
export function newerBuildReloadAllowed(context: ReloadContext = {}): boolean {
  const now = (context.now ?? Date.now)();
  try {
    const storage = context.storage ?? globalThis.sessionStorage;
    const previous = Number(storage.getItem(RELOAD_MARKER) ?? 0);
    return !(Number.isFinite(previous) && now - previous < RELOAD_LOOP_WINDOW_MS);
  } catch {
    return true;
  }
}

/** Reload for a newer build: through the page's handler, or at once. */
export function reloadForNewerBuild(context: ReloadContext = {}): void {
  const reload = () => {
    try {
      (context.storage ?? globalThis.sessionStorage).setItem(
        RELOAD_MARKER,
        String((context.now ?? Date.now)()),
      );
    } catch {
      // Reloading still loads the newer build.
    }
    (context.reload ?? (() => globalThis.location.reload()))();
  };
  if (!handler) return reload();
  try {
    handler(reload);
  } catch (error) {
    console.warn("Jazz: onNewerBuild threw; reloading for the newer build.", error);
    reload();
  }
}
