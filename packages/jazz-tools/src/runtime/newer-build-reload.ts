import type { WasmSchema } from "../drivers/types.js";
import { getRuntimeSchemaCacheKey } from "../drivers/schema-wire.js";

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

/**
 * The build this tab last reloaded away from, for a newer one. A tab that
 * finds itself on that same build again (a CDN still serving it, or a
 * rollback to it) doesn't reload again in this tab's session: it waits, as
 * for a build it can't order. Keyed by build, not time, so it can't loop.
 */
const RELOAD_MARKER = "jazz:newer-build-reloaded-from";

let handler: NewerBuildHandler | undefined;

/** @internal Register the page's handler (`undefined`: reload). */
export function setNewerBuildHandler(next: NewerBuildHandler | undefined): void {
  handler = next;
}

type ReloadContext = {
  storage?: Pick<Storage, "getItem" | "setItem">;
  reload?: () => void;
};

/** A short, stable name for the build that ships `schema`: its exact schema. */
export function browserBuildId(schema: WasmSchema): string {
  // FNV-1a: sessionStorage keeps a few characters, not the whole schema.
  let hash = 0x811c9dc5;
  for (const char of getRuntimeSchemaCacheKey(schema)) {
    hash ^= char.codePointAt(0)!;
    hash = Math.imul(hash, 0x01000193) >>> 0;
  }
  return hash.toString(16);
}

/**
 * Whether this tab, on build `buildId`, may reload for a newer build: not
 * when it already reloaded away from this very build in this tab's session
 * and loaded it again (reloading again would loop). Without sessionStorage
 * it may (and can't loop more than the browser lets it).
 */
export function newerBuildReloadAllowed(buildId: string, context: ReloadContext = {}): boolean {
  try {
    return (context.storage ?? globalThis.sessionStorage).getItem(RELOAD_MARKER) !== buildId;
  } catch {
    return true;
  }
}

/** Reload away from build `buildId` for a newer one: through the page's handler, or at once. */
export function reloadForNewerBuild(buildId: string, context: ReloadContext = {}): void {
  const reload = () => {
    try {
      (context.storage ?? globalThis.sessionStorage).setItem(RELOAD_MARKER, buildId);
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
