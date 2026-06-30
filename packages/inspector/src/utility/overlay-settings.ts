/**
 * Settings that the inspector content (this iframe) shares with the overlay
 * chrome that hosts it. The chrome lives in jazz-tools
 * (`src/dev/inspector-overlay/loader.ts`), runs in the top window, and reads the
 * same localStorage key — both are same-origin, so the value crosses the iframe
 * boundary, and a `storage` event lets the chrome react live when this UI
 * changes it. The key string is duplicated there on purpose (the loader is a
 * deep jazz-tools internal, not a public export); keep the two in sync.
 *
 * Stored via {@link useLocalStorageState}, i.e. JSON — so the value is the
 * literal `true` / `false`, which the loader reads as `raw === "true"`.
 */
export const OVERLAY_HIDE_LAUNCHER_STORAGE_KEY = "jazz-inspector-overlay:hide-toggle";

export function isBoolean(value: unknown): value is boolean {
  return typeof value === "boolean";
}

// postMessage type the overlay chrome (jazz-tools loader.ts) listens for to
// dismiss the dock. The chrome owns no close button anymore — Close lives in the
// inspector's top bar and asks the chrome to close via this message. Duplicated
// in the loader on purpose (separate package); keep the two in sync.
const OVERLAY_CLOSE_MESSAGE_TYPE = "jazz-inspector-overlay:close";

/** Ask the overlay chrome (parent window) to close the inspector dock. */
export function requestCloseOverlay(): void {
  try {
    window.parent.postMessage({ type: OVERLAY_CLOSE_MESSAGE_TYPE }, window.location.origin);
  } catch {
    // No parent / cross-origin (e.g. standalone app) — nothing to close.
  }
}
