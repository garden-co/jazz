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
export const OVERLAY_SHOW_DETACH_BUTTON_STORAGE_KEY = "jazz-inspector-overlay:show-detach-button";

const IS_APPLE =
  typeof navigator !== "undefined" && /Mac|iPhone|iPad|iPod/.test(navigator.platform);
export const DETACH_SHORTCUT_KEYS = IS_APPLE ? ["⌥", "⇧", "D"] : ["Alt", "Shift", "D"];
export const DETACH_SHORTCUT_TOOLTIP = IS_APPLE
  ? "Open in separate window ⌥⇧D"
  : "Open in separate window Alt+Shift+D";

export function isBoolean(value: unknown): value is boolean {
  return typeof value === "boolean";
}

// The close message and direct overlay control are duplicated in jazz-tools'
// loader on purpose (separate package); keep them in sync.
const OVERLAY_CLOSE_MESSAGE_TYPE = "jazz-inspector-overlay:close";
const OVERLAY_CONTROL_GLOBAL = "__jazzInspectorOverlay";
export const OVERLAY_ROUTE_MESSAGE_TYPE = "jazz-inspector-overlay:route";

interface InspectorOverlayControl {
  detach(route: string): boolean;
  setActiveRoute(route: string): void;
}

export function isDetachedInspector(): boolean {
  return new URLSearchParams(window.location.search).get("detached") === "1";
}

export function requestDetachOverlay(route: string): void {
  try {
    const control = (window.parent as unknown as Record<string, unknown>)[OVERLAY_CONTROL_GLOBAL] as
      | InspectorOverlayControl
      | undefined;
    if (control?.detach(route) && document.activeElement instanceof HTMLElement) {
      document.activeElement.blur();
    }
  } catch {
    // No parent / cross-origin (e.g. standalone app) — nothing to detach.
  }
}

export function setOverlayActiveRoute(route: string): void {
  try {
    const overlayWindow = isDetachedInspector() ? window.opener : window.parent;
    const control = (overlayWindow as unknown as Record<string, unknown>)[OVERLAY_CONTROL_GLOBAL] as
      | InspectorOverlayControl
      | undefined;
    control?.setActiveRoute(route);
  } catch {
    // No parent / cross-origin (e.g. standalone app) — nothing to synchronize.
  }
}

/** Close a detached window or ask the overlay chrome to close the inspector dock. */
export function requestCloseOverlay(): void {
  try {
    if (isDetachedInspector()) {
      window.close();
      return;
    }
    window.parent.postMessage({ type: OVERLAY_CLOSE_MESSAGE_TYPE }, window.location.origin);
  } catch {
    // No parent / cross-origin (e.g. standalone app) — nothing to close.
  }
}
