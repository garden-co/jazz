// `jazz-tools/inspector`: the Inspector overlay for a local-first page. It
// shows the page's own Db, what this browser holds and what it hasn't synced
// yet, docked at the bottom of the page (Alt+Shift+J toggles it).
//
//   import { mountInspector } from "jazz-tools/inspector";
//   if (import.meta.env.DEV) await mountInspector(db);
//
// Importing this module is cheap and safe on a server: the overlay itself is
// a separate chunk, loaded when `mountInspector` first runs in a browser.
//
// The overlay's page comes from the app's backend, at
// `<serverUrl>/inspect/embedded.html`, which `createJazz` serves. It reads the
// Db through the window, so it sees exactly what this page's code can see;
// it never reaches the backend's Inspector API.

import type { Db } from "../runtime/db.js";
import type { OverlayOptions } from "../dev/inspector-overlay/loader.js";

export type MountInspectorOptions = OverlayOptions;

/**
 * Mount the Inspector overlay for `db` on this page. Calling it again with a
 * new Db (after a sign-in, say) rebinds the overlay to it. Rejects outside a
 * browser page, and when the overlay's page would be on another origin.
 */
export async function mountInspector(db: Db, options: MountInspectorOptions = {}): Promise<void> {
  if (typeof window === "undefined" || typeof document === "undefined") {
    throw new Error("mountInspector needs a browser page: it shows that page's Db.");
  }
  const { startInspectorOverlay } = await import("../dev/inspector-overlay/loader.js");
  startInspectorOverlay(db, options);
}
