import type { Db } from "../../runtime/db.js";
import {
  INSPECTOR_HOST_GLOBAL,
  INSPECTOR_SUBSCRIPTIONS_MESSAGE,
  serializeActiveSubscriptions,
  type JazzInspectorHost,
} from "../../dev-tools/inspector-host-types.js";

/**
 * Publish the host handle + push the active-subscription list to the overlay
 * iframe. Replaces `attachDevTools` + the relay: the overlay is same-origin, so
 * it reads the config off `window.__jazzInspectorHost` and connects on its own,
 * while we push only a stack-less subscription list (one-way, plain JSON). The
 * host realm owns the listener, so there's no dead-iframe-listener hazard and no
 * cross-realm value issue. Returns a disposer.
 */
export function installInspectorHost(db: Db, iframeWindow: Window, origin: string): () => void {
  db.setDevMode(true);

  const handle: JazzInspectorHost = {
    getConnectionConfig() {
      const c = db.getConfig();
      if (!c.serverUrl) {
        throw new Error(
          "Inspector: the host Db has no serverUrl — the overlay needs a synced connection.",
        );
      }
      return {
        appId: c.appId,
        serverUrl: c.serverUrl,
        env: c.env ?? "",
        userBranch: c.userBranch,
        adminSecret: c.adminSecret,
        jwtToken: c.jwtToken,
      };
    },
    getWasmSchema() {
      return db.getRuntimeSchema();
    },
  };
  (window as unknown as Record<string, unknown>)[INSPECTOR_HOST_GLOBAL] = handle;

  const push = () => {
    iframeWindow.postMessage(
      {
        type: INSPECTOR_SUBSCRIPTIONS_MESSAGE,
        list: serializeActiveSubscriptions(db.getActiveQuerySubscriptions()),
      },
      origin,
    );
  };
  // onActiveQuerySubscriptionsChange invokes the listener immediately (db.ts),
  // so registering also pushes the initial snapshot.
  const stop = db.onActiveQuerySubscriptionsChange(push);

  return () => {
    stop();
    delete (window as unknown as Record<string, unknown>)[INSPECTOR_HOST_GLOBAL];
  };
}
