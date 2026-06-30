import { resolveBrokerWorkerUrl } from "../../runtime/browser-broker-client.js";
import type { Db } from "../../runtime/db.js";
import {
  INSPECTOR_HOST_GLOBAL,
  INSPECTOR_SUBSCRIPTIONS_MESSAGE,
  serializeActiveSubscriptions,
  type JazzInspectorHost,
} from "./inspector-host-types.js";

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
      return {
        appId: c.appId,
        serverUrl: c.serverUrl,
        env: c.env ?? "",
        userBranch: c.userBranch,
        dbName: c.dbName,
        // The overlay joins this exact broker (same OPFS store) to see local
        // data and work offline. Resolved here, in the host bundle, so it
        // matches the URL the host's own broker was constructed with.
        brokerWorkerUrl: resolveBrokerWorkerUrl(c.runtimeSources),
        secret: c.secret,
        adminSecret: c.adminSecret,
        jwtToken: c.jwtToken,
      };
    },
    getWasmSchema() {
      return db.getRuntimeSchema();
    },
    getActiveSubscriptions() {
      return serializeActiveSubscriptions(db.getActiveQuerySubscriptions());
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
