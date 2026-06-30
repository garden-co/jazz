import { resolveBrokerWorkerUrl } from "../../runtime/browser-broker-client.js";
import type { Db } from "../../runtime/db.js";
import { resolveDefaultPersistentDbName } from "../../runtime/db.js";
import { getRegisteredWasmSchema } from "../../typed-app.js";
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
        // The *resolved* OPFS namespace (e.g. `appId::user_id` for an
        // authenticated session), not the raw `c.dbName` which is usually unset.
        // Resolved in the host bundle so it matches the host Db's own store.
        dbName: resolveDefaultPersistentDbName(c),
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
      // The live client's schema is authoritative when a client exists (it's
      // per-db and engine-normalized). Before any query has created one — e.g. a
      // write-only page (useDb/insert, no useAll) — fall back to the statically-
      // registered app schema (known at defineApp time).
      try {
        return db.getRuntimeSchema();
      } catch {
        const registered = getRegisteredWasmSchema();
        if (registered) return registered;
        throw new Error("Inspector: no schema available — no client and no defineApp() yet.");
      }
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
