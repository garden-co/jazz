import { resolveBrokerWorkerUrl } from "../../runtime/browser-broker-client.js";
import type { Db, DbConfig } from "../../runtime/db.js";
import { resolveDefaultPersistentDbName } from "../../runtime/db.js";
import { getRegisteredWasmSchema } from "../../typed-app.js";
import {
  INSPECTOR_HOST_GLOBAL,
  INSPECTOR_SUBSCRIPTIONS_MESSAGE,
  serializeActiveSubscriptions,
  type JazzInspectorHost,
} from "./inspector-host-types.js";

/**
 * Build the config the overlay's own worker client connects with. Derived here,
 * in the host bundle, so the config the overlay gets is already valid and
 * already resolved — the overlay passes it to its provider verbatim instead of
 * re-encoding jazz-tools' credential/resolution rules in a second package.
 */
function buildOverlayDbConfig(c: DbConfig): DbConfig {
  // Pass exactly one identity credential — secret/jwtToken/cookieSession are
  // mutually exclusive, and a local-first host carries both a secret and a
  // derived jwtToken. Use the host's *identity* (live session → seed) so the
  // overlay is the same user as the host and reads its local store. adminSecret
  // is independent of identity (not mutually exclusive with it) and, when
  // present, always wins the broker's authClass fingerprint — see
  // resolveBrokerAuthClass — so it must always be forwarded when the host has
  // one, regardless of which identity credential is also set.
  const identityCredential = c.jwtToken
    ? { jwtToken: c.jwtToken }
    : c.secret
      ? { secret: c.secret }
      : c.cookieSession
        ? { cookieSession: c.cookieSession }
        : {};
  return {
    appId: c.appId,
    /* Optional — the overlay can run purely on local storage when offline. */
    serverUrl: c.serverUrl,
    env: c.env,
    userBranch: c.userBranch,
    ...identityCredential,
    ...(c.adminSecret ? { adminSecret: c.adminSecret } : {}),
    // Join the host's persistent store: the *resolved* OPFS namespace (e.g.
    // `appId::user_id` for an authenticated session, not the raw `c.dbName`
    // which is usually unset) and the exact broker SharedWorker URL the host's
    // own broker was constructed with (same `(url, name)` joins instead of
    // spawning an empty one). This is how the overlay sees the host's local
    // data — including unsynced local-only rows — and works offline.
    driver: { type: "persistent", dbName: resolveDefaultPersistentDbName(c) },
    runtimeSources: { brokerWorkerUrl: resolveBrokerWorkerUrl(c.runtimeSources) },
  };
}

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
      return buildOverlayDbConfig(db.getConfig());
    },
    getWasmSchema() {
      // The live client's schema is authoritative when a client exists (it's
      // per-db and engine-normalized). Before any query has created one — e.g. a
      // write-only page (useDb/insert, no useAll) — fall back to the statically-
      // registered app schema (known at defineApp time).
      const live = db.getRuntimeSchema();
      if (live) return live;
      const registered = getRegisteredWasmSchema();
      if (registered) return registered;
      throw new Error("Inspector: no schema available — no client and no defineApp() yet.");
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
        list: handle.getActiveSubscriptions(),
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
