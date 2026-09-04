import type { Db, DbConfig } from "../../runtime/db.js";
import { resolveDefaultPersistentDbName, resolvePersistentDbBaseName } from "../../runtime/db.js";
import {
  ANONYMOUS_JWT_ISSUER,
  internalSessionFromVerifiedReservedJwtPayload,
  LOCAL_FIRST_JWT_ISSUER,
  parseJwtPayload,
} from "../../runtime/client-session.js";
import { getRegisteredWasmSchema } from "../../typed-app.js";
import {
  INSPECTOR_HOST_GLOBAL,
  INSPECTOR_SUBSCRIPTIONS_MESSAGE,
  serializeActiveSubscriptions,
  type JazzInspectorHost,
} from "./inspector-host-types.js";
import { openAggregatedBrowserInspectorControlPort } from "./browser-control-registry.js";
import {
  getDbInternalSession,
  setTrustedReservedSession,
} from "../../runtime/db-internal-session.js";

function overlayBrowserWorkerSession(
  config: DbConfig,
  fallback: ReturnType<typeof getDbInternalSession>,
) {
  const payload = parseJwtPayload(config.jwtToken ?? "");
  const authMode =
    payload?.iss === LOCAL_FIRST_JWT_ISSUER
      ? "local-first"
      : payload?.iss === ANONYMOUS_JWT_ISSUER
        ? "anonymous"
        : null;

  // The host's resolved reserved-issuer token is the durable source of truth
  // for an attached peer's session. Do not make the inspector handoff depend
  // on the Db's private WeakMap bookkeeping: the receiving native runtime
  // verifies the same token before admitting the peer.
  return payload && authMode
    ? internalSessionFromVerifiedReservedJwtPayload(payload, authMode)
    : fallback;
}

/**
 * Build the ready-to-use browser config in the host bundle, where the host's
 * logical storage base and worker URL are known. The overlay resolves the
 * physical coordinate once from that base instead of re-deriving a physical
 * host name as though it were another caller-supplied base.
 */
function buildOverlayDbConfig(
  config: DbConfig,
  session: ReturnType<typeof getDbInternalSession>,
): DbConfig {
  const browserWorkerSession = overlayBrowserWorkerSession(config, session);
  const identityCredential = config.jwtToken
    ? { jwtToken: config.jwtToken }
    : config.secret
      ? { secret: config.secret }
      : config.cookieSession
        ? { cookieSession: config.cookieSession }
        : {};

  const physicalConfig: DbConfig = {
    appId: config.appId,
    serverUrl: config.serverUrl,
    env: config.env,
    ...identityCredential,
    ...(config.adminSecret ? { adminSecret: config.adminSecret } : {}),
    // `persistent` selects the SharedWorker connection so this client joins
    // the host's IndexedDB-backed runtime. Pass the original logical base so
    // the usual app/environment/auth derivation yields that exact same root.
    driver: { type: "persistent", dbName: resolvePersistentDbBaseName(config) },
  };
  // The handle is a same-origin capability created by an already admitted Db.
  // Derive the exact auth-scoped physical root in the host realm, where the
  // verified reserved session is available. A child iframe has a separate
  // module realm and cannot observe this WeakMap capability itself.
  setTrustedReservedSession(physicalConfig, browserWorkerSession);
  const inspectorHostPhysicalDbName = resolveDefaultPersistentDbName(physicalConfig);
  return {
    ...physicalConfig,
    ...(browserWorkerSession
      ? {
          runtimeSources: {
            browserWorkerSession: structuredClone(browserWorkerSession),
            inspectorHostPhysicalDbName,
          },
        }
      : { runtimeSources: { inspectorHostPhysicalDbName } }),
  };
}

/**
 * Publish the same-origin host handle and the one-way active-subscription feed.
 * No live Db crosses the iframe boundary and no devtools protocol is involved.
 */
export function installInspectorHost(
  db: Db,
  iframeWindow: Window,
  origin: string,
  inspectorWindows = new Set<Window>(),
): () => void {
  db.setDevMode(true);
  inspectorWindows.add(iframeWindow);

  const handle: JazzInspectorHost = {
    getConnectionConfig() {
      return buildOverlayDbConfig(db.getConfig(), getDbInternalSession(db));
    },
    openControlPort() {
      return openAggregatedBrowserInspectorControlPort(() => db.openInspectorControlPort());
    },
    getWasmSchema() {
      const live = db.getRuntimeSchema();
      if (live) return live;
      const registered = getRegisteredWasmSchema();
      if (registered) return registered;
      throw new Error("Inspector: no schema available — no client and no defineApp() yet.");
    },
    getActiveSubscriptions() {
      return serializeActiveSubscriptions(db.getActiveQuerySubscriptions());
    },
    registerInspectorWindow(target) {
      inspectorWindows.add(target);
    },
    unregisterInspectorWindow(target) {
      inspectorWindows.delete(target);
    },
  };
  (window as unknown as Record<string, unknown>)[INSPECTOR_HOST_GLOBAL] = handle;

  const push = () => {
    const message = {
      type: INSPECTOR_SUBSCRIPTIONS_MESSAGE,
      list: handle.getActiveSubscriptions(),
    };
    for (const target of inspectorWindows) {
      try {
        if (target.closed) inspectorWindows.delete(target);
        else target.postMessage(message, origin);
      } catch {
        inspectorWindows.delete(target);
      }
    }
  };
  // Db currently invokes this listener synchronously with its initial snapshot.
  // Keep the host bridge correct if that delivery is ever deferred: a detached
  // inspector retained across a host rebind must see the new Db immediately,
  // rather than waiting for its next subscription mutation.
  let receivedInitialSnapshot = false;
  const stop = db.onActiveQuerySubscriptionsChange(() => {
    receivedInitialSnapshot = true;
    push();
  });
  if (!receivedInitialSnapshot) push();

  return () => {
    stop();
    inspectorWindows.delete(iframeWindow);
    delete (window as unknown as Record<string, unknown>)[INSPECTOR_HOST_GLOBAL];
  };
}

export type InspectorHostDb = Db;
