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
 * Build a config that lets the overlay join the host's browser broker as an
 * ordinary tab, including when the host is offline.
 */
function buildOverlayDbConfig(config: DbConfig): DbConfig {
  const identityCredential = config.jwtToken
    ? { jwtToken: config.jwtToken }
    : config.secret
      ? { secret: config.secret }
      : config.cookieSession
        ? { cookieSession: config.cookieSession }
        : {};

  return {
    appId: config.appId,
    serverUrl: config.serverUrl,
    env: config.env,
    userBranch: config.userBranch,
    ...identityCredential,
    ...(config.adminSecret ? { adminSecret: config.adminSecret } : {}),
    driver: { type: "persistent", dbName: resolveDefaultPersistentDbName(config) },
    runtimeSources: { brokerWorkerUrl: resolveBrokerWorkerUrl(config.runtimeSources) },
  };
}

/** Publish the host metadata and active-subscription feed to the overlay. */
export function installInspectorHost(db: Db, iframeWindow: Window, origin: string): () => void {
  db.setDevMode(true);

  const handle: JazzInspectorHost = {
    getConnectionConfig() {
      return buildOverlayDbConfig(db.getConfig());
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
  const stop = db.onActiveQuerySubscriptionsChange(push);

  return () => {
    stop();
    delete (window as unknown as Record<string, unknown>)[INSPECTOR_HOST_GLOBAL];
  };
}

export type InspectorHostDb = Db;
