import type { Db, DbConfig } from "../../runtime/db.js";
import { getRegisteredWasmSchema } from "../../typed-app.js";
import {
  INSPECTOR_HOST_GLOBAL,
  INSPECTOR_SUBSCRIPTIONS_MESSAGE,
  serializeActiveSubscriptions,
  type JazzInspectorHost,
} from "./inspector-host-types.js";

/**
 * Build an isolated direct-mode config for the overlay. Until tab leadership
 * is restored, the overlay must not open a second persistent worker against
 * the host's OPFS namespace.
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
    driver: { type: "memory" },
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
