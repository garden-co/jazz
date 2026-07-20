import type { Db, DbConfig } from "../../runtime/db.js";
import {
  createInProcessSubscriptionChannel,
  type SubscriptionChannel,
  type SubscriptionChannelTarget,
} from "../../runtime/subscription-channel.js";
import { getRegisteredWasmSchema } from "../../typed-app.js";
import {
  INSPECTOR_HOST_GLOBAL,
  INSPECTOR_SUBSCRIPTIONS_MESSAGE,
  serializeActiveSubscriptions,
  type JazzInspectorHost,
} from "./inspector-host-types.js";

/**
 * What the host side can bind. The sync client hands us the real in-page Db;
 * the async client hands us its channel facade, which lends us the shared
 * channel plus (asynchronously) the channel owner's real Db — on this branch
 * the channel owner runs in the host page, so the real Db is always reachable
 * in-realm.
 */
export type InspectorHostDb =
  | Db
  | {
      getConfig(): DbConfig;
      getSubscriptionChannel(): SubscriptionChannel;
    };

type OwnerDbCapableChannel = SubscriptionChannel & { ownerDb?(): Promise<Db> };

function isSyncDb(db: InspectorHostDb): db is Db {
  return "setDevMode" in db;
}

/**
 * Build the config the overlay client uses. Identity and app coordinates only:
 * the overlay connects through the host's own subscription channel (see
 * {@link JazzInspectorHost.getSubscriptionChannel}), so it opens no storage, no
 * worker, and no server connection of its own — which is also why no driver or
 * runtime sources are forwarded.
 */
function buildOverlayDbConfig(c: DbConfig): DbConfig {
  // Pass exactly one identity credential — secret/jwtToken/cookieSession are
  // mutually exclusive, and a local-first host carries both a secret and a
  // derived jwtToken. Use the host's *identity* (live session → seed) so the
  // overlay is the same user as the host. adminSecret is independent of
  // identity (not mutually exclusive with it), so forward it whenever the host
  // has one.
  const identityCredential = c.jwtToken
    ? { jwtToken: c.jwtToken }
    : c.secret
      ? { secret: c.secret }
      : c.cookieSession
        ? { cookieSession: c.cookieSession }
        : {};
  return {
    appId: c.appId,
    /* Display/telemetry only — the overlay never dials the server itself. */
    serverUrl: c.serverUrl,
    env: c.env,
    userBranch: c.userBranch,
    ...identityCredential,
    ...(c.adminSecret ? { adminSecret: c.adminSecret } : {}),
  };
}

/**
 * The overlay client calls `shutdown?.()` on its channel when its provider
 * unmounts — mask it so tearing down the overlay can never shut down the
 * host's store.
 */
function withoutShutdown(channel: SubscriptionChannel): SubscriptionChannel {
  return new Proxy(channel, {
    get(target, property) {
      if (property === "shutdown") return undefined;
      const value = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  }) as SubscriptionChannel;
}

/**
 * Publish the host handle + push the active-subscription list to the overlay
 * iframe. The overlay is same-origin, so it reads the config and the live
 * subscription channel off `window.__jazzInspectorHost` and connects through
 * the host's own store, while we push only a stack-less subscription list
 * (one-way, plain JSON). The host realm owns the listener, so there's no
 * dead-iframe-listener hazard and no cross-realm value issue. Returns a
 * disposer.
 */
export function installInspectorHost(
  db: InspectorHostDb,
  iframeWindow: Window,
  origin: string,
): () => void {
  let disposed = false;
  let stopSubscriptionPush: (() => void) | null = null;
  let ownerDb: Db | null = null;

  const push = () => {
    iframeWindow.postMessage(
      {
        type: INSPECTOR_SUBSCRIPTIONS_MESSAGE,
        list: handle.getActiveSubscriptions(),
      },
      origin,
    );
  };

  const bindOwnerDb = (resolved: Db) => {
    if (disposed) return;
    ownerDb = resolved;
    resolved.setDevMode(true);
    // onActiveQuerySubscriptionsChange invokes the listener immediately
    // (db.ts), so registering also pushes the initial snapshot.
    stopSubscriptionPush = resolved.onActiveQuerySubscriptionsChange(push);
  };

  const channel: SubscriptionChannel = isSyncDb(db)
    ? createInProcessSubscriptionChannel(db as unknown as SubscriptionChannelTarget)
    : db.getSubscriptionChannel();

  const config = db.getConfig();

  const handle: JazzInspectorHost = {
    getConnectionConfig() {
      return buildOverlayDbConfig(ownerDb ? ownerDb.getConfig() : config);
    },
    getSubscriptionChannel() {
      return withoutShutdown(channel);
    },
    getWasmSchema() {
      // The live client's schema is authoritative when a client exists (it's
      // per-db and engine-normalized). Before any query has created one — e.g.
      // a write-only page (useDb/insert, no useAll) — fall back to the
      // statically-registered app schema (known at defineApp time).
      const live = ownerDb?.getRuntimeSchema();
      if (live) return live;
      const registered = getRegisteredWasmSchema();
      if (registered) return registered;
      throw new Error("Inspector: no schema available — no client and no defineApp() yet.");
    },
    getActiveSubscriptions() {
      return ownerDb ? serializeActiveSubscriptions(ownerDb.getActiveQuerySubscriptions()) : [];
    },
  };
  (window as unknown as Record<string, unknown>)[INSPECTOR_HOST_GLOBAL] = handle;

  if (isSyncDb(db)) {
    bindOwnerDb(db);
  } else {
    // Owner resolution is async (the shared owner opens lazily); until it
    // lands, the handle serves the statically-registered schema and an empty
    // subscription list, and the overlay's poll/push paths pick up the rest.
    void (channel as OwnerDbCapableChannel)
      .ownerDb?.()
      .then(bindOwnerDb)
      .catch(() => {});
  }

  return () => {
    disposed = true;
    stopSubscriptionPush?.();
    stopSubscriptionPush = null;
    delete (window as unknown as Record<string, unknown>)[INSPECTOR_HOST_GLOBAL];
  };
}
