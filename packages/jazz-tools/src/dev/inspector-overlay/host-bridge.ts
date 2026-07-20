import type { Db, DbConfig } from "../../runtime/db.js";
import type { SubscriptionChannel } from "../../runtime/subscription-channel.js";
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

function isChannelFacade(db: InspectorHostDb): db is Exclude<InspectorHostDb, Db> {
  // The facade's channel accessor is the union's actual discriminant — the
  // real Db drives its store directly and has no channel to hand out.
  return "getSubscriptionChannel" in db;
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
 * iframe. The overlay is same-origin, so it reads the config — which carries a
 * live, shutdown-masked subscription channel into the host's own store — off
 * `window.__jazzInspectorHost` and connects through it verbatim: it opens no
 * storage, no worker, and no server connection of its own, and its identity is
 * the host's (auth state comes from the shared channel). We push only a
 * stack-less subscription list (one-way, plain JSON). The host realm owns the
 * listener, so there's no dead-iframe-listener hazard and no cross-realm value
 * issue. Returns a disposer.
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

  // Masked once: the handle is polled by the overlay, and the client registry
  // dedupes on channel identity, so every read must yield the same object. A
  // sync Db is itself a valid channel target (the in-process channel is pure
  // delegation over the same surface), so one masking layer covers both arms.
  const maskedChannel = withoutShutdown(
    isChannelFacade(db) ? db.getSubscriptionChannel() : (db as unknown as SubscriptionChannel),
  );

  const handle: JazzInspectorHost = {
    getConnectionConfig() {
      // appId plus the channel is the whole contract: the overlay client reads
      // auth state, data, and writes through the channel, so no credential,
      // server URL, or storage coordinates are forwarded (and no secret is
      // parked on a window global).
      return { appId: (ownerDb ?? db).getConfig().appId, subscriptionChannel: maskedChannel };
    },
    getSubscriptionChannel() {
      return maskedChannel;
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

  if (isChannelFacade(db)) {
    // Owner resolution is async (the shared owner opens lazily); until it
    // lands, the handle serves the statically-registered schema and an empty
    // subscription list, and the overlay's poll/push paths pick up the rest.
    void db
      .getSubscriptionChannel()
      .ownerDb?.()
      .then(bindOwnerDb)
      .catch(() => {});
  } else {
    bindOwnerDb(db);
  }

  return () => {
    disposed = true;
    stopSubscriptionPush?.();
    stopSubscriptionPush = null;
    delete (window as unknown as Record<string, unknown>)[INSPECTOR_HOST_GLOBAL];
  };
}
