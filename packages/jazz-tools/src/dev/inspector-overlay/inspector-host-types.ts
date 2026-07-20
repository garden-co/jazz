import type { WasmSchema } from "../../drivers/types.js";
import type { ActiveQuerySubscriptionTrace, DbConfig } from "../../runtime/db.js";
import type { SubscriptionChannel } from "../../runtime/subscription-channel.js";

/** Active subscription as sent to the overlay — the trace minus the JS stack. */
export type InspectorSubscription = Omit<ActiveQuerySubscriptionTrace, "stack">;

/** Read-once handle the host publishes on `window` for the same-origin overlay. */
export interface JazzInspectorHost {
  /**
   * A ready-to-use config for the overlay client: the app id plus the live
   * subscription channel from {@link getSubscriptionChannel}. Built entirely on
   * the host side — the overlay passes it to its provider verbatim, so it never
   * opens its own storage, worker, or server connection, and no credential is
   * exposed on the handle. No schemaHash: the host injects its runtime schema
   * directly (plain data), so the overlay skips the server schema fetch.
   */
  getConnectionConfig(): DbConfig;
  /**
   * A live subscription channel into the host's own store. The overlay is a
   * same-origin iframe, so it can call the host realm's channel directly; its
   * client plugs this into `subscriptionChannel` and thereby sees exactly what
   * the host sees — local unsynced rows included, offline included — without
   * contending for the host's OPFS/worker leadership. The channel's shutdown is
   * masked: the overlay tearing down its client must not shut the host down.
   */
  getSubscriptionChannel(): SubscriptionChannel;
  /** The host's runtime schema (plain serializable data — safe across realms). */
  getWasmSchema(): WasmSchema;
  /**
   * Current active subscriptions (stack-less) — read once on overlay mount so
   * the initial state isn't lost to the push race (the iframe's message
   * listener may not be ready when the first push fires). Live updates still
   * arrive via the `INSPECTOR_SUBSCRIPTIONS_MESSAGE` push.
   */
  getActiveSubscriptions(): InspectorSubscription[];
}

export const INSPECTOR_HOST_GLOBAL = "__jazzInspectorHost" as const;
export const INSPECTOR_SUBSCRIPTIONS_MESSAGE = "jazz-inspector:subscriptions" as const;

/** One-way host→overlay push carrying the active subscription list (no stacks). */
export interface InspectorSubscriptionsMessage {
  type: typeof INSPECTOR_SUBSCRIPTIONS_MESSAGE;
  list: InspectorSubscription[];
}

export function serializeActiveSubscriptions(
  traces: readonly ActiveQuerySubscriptionTrace[],
): InspectorSubscription[] {
  return traces.map(({ stack: _stack, ...rest }) => rest);
}
