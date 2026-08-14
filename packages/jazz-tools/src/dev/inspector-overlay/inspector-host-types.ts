import type { WasmSchema } from "../../drivers/types.js";
import type { ActiveQuerySubscriptionTrace, DbConfig } from "../../runtime/db.js";

/** Active subscription as sent to the overlay — the trace minus the JS stack. */
export type InspectorSubscription = Omit<ActiveQuerySubscriptionTrace, "stack">;

/** Read-once handle the host publishes on `window` for the same-origin overlay. */
export interface JazzInspectorHost {
  /**
   * A ready-to-use config for the overlay's own browser client: the host's
   * identity plus the resolved persistent store coordinates, so the overlay
   * joins the host's broker SharedWorker and sees the same local data. The
   * overlay's main-thread Db remains in-memory; `driver: "persistent"` selects
   * the broker-backed browser topology.
   */
  getConnectionConfig(): DbConfig;
  /** The host's runtime schema (plain serializable data — safe across realms). */
  getWasmSchema(): WasmSchema;
  /** Current active subscriptions, without JS stacks. */
  getActiveSubscriptions(): InspectorSubscription[];
}

export const INSPECTOR_HOST_GLOBAL = "__jazzInspectorHost" as const;
export const INSPECTOR_SUBSCRIPTIONS_MESSAGE = "jazz-inspector:subscriptions" as const;

export interface InspectorSubscriptionsMessage {
  type: typeof INSPECTOR_SUBSCRIPTIONS_MESSAGE;
  list: InspectorSubscription[];
}

export function serializeActiveSubscriptions(
  traces: readonly ActiveQuerySubscriptionTrace[],
): InspectorSubscription[] {
  return traces.map(({ stack: _stack, ...rest }) => rest);
}
