import type { WasmSchema } from "../drivers/types.js";
import type { ActiveQuerySubscriptionTrace } from "../runtime/db.js";

/** Active subscription as sent to the overlay — the trace minus the JS stack. */
export type InspectorSubscription = Omit<ActiveQuerySubscriptionTrace, "stack">;

/**
 * Connection config the host publishes for the overlay's own worker client.
 * No schemaHash: the host injects its runtime schema directly (plain data),
 * so the overlay skips the server schema fetch.
 */
export interface InspectorConnectionConfig {
  appId: string;
  serverUrl: string;
  env: string;
  userBranch?: string;
  adminSecret?: string;
  jwtToken?: string;
}

/** Read-once handle the host publishes on `window` for the same-origin overlay. */
export interface JazzInspectorHost {
  getConnectionConfig(): InspectorConnectionConfig;
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
