import type { ActiveQuerySubscriptionTrace } from "../runtime/db.js";

/** Active subscription as sent to the overlay — the trace minus the JS stack. */
export type InspectorSubscription = Omit<ActiveQuerySubscriptionTrace, "stack">;

/** Connection config the host publishes for the overlay's own worker client. */
export interface InspectorConnectionConfig {
  appId: string;
  serverUrl: string;
  env: string;
  userBranch?: string;
  adminSecret?: string;
  jwtToken?: string;
  schemaHash: string;
}

/** Read-once handle the host publishes on `window` for the same-origin overlay. */
export interface JazzInspectorHost {
  getConnectionConfig(): InspectorConnectionConfig;
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
