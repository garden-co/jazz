import type { WasmSchema } from "../../drivers/types.js";
import type { ActiveQuerySubscriptionTrace, DbConfig } from "../../runtime/db.js";
import {
  deserializeBrowserRelayError,
  type BrowserRelayError,
} from "../../runtime/native-runtime/browser-worker-protocol.js";

/** Active subscription as sent to the overlay — the trace minus the JS stack. */
export type InspectorSubscription = Omit<ActiveQuerySubscriptionTrace, "stack">;

/** Read-once handle the host publishes on `window` for the same-origin overlay. */
export interface JazzInspectorHost {
  /**
   * A ready-to-use config for the overlay's own browser client: the host's
   * identity and logical persistent-store base. The inspector obtains its
   * actual worker peer from {@link openControlPort}; it never constructs a
   * second SharedWorker.
   */
  getConnectionConfig(): DbConfig;
  /** Open a session-scoped channel for discovering and attaching to worker contexts. */
  openControlPort(signal?: AbortSignal): Promise<MessagePort>;
  /** The host's runtime schema (plain serializable data — safe across realms). */
  getWasmSchema(): WasmSchema;
  /** Current active subscriptions, without JS stacks. */
  getActiveSubscriptions(): InspectorSubscription[];
  registerInspectorWindow(target: Window): void;
  unregisterInspectorWindow(target: Window): void;
}

export const INSPECTOR_HOST_GLOBAL = "__jazzInspectorHost" as const;
export const INSPECTOR_SUBSCRIPTIONS_MESSAGE = "jazz-inspector:subscriptions" as const;

export interface InspectorSubscriptionsMessage {
  type: typeof INSPECTOR_SUBSCRIPTIONS_MESSAGE;
  list: InspectorSubscription[];
}

export function deserializeInspectorControlError(error: unknown): Error {
  return deserializeBrowserRelayError(error as BrowserRelayError);
}

export function serializeActiveSubscriptions(
  traces: readonly ActiveQuerySubscriptionTrace[],
): InspectorSubscription[] {
  return traces.map(({ stack: _stack, ...rest }) => rest);
}
