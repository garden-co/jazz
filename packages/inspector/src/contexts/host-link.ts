import { useEffect, useState } from "react";
import {
  INSPECTOR_HOST_GLOBAL,
  INSPECTOR_SUBSCRIPTIONS_MESSAGE,
  type DbConfig,
  type InspectorSubscription,
  type InspectorSubscriptionsMessage,
  type JazzInspectorHost,
  type WasmSchema,
} from "jazz-tools";

/**
 * Reads the host handle the overlay loader publishes on the parent window
 * (`window.__jazzInspectorHost`). Same-origin only; returns null in the
 * standalone build (no parent) or if reading the parent throws.
 */
function readHost(): JazzInspectorHost | null {
  try {
    const host = (window.parent as unknown as Record<string, unknown>)[INSPECTOR_HOST_GLOBAL];
    return (host as JazzInspectorHost | undefined) ?? null;
  } catch {
    return null;
  }
}

export function readInspectorHostConfig(): DbConfig | null {
  const host = readHost();
  if (!host) return null;
  // The overlay client connects through the host's own subscription channel
  // (same-origin realm call), so it never opens storage, a worker, or a server
  // connection of its own — and sees exactly the host's local store.
  return {
    ...host.getConnectionConfig(),
    subscriptionChannel: host.getSubscriptionChannel(),
  };
}

export function readInspectorHostSchema(): WasmSchema | null {
  const host = readHost();
  if (!host) return null;
  try {
    return host.getWasmSchema();
  } catch {
    // getWasmSchema throws while no schema exists anywhere yet (no client and
    // no defineApp) — treat that as "not ready" and let the poll retry.
    return null;
  }
}

/**
 * The host app's active subscriptions. Seeds from the handle (so the initial
 * state isn't lost to the push race) and updates from the one-way push.
 */
export function useHostSubscriptions(): InspectorSubscription[] {
  const [list, setList] = useState<InspectorSubscription[]>(
    () => readHost()?.getActiveSubscriptions() ?? [],
  );

  useEffect(() => {
    const onMessage = (event: MessageEvent) => {
      // The host handle push always comes from the parent window (same-origin,
      // per readHost() above) — mirror the deleted bridge's event.source guard
      // so an unrelated same-origin frame/tab can't spoof a subscription push.
      if (event.source !== window.parent) return;
      const data = event.data as InspectorSubscriptionsMessage | undefined;
      if (data?.type === INSPECTOR_SUBSCRIPTIONS_MESSAGE && Array.isArray(data.list)) {
        setList(data.list);
      }
    };
    window.addEventListener("message", onMessage);
    // Re-read in case a push landed between initial render and listener attach.
    const current = readHost()?.getActiveSubscriptions();
    if (current) setList(current);
    return () => window.removeEventListener("message", onMessage);
  }, []);

  return list;
}
