import { useEffect, useState } from "react";
import {
  INSPECTOR_HOST_GLOBAL,
  INSPECTOR_SUBSCRIPTIONS_MESSAGE,
  type InspectorConnectionConfig,
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

export function readInspectorHostConfig(): InspectorConnectionConfig | null {
  const host = readHost();
  return host ? host.getConnectionConfig() : null;
}

export function readInspectorHostSchema(): WasmSchema | null {
  const host = readHost();
  return host ? host.getWasmSchema() : null;
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
      const data = event.data as InspectorSubscriptionsMessage | undefined;
      if (data?.type === INSPECTOR_SUBSCRIPTIONS_MESSAGE) {
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
