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

export interface InspectorRuntimeContext {
  key: string;
  appId: string;
  dbName: string;
  schema: WasmSchema;
}

export interface InspectorRuntimeSession {
  contexts: InspectorRuntimeContext[];
  listContexts(): Promise<InspectorRuntimeContext[]>;
  attach(contextKey: string): Promise<MessagePort>;
  close(): void;
}

/**
 * Reads the host handle from the dock's parent or the detached window's opener.
 * Same-origin only; returns null in the standalone build.
 */
function readHost(): { handle: JazzInspectorHost; window: Window } | null {
  const candidates = [window.opener];
  try {
    candidates.push(window.opener?.parent ?? null);
  } catch {
    // A cross-origin opener may deny access to its parent.
  }
  candidates.push(window.parent);
  for (const candidate of candidates) {
    if (!candidate) continue;
    try {
      const host = (candidate as unknown as Record<string, unknown>)[INSPECTOR_HOST_GLOBAL];
      if (host) return { handle: host as JazzInspectorHost, window: candidate };
    } catch {
      // An opener candidate may cross an origin boundary.
    }
  }
  return null;
}

export function readInspectorHostConfig(): DbConfig | null {
  const host = readHost();
  return host ? host.handle.getConnectionConfig() : null;
}

export function readInspectorHostSchema(): WasmSchema | null {
  const host = readHost();
  if (!host) return null;
  try {
    return host.handle.getWasmSchema();
  } catch {
    // getWasmSchema throws while no schema exists anywhere yet (no client and
    // no defineApp) — treat that as "not ready" and let the poll retry.
    return null;
  }
}

export async function openInspectorRuntimeSession(): Promise<InspectorRuntimeSession | null> {
  const host = readHost();
  if (!host) return null;
  const control = await host.handle.openControlPort();
  control.start();
  let nextId = 1;

  const request = <T>(message: Record<string, unknown>, transfer: Transferable[] = []) =>
    new Promise<T>((resolve, reject) => {
      const id = nextId++;
      const onMessage = (event: MessageEvent) => {
        if (event.data?.id !== id) return;
        control.removeEventListener("message", onMessage);
        if (event.data.error) reject(new Error(event.data.error));
        else resolve(event.data);
      };
      control.addEventListener("message", onMessage);
      control.postMessage({ ...message, id }, transfer);
    });

  const listContexts = async () =>
    (
      await request<{ contexts: InspectorRuntimeContext[] }>({
        type: "list-contexts",
      })
    ).contexts;
  const contexts = await listContexts();
  return {
    contexts,
    listContexts,
    async attach(contextKey) {
      const channel = new MessageChannel();
      await request(
        {
          type: "attach-context",
          contextKey,
          tabId: crypto.randomUUID(),
          port: channel.port2,
        },
        [channel.port2],
      );
      return channel.port1;
    },
    close() {
      control.postMessage({ type: "close" });
      control.close();
    },
  };
}

/**
 * The host app's active subscriptions. Seeds from the handle (so the initial
 * state isn't lost to the push race) and updates from the one-way push.
 */
export function useHostSubscriptions(): InspectorSubscription[] {
  const [list, setList] = useState<InspectorSubscription[]>(
    () => readHost()?.handle.getActiveSubscriptions() ?? [],
  );

  useEffect(() => {
    const host = readHost();
    const isDetached = window.parent === window && window.opener !== null;
    if (isDetached) host?.handle.registerInspectorWindow(window);
    const onMessage = (event: MessageEvent) => {
      // Accept pushes only from the window that owns the host handle.
      if (event.origin !== window.location.origin || event.source !== host?.window) return;
      const data = event.data as InspectorSubscriptionsMessage | undefined;
      if (data?.type === INSPECTOR_SUBSCRIPTIONS_MESSAGE && Array.isArray(data.list)) {
        setList(data.list);
      }
    };
    window.addEventListener("message", onMessage);
    // Re-read in case a push landed between initial render and listener attach.
    const current = host?.handle.getActiveSubscriptions();
    if (current) setList(current);
    return () => {
      window.removeEventListener("message", onMessage);
      if (isDetached) host?.handle.unregisterInspectorWindow(window);
    };
  }, []);

  return list;
}
