import { useEffect, useState } from "react";
import {
  INSPECTOR_HOST_GLOBAL,
  INSPECTOR_SUBSCRIPTIONS_MESSAGE,
  deserializeInspectorControlError,
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

export interface InspectorRuntimeSessionOptions {
  /** Cancel opening before the host has returned a control port. */
  signal?: AbortSignal;
  /** Absolute epoch deadline shared by opening and the first request. */
  deadline?: number;
}

const CONTROL_REQUEST_TIMEOUT_MS = 5_000;

function abortError(): Error {
  return new Error("Inspector control session opening was cancelled");
}

function sessionClosedError(): Error {
  return new Error("Inspector control session is closed");
}

const closedRemotePorts = new WeakSet<MessagePort>();

function closeRemotePort(port: MessagePort): void {
  if (closedRemotePorts.has(port)) return;
  closedRemotePorts.add(port);
  try {
    port.postMessage({ type: "close" });
  } catch {
    // The remote endpoint may already have torn down its side of the channel.
  }
  try {
    port.close();
  } catch {
    // Closing an already unusable local endpoint is best effort.
  }
}

export function closeInspectorRuntimePort(port: MessagePort): void {
  closeRemotePort(port);
}

export async function openInspectorRuntimeSession(
  options: InspectorRuntimeSessionOptions = {},
): Promise<InspectorRuntimeSession | null> {
  const host = readHost();
  if (!host) return null;

  const { signal, deadline } = options;
  if (signal?.aborted || (deadline !== undefined && deadline <= Date.now())) {
    throw abortError();
  }

  const openingController = new AbortController();
  const opening = Promise.resolve().then(() =>
    host.handle.openControlPort(openingController.signal),
  );
  let openingSettled = false;
  let openingTimer: ReturnType<typeof setTimeout> | undefined;
  let removeAbortListener: (() => void) | undefined;
  const control = await new Promise<MessagePort>((resolve, reject) => {
    const finish = (error?: unknown, port?: MessagePort) => {
      if (openingSettled) {
        if (port) closeRemotePort(port);
        return;
      }

      if (error !== undefined) openingController.abort();
      openingSettled = true;
      clearTimeout(openingTimer);
      openingTimer = undefined;
      removeAbortListener?.();
      if (error !== undefined) reject(error);
      else resolve(port!);
    };

    opening.then(
      (port) => finish(undefined, port),
      (error) => finish(error),
    );
    const remaining = deadline === undefined ? undefined : deadline - Date.now();
    if (remaining !== undefined) {
      openingTimer = setTimeout(() => finish(abortError()), Math.max(0, remaining));
    }
    if (signal) {
      const onAbort = () => finish(abortError());
      signal.addEventListener("abort", onAbort, { once: true });
      removeAbortListener = () => signal.removeEventListener("abort", onAbort);
    }
  });

  control.start();
  let nextId = 1;
  let closed = false;
  let requestDeadline = deadline;
  type PendingRequest = {
    resolve: (value: unknown) => void;
    reject: (error: unknown) => void;
    timer: ReturnType<typeof setTimeout>;
  };
  const pending = new Map<number, PendingRequest>();
  const attachedPorts = new Set<MessagePort>();
  const onMessage = (event: MessageEvent) => {
    const message = event.data as { id?: unknown; error?: unknown } | null;
    if (!message || typeof message.id !== "number") return;
    const entry = pending.get(message.id);
    if (!entry) return;
    pending.delete(message.id);
    clearTimeout(entry.timer);
    if (message.error) entry.reject(deserializeInspectorControlError(message.error));
    else entry.resolve(message);
  };

  // The control channel has one dispatcher for the lifetime of the session.
  // Requests only add entries to the pending map, so unrelated responses cannot
  // accumulate event listeners.
  let removeSessionAbortListener: (() => void) | undefined;
  control.addEventListener("message", onMessage);

  const close = () => {
    if (closed) return;
    closed = true;
    removeSessionAbortListener?.();
    control.removeEventListener("message", onMessage);
    const error = sessionClosedError();
    for (const [id, entry] of pending) {
      pending.delete(id);
      clearTimeout(entry.timer);
      entry.reject(error);
    }
    for (const port of attachedPorts) closeRemotePort(port);
    attachedPorts.clear();
    closeRemotePort(control);
  };
  if (signal?.aborted) {
    close();
  } else if (signal) {
    const onSessionAbort = () => close();
    signal.addEventListener("abort", onSessionAbort, { once: true });
    removeSessionAbortListener = () => signal.removeEventListener("abort", onSessionAbort);
  }
  const request = <T>(message: Record<string, unknown>, transfer: Transferable[] = []) =>
    new Promise<T>((resolve, reject) => {
      if (closed) {
        reject(sessionClosedError());
        return;
      }
      const id = nextId++;
      const timer = setTimeout(
        () => {
          if (!pending.delete(id)) return;
          reject(new Error("Inspector control request timed out"));
        },
        Math.max(
          0,
          Math.min(
            CONTROL_REQUEST_TIMEOUT_MS,
            requestDeadline === undefined
              ? CONTROL_REQUEST_TIMEOUT_MS
              : requestDeadline - Date.now(),
          ),
        ),
      );
      pending.set(id, {
        resolve: resolve as (value: unknown) => void,
        reject,
        timer,
      });
      try {
        control.postMessage({ ...message, id }, transfer);
      } catch (error) {
        pending.delete(id);
        clearTimeout(timer);
        reject(error);
      }
    });

  const listContexts = async () =>
    (
      await request<{ contexts: InspectorRuntimeContext[] }>({
        type: "list-contexts",
      })
    ).contexts;

  let contexts: InspectorRuntimeContext[];
  try {
    contexts = await listContexts();
  } catch (error) {
    close();
    throw error;
  }
  requestDeadline = undefined;
  if (closed) throw sessionClosedError();
  removeSessionAbortListener?.();
  removeSessionAbortListener = undefined;
  return {
    contexts,
    listContexts,
    async attach(contextKey) {
      const channel = new MessageChannel();
      const attachedPort = channel.port1;
      attachedPorts.add(attachedPort);
      try {
        await request(
          {
            type: "attach-context",
            contextKey,
            tabId: crypto.randomUUID(),
            port: channel.port2,
          },
          [channel.port2],
        );
        attachedPorts.delete(attachedPort);
        if (closed) {
          closeRemotePort(attachedPort);
          throw sessionClosedError();
        }
        return attachedPort;
      } catch (error) {
        attachedPorts.delete(attachedPort);
        closeRemotePort(attachedPort);
        throw error;
      }
    },
    close,
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
