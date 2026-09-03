import type {
  BrowserInspectorContext,
  BrowserInspectorControlEvent,
  BrowserInspectorControlRequest,
} from "../../runtime/native-runtime/browser-worker-protocol.js";
import {
  deserializeBrowserRelayError,
  serializeBrowserRelayError,
} from "../../runtime/native-runtime/browser-worker-protocol.js";
import {
  closeInspectorControlPort,
  inspectorControlAbortError,
} from "../../runtime/native-runtime/inspector-control-lifecycle.js";

type ControlPortFactory = (signal?: AbortSignal) => Promise<MessagePort>;
type ControlRequestWithoutId = BrowserInspectorControlRequest extends infer Request
  ? Request extends { id: number }
    ? Omit<Request, "id">
    : Request
  : never;

type RegistryState = { factories: Map<number, ControlPortFactory>; nextFactoryId: number };
const REGISTRY_KEY = Symbol.for("jazz.browser-inspector-control-registry");

function registry(): RegistryState {
  const scope = globalThis as typeof globalThis & { [REGISTRY_KEY]?: RegistryState };
  return (scope[REGISTRY_KEY] ??= { factories: new Map(), nextFactoryId: 1 });
}

const CONTROL_REQUEST_TIMEOUT_MS = 4_500;

export function registerBrowserInspectorControl(factory: ControlPortFactory): () => void {
  const state = registry();
  const id = state.nextFactoryId++;
  state.factories.set(id, factory);
  return () => state.factories.delete(id);
}

export async function openAggregatedBrowserInspectorControlPort(
  fallback: ControlPortFactory,
  signal?: AbortSignal,
): Promise<MessagePort> {
  if (signal?.aborted) throw inspectorControlAbortError();
  const factories = registry().factories;
  if (factories.size === 0) return fallback(signal);

  type Control = { id: number; port: MessagePort; nextId: number };
  const controls: Control[] = [];
  let acquisitionCancelled = false;
  let rejectAcquisition: ((error: Error) => void) | undefined;
  const closeAcquiredControls = () => {
    for (const control of controls.splice(0)) closeInspectorControlPort(control.port);
  };
  const onAcquisitionAbort = () => {
    acquisitionCancelled = true;
    closeAcquiredControls();
    rejectAcquisition?.(inspectorControlAbortError());
  };
  signal?.addEventListener("abort", onAcquisitionAbort, { once: true });
  try {
    const acquisition = Promise.all(
      [...factories].map(async ([id, factory]) => {
        const port = await factory(signal);
        if (acquisitionCancelled || signal?.aborted) {
          closeInspectorControlPort(port);
          throw inspectorControlAbortError();
        }
        controls.push({ id, port, nextId: 1 });
      }),
    );
    await (signal
      ? Promise.race([
          acquisition,
          new Promise<never>((_, reject) => {
            rejectAcquisition = reject;
          }),
        ])
      : acquisition);
  } catch (error) {
    acquisitionCancelled = true;
    closeAcquiredControls();
    throw error;
  } finally {
    signal?.removeEventListener("abort", onAcquisitionAbort);
    rejectAcquisition = undefined;
  }

  for (const control of controls) control.port.start();
  const routes = new Map<string, { port: MessagePort; contextKey: string }>();
  const channel = new MessageChannel();
  const port = channel.port1;
  const pendingCancels = new Set<(error: Error) => void>();
  let disposed = false;

  const request = <T extends BrowserInspectorControlEvent>(
    control: Control,
    message: ControlRequestWithoutId,
    transfer: Transferable[] = [],
  ) =>
    new Promise<T>((resolve, reject) => {
      const id = control.nextId++;
      let timer: ReturnType<typeof setTimeout> | undefined;
      let settled = false;
      const finish = (error?: unknown, event?: BrowserInspectorControlEvent) => {
        if (settled) return;
        settled = true;
        control.port.removeEventListener("message", onMessage);
        clearTimeout(timer);
        pendingCancels.delete(cancel);
        if (error !== undefined) reject(error);
        else resolve(event as T);
      };
      const cancel = (error: Error) => finish(error);
      const onMessage = (event: MessageEvent<BrowserInspectorControlEvent>) => {
        if (event.data.id !== id) return;
        if ("error" in event.data && event.data.error) {
          finish(deserializeBrowserRelayError(event.data.error));
        } else {
          finish(undefined, event.data);
        }
      };
      pendingCancels.add(cancel);
      control.port.addEventListener("message", onMessage);
      timer = setTimeout(
        () => finish(new Error("Inspector relay control request timed out")),
        CONTROL_REQUEST_TIMEOUT_MS,
      );
      try {
        control.port.postMessage({ ...message, id }, transfer);
      } catch (error) {
        finish(error);
      }
    });

  const dispose = () => {
    if (disposed) return;
    disposed = true;
    port.removeEventListener("message", onMessage);
    for (const cancel of [...pendingCancels]) {
      cancel(new Error("Inspector aggregate control is closed"));
    }
    routes.clear();
    closeAcquiredControls();
    port.close();
  };

  const onMessage = async (event: MessageEvent<BrowserInspectorControlRequest>) => {
    const message = event.data;
    try {
      if (message.type === "close") {
        dispose();
        return;
      }
      if (message.type === "list-contexts") {
        const lists = await Promise.all(
          controls.map(async (control) => ({
            control,
            event: await request<Extract<BrowserInspectorControlEvent, { type: "contexts" }>>(
              control,
              { type: "list-contexts" },
            ),
          })),
        );
        const contexts: BrowserInspectorContext[] = [];
        const nextRoutes = new Map<string, { port: MessagePort; contextKey: string }>();
        for (const { control, event: response } of lists) {
          for (const context of response.contexts) {
            const key = `${control.id}:${context.key}`;
            nextRoutes.set(key, { port: control.port, contextKey: context.key });
            contexts.push({ ...context, key });
          }
        }
        routes.clear();
        for (const [key, route] of nextRoutes) routes.set(key, route);
        port.postMessage({ type: "contexts", id: message.id, contexts });
        return;
      }
      if (message.type === "terminate-worker") {
        throw new Error("Worker termination is only available on a direct browser control port");
      }
      if (message.type === "lifecycle-trace") {
        throw new Error(
          "Worker lifecycle trace is only available on a direct browser control port",
        );
      }
      const route = routes.get(message.contextKey);
      if (!route) throw new Error("Inspector context is no longer available");
      const control = controls.find((candidate) => candidate.port === route.port)!;
      await request(
        control,
        {
          type: "attach-context",
          contextKey: route.contextKey,
          tabId: message.tabId,
          port: message.port,
        },
        [message.port],
      );
      port.postMessage({ type: "result", id: message.id });
    } catch (error) {
      if (message.type === "attach-context") message.port.close();
      if (disposed) return;
      port.postMessage({
        type: "result",
        id: "id" in message ? message.id : 0,
        error: serializeBrowserRelayError(error),
      });
    }
  };
  port.addEventListener("message", onMessage);
  port.start();
  return channel.port2;
}
