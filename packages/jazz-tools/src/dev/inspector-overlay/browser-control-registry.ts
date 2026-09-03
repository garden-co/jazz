import type {
  BrowserInspectorContext,
  BrowserInspectorControlEvent,
  BrowserInspectorControlRequest,
} from "../../runtime/native-runtime/browser-worker-protocol.js";
import {
  deserializeBrowserRelayError,
  serializeBrowserRelayError,
} from "../../runtime/native-runtime/browser-worker-protocol.js";

type ControlPortFactory = () => Promise<MessagePort>;
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

export function registerBrowserInspectorControl(factory: ControlPortFactory): () => void {
  const state = registry();
  const id = state.nextFactoryId++;
  state.factories.set(id, factory);
  return () => state.factories.delete(id);
}

export async function openAggregatedBrowserInspectorControlPort(
  fallback: ControlPortFactory,
): Promise<MessagePort> {
  const factories = registry().factories;
  if (factories.size === 0) return fallback();
  const registrations = [...factories];
  const controls = await Promise.all(
    registrations.map(async ([id, factory]) => ({ id, port: await factory(), nextId: 1 })),
  );
  for (const control of controls) control.port.start();
  const routes = new Map<string, { port: MessagePort; contextKey: string }>();
  const channel = new MessageChannel();
  const port = channel.port1;

  const request = <T extends BrowserInspectorControlEvent>(
    control: (typeof controls)[number],
    message: ControlRequestWithoutId,
    transfer: Transferable[] = [],
  ) =>
    new Promise<T>((resolve, reject) => {
      const id = control.nextId++;
      const onMessage = (event: MessageEvent<BrowserInspectorControlEvent>) => {
        if (event.data.id !== id) return;
        control.port.removeEventListener("message", onMessage);
        if ("error" in event.data && event.data.error)
          reject(deserializeBrowserRelayError(event.data.error));
        else resolve(event.data as T);
      };
      control.port.addEventListener("message", onMessage);
      control.port.postMessage({ ...message, id }, transfer);
    });

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
      port.postMessage({
        type: "result",
        id: "id" in message ? message.id : 0,
        error: serializeBrowserRelayError(error),
      });
    }
  };
  const dispose = () => {
    port.removeEventListener("message", onMessage);
    for (const control of controls) {
      control.port.postMessage({ type: "close" });
      control.port.close();
    }
    port.close();
  };
  port.addEventListener("message", onMessage);
  port.start();
  return channel.port2;
}
