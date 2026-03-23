import * as Comlink from "comlink";
import {
  ActiveQuerySubscriptionTrace,
  JazzClient,
  DurabilityTier,
  QueryExecutionOptions,
  QueryInput,
  RequestLike,
  Row,
  Runtime,
  Session,
  SessionClient,
  SubscriptionCallback,
  Value,
  WasmModule,
  WasmSchema,
} from "../index.js";
import { Db, DbConfig } from "../runtime/db.js";
import { resolveLocalAuthDefaults } from "../runtime/local-auth.js";
import {
  DEVTOOLS_MC_CHANNEL,
  DEVTOOLS_PORT_NAME,
  type DevToolsBootstrap,
  type DevtoolsEvent,
  type DevtoolsRuntimeAPI,
  isRecord,
  isSerializableDbConfig,
  sanitizeDbConfigForBridge,
} from "./protocol.js";

const ANNOUNCE_POLL_INTERVAL_MS = 500;

type DevToolsPortListener = () => void;
type ActiveQuerySubscriptionsListener = (
  subscriptions: readonly ActiveQuerySubscriptionTrace[],
) => void;

const devtoolsPortDisconnectListeners = new Set<DevToolsPortListener>();
const devtoolsPortConnectListeners = new Set<DevToolsPortListener>();
const activeQuerySubscriptionsListeners = new Set<ActiveQuerySubscriptionsListener>();

let devtoolsPort: any | null = null;
let runtimeProxy: Comlink.Remote<DevtoolsRuntimeAPI> | null = null;
let announcedBootstrap: DevToolsBootstrap | null = null;
let announcePromise: Promise<DevToolsBootstrap> | null = null;
const pendingSubscriptionCallbacks = new Map<string, SubscriptionCallback>();
const pendingSubscriptionBridgeIds = new Map<number, string>();
let nextSubscriptionHandle = 1;
let activeQuerySubscriptions: ActiveQuerySubscriptionTrace[] = [];

function cloneActiveQuerySubscriptions(
  subscriptions: readonly ActiveQuerySubscriptionTrace[],
): ActiveQuerySubscriptionTrace[] {
  return subscriptions.map((s) => ({ ...s, branches: [...s.branches] }));
}

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function notifyDevtoolsPortConnected(): void {
  for (const listener of devtoolsPortConnectListeners) listener();
}

function notifyDevtoolsPortDisconnected(): void {
  for (const listener of devtoolsPortDisconnectListeners) listener();
}

function notifyActiveQuerySubscriptionsChanged(): void {
  const snapshot = cloneActiveQuerySubscriptions(activeQuerySubscriptions);
  for (const listener of activeQuerySubscriptionsListeners) listener(snapshot);
}

function getRuntimeLastErrorMessage(): string | null {
  const global = globalThis as any;
  const message = global?.chrome?.runtime?.lastError?.message;
  return typeof message === "string" ? message : null;
}

function isMissingReceivingEndError(error: unknown): boolean {
  if (!(error instanceof Error)) return false;
  return error.message.includes("Receiving end does not exist");
}

function chromePortEndpoint(port: any): Comlink.Endpoint {
  const listeners = new Map<EventListenerOrEventListenerObject, (data: unknown) => void>();
  return {
    postMessage: (msg: unknown) => port.postMessage(msg),
    addEventListener: (_type: string, handler: EventListenerOrEventListenerObject) => {
      const wrapper = (data: unknown) => {
        if (typeof handler === "function") {
          handler({ data } as MessageEvent);
        }
      };
      listeners.set(handler, wrapper);
      port.onMessage.addListener(wrapper);
    },
    removeEventListener: (_type: string, handler: EventListenerOrEventListenerObject) => {
      const wrapper = listeners.get(handler);
      if (wrapper) {
        port.onMessage.removeListener(wrapper);
        listeners.delete(handler);
      }
    },
  };
}

function handleDevtoolsEvent(event: DevtoolsEvent): void {
  if (event.type === "subscription-delta") {
    const callback = pendingSubscriptionCallbacks.get(event.subscriptionId);
    if (callback && Array.isArray(event.delta)) {
      callback(event.delta as Parameters<SubscriptionCallback>[0]);
    }
    return;
  }

  if (event.type === "active-query-subscriptions-changed") {
    if (Array.isArray(event.subscriptions)) {
      activeQuerySubscriptions = cloneActiveQuerySubscriptions(event.subscriptions);
      notifyActiveQuerySubscriptionsChanged();
    }
    return;
  }
}

function isDevtoolsEvent(data: unknown): data is DevtoolsEvent {
  if (!isRecord(data)) return false;
  return data.type === "subscription-delta" || data.type === "active-query-subscriptions-changed";
}

function createBridgeInstallerScript(mcChannel: string, portName: string): void {
  const globalWindow = window as unknown as Record<string, unknown>;
  const globalAny = globalThis as any;
  const chromeApi = globalAny?.chrome;
  if (!chromeApi?.runtime || typeof chromeApi.runtime.onConnect?.addListener !== "function") return;
  const installMarker = "__jazzDevtoolsBridgeInstalledV2";
  if (globalWindow[installMarker]) return;
  globalWindow[installMarker] = true;

  chromeApi.runtime.onConnect.addListener((chromePort: any) => {
    if (chromePort.name !== portName) return;

    let messagePort: MessagePort | null = null;

    const onWindowMessage = (event: MessageEvent) => {
      if (event.source !== window) return;
      const data = event.data;
      if (!data || typeof data !== "object") return;
      if (data.channel !== mcChannel) return;
      if (!event.ports || event.ports.length === 0) return;

      messagePort = event.ports[0]!;

      // Relay: MessagePort -> Chrome port
      messagePort.onmessage = (msgEvent: MessageEvent) => {
        chromePort.postMessage(msgEvent.data);
      };

      // Relay: Chrome port -> MessagePort
      chromePort.onMessage.addListener((message: unknown) => {
        messagePort?.postMessage(message);
      });
    };

    window.addEventListener("message", onWindowMessage);

    // Request a fresh MessageChannel port from the runtime
    window.postMessage({ channel: mcChannel, kind: "request-port" }, "*");

    chromePort.onDisconnect.addListener(() => {
      window.removeEventListener("message", onWindowMessage);
      if (messagePort) {
        messagePort.close();
        messagePort = null;
      }
    });
  });
}

async function installBridgeInInspectedTab(chromeApi: any, tabId: number): Promise<void> {
  if (!chromeApi?.scripting || typeof chromeApi.scripting.executeScript !== "function") {
    throw new Error(
      "DevTools bridge receiver is missing and chrome.scripting.executeScript is unavailable.",
    );
  }

  await chromeApi.scripting.executeScript({
    target: { tabId, allFrames: true },
    world: "ISOLATED",
    func: createBridgeInstallerScript,
    args: [DEVTOOLS_MC_CHANNEL, DEVTOOLS_PORT_NAME],
  });
}

async function connectValidatedPort(chromeApi: any, tabId: number): Promise<any> {
  const port = chromeApi.tabs.connect(tabId, { name: DEVTOOLS_PORT_NAME });
  await new Promise<void>((resolve, reject) => {
    let settled = false;
    const timerId = window.setTimeout(() => {
      if (settled) return;
      settled = true;
      port.onDisconnect.removeListener(onDisconnect);
      resolve();
    }, 30);

    const onDisconnect = () => {
      if (settled) return;
      settled = true;
      window.clearTimeout(timerId);
      port.onDisconnect.removeListener(onDisconnect);
      const lastErrorMessage = getRuntimeLastErrorMessage();
      reject(new Error(lastErrorMessage ?? "DevTools bridge disconnected during connect."));
    };

    port.onDisconnect.addListener(onDisconnect);
  });

  return port;
}

async function ensureDevtoolsProxy(): Promise<Comlink.Remote<DevtoolsRuntimeAPI>> {
  if (runtimeProxy) return runtimeProxy;

  const global = globalThis as any;
  const chromeApi = global?.chrome;

  if (
    !chromeApi?.devtools?.inspectedWindow ||
    !chromeApi?.tabs ||
    typeof chromeApi.tabs.connect !== "function"
  ) {
    throw new Error("Chrome DevTools API is not available.");
  }

  const tabId = chromeApi.devtools.inspectedWindow.tabId;
  try {
    devtoolsPort = await connectValidatedPort(chromeApi, tabId);
  } catch (error) {
    if (!isMissingReceivingEndError(error)) throw error;
    await installBridgeInInspectedTab(chromeApi, tabId);
    devtoolsPort = await connectValidatedPort(chromeApi, tabId);
  }

  // Listen for event messages on the raw port
  devtoolsPort.onMessage.addListener((message: unknown) => {
    if (isDevtoolsEvent(message)) {
      handleDevtoolsEvent(message);
    }
    // Comlink messages are handled separately via chromePortEndpoint
  });

  runtimeProxy = Comlink.wrap<DevtoolsRuntimeAPI>(chromePortEndpoint(devtoolsPort));

  devtoolsPort.onDisconnect.addListener(() => {
    pendingSubscriptionCallbacks.clear();
    pendingSubscriptionBridgeIds.clear();
    nextSubscriptionHandle = 1;
    activeQuerySubscriptions = [];
    runtimeProxy = null;
    devtoolsPort = null;
    announcedBootstrap = null;
    announcePromise = null;
    notifyActiveQuerySubscriptionsChanged();
    notifyDevtoolsPortDisconnected();
  });

  notifyDevtoolsPortConnected();
  return runtimeProxy;
}

async function ensureDevtoolsAnnounced(): Promise<DevToolsBootstrap> {
  if (announcedBootstrap) return announcedBootstrap;
  if (announcePromise) return announcePromise;

  announcePromise = (async () => {
    while (true) {
      try {
        const proxy = await ensureDevtoolsProxy();
        const result = await proxy.announce();

        if (
          !result ||
          result.ready !== true ||
          !isRecord(result.wasmSchema) ||
          !isSerializableDbConfig(result.dbConfig)
        ) {
          await wait(ANNOUNCE_POLL_INTERVAL_MS);
          continue;
        }

        announcedBootstrap = {
          wasmSchema: result.wasmSchema as WasmSchema,
          dbConfig: sanitizeDbConfigForBridge(result.dbConfig as DbConfig)!,
        };

        activeQuerySubscriptions = cloneActiveQuerySubscriptions(
          await proxy.listActiveQuerySubscriptions(),
        );
        notifyActiveQuerySubscriptionsChanged();

        return announcedBootstrap;
      } catch {
        await wait(ANNOUNCE_POLL_INTERVAL_MS);
      }
    }
  })().finally(() => {
    announcePromise = null;
  });

  return announcePromise;
}

export async function createDbFromInspectedPage(): Promise<DevToolsDb> {
  const bootstrap = await waitForDevToolsBootstrap();
  const resolvedConfig = resolveLocalAuthDefaults(bootstrap.dbConfig);
  return new DevToolsDb(resolvedConfig, null);
}

export function getRegisteredWasmSchema(): WasmSchema | null {
  return announcedBootstrap?.wasmSchema ?? null;
}

export function getRegisteredDbConfig(): DbConfig | null {
  return announcedBootstrap?.dbConfig ?? null;
}

export async function waitForDevToolsBootstrap(): Promise<DevToolsBootstrap> {
  return ensureDevtoolsAnnounced();
}

export function onDevToolsPortDisconnect(listener: DevToolsPortListener): () => void {
  devtoolsPortDisconnectListeners.add(listener);
  return () => {
    devtoolsPortDisconnectListeners.delete(listener);
  };
}

export function onDevToolsPortConnect(listener: DevToolsPortListener): () => void {
  devtoolsPortConnectListeners.add(listener);
  return () => {
    devtoolsPortConnectListeners.delete(listener);
  };
}

export function getActiveQuerySubscriptions(): ActiveQuerySubscriptionTrace[] {
  return cloneActiveQuerySubscriptions(activeQuerySubscriptions);
}

export function onActiveQuerySubscriptionsChange(
  listener: ActiveQuerySubscriptionsListener,
): () => void {
  activeQuerySubscriptionsListeners.add(listener);
  listener(getActiveQuerySubscriptions());
  return () => {
    activeQuerySubscriptionsListeners.delete(listener);
  };
}

class DevToolsDb extends Db {
  constructor(config: DbConfig, wasmModule: WasmModule | null) {
    super(config, wasmModule);
  }

  async connectProxyRuntime(): Promise<DevToolsBootstrap> {
    return ensureDevtoolsAnnounced();
  }

  getConnectedSchema(): WasmSchema | null {
    return announcedBootstrap?.wasmSchema ?? null;
  }

  getConnectedConfig(): DbConfig | null {
    return announcedBootstrap?.dbConfig ?? null;
  }

  protected getClient(schema: WasmSchema): JazzClient {
    // @ts-expect-error proxy client intentionally implements a constrained bridge-backed surface.
    return new DevToolsJazzClient(schema);
  }
}

// @ts-expect-error
class DevToolsJazzClient implements JazzClient {
  private readonly fallbackSchema: WasmSchema;

  constructor(schema: WasmSchema) {
    this.fallbackSchema = schema;
  }

  forSession(session: Session): SessionClient {
    throw new Error("Method not implemented.");
  }
  forRequest(request: RequestLike): SessionClient {
    throw new Error("Method not implemented.");
  }
  create(table: string, values: Value[]): Row {
    throw new Error("DevTools client does not support non-durable create().");
  }
  async createDurable(
    table: string,
    values: Value[],
    options?: { tier?: DurabilityTier },
  ): Promise<Row> {
    await ensureDevtoolsAnnounced();
    const proxy = await ensureDevtoolsProxy();
    return await proxy.insertDurable(table, values, options?.tier);
  }
  async query(query: string | QueryInput, options?: QueryExecutionOptions): Promise<Row[]> {
    await ensureDevtoolsAnnounced();
    const proxy = await ensureDevtoolsProxy();
    return (await proxy.query(query, options)) as Row[];
  }
  queryInternal(
    queryJson: string,
    session?: Session,
    options?: QueryExecutionOptions,
  ): Promise<Row[]> {
    throw new Error("Method not implemented.");
  }
  update(
    objectId: string,
    updates: Record<string, Value>,
    options?: { tier?: DurabilityTier },
  ): void {
    throw new Error("DevTools client does not support non-durable update().");
  }
  async updateDurable(
    objectId: string,
    updates: Record<string, Value>,
    options?: { tier?: DurabilityTier },
  ): Promise<void> {
    await ensureDevtoolsAnnounced();
    const proxy = await ensureDevtoolsProxy();
    await proxy.updateDurable(objectId, updates, options?.tier);
  }
  delete(objectId: string, options?: { tier?: DurabilityTier }): void {
    throw new Error("DevTools client does not support non-durable delete().");
  }
  async deleteDurable(objectId: string, options?: { tier?: DurabilityTier }): Promise<void> {
    await ensureDevtoolsAnnounced();
    const proxy = await ensureDevtoolsProxy();
    await proxy.deleteDurable(objectId, options?.tier);
  }
  subscribe(
    query: string | QueryInput,
    callback: SubscriptionCallback,
    options?: QueryExecutionOptions,
  ): number {
    const handle = nextSubscriptionHandle++;
    const bridgeSubscriptionId = `sub-${handle}-${Date.now()}`;
    pendingSubscriptionCallbacks.set(bridgeSubscriptionId, callback);
    pendingSubscriptionBridgeIds.set(handle, bridgeSubscriptionId);

    void ensureDevtoolsAnnounced()
      .then(() => ensureDevtoolsProxy())
      .then((proxy) => proxy.subscribe(query, bridgeSubscriptionId, options))
      .catch(() => {
        pendingSubscriptionCallbacks.delete(bridgeSubscriptionId);
        pendingSubscriptionBridgeIds.delete(handle);
      });

    return handle;
  }
  subscribeInternal(
    query: string | QueryInput,
    callback: SubscriptionCallback,
    session?: Session,
    options?: QueryExecutionOptions,
  ): number {
    if (session)
      throw new Error("DevTools subscribe does not support session-scoped subscriptions.");
    return this.subscribe(query, callback, options);
  }
  unsubscribe(subscriptionId: number): void {
    const bridgeSubscriptionId = pendingSubscriptionBridgeIds.get(subscriptionId);
    if (!bridgeSubscriptionId) return;
    pendingSubscriptionBridgeIds.delete(subscriptionId);
    pendingSubscriptionCallbacks.delete(bridgeSubscriptionId);
    void ensureDevtoolsProxy()
      .then((proxy) => proxy.unsubscribe(bridgeSubscriptionId))
      .catch(() => undefined);
  }
  getSchema(): WasmSchema {
    if (announcedBootstrap?.wasmSchema) return announcedBootstrap.wasmSchema;
    return this.fallbackSchema;
  }
  getRuntime(): Runtime {
    throw new Error("Method not implemented.");
  }
  getServerUrl(): string | undefined {
    throw new Error("Method not implemented.");
  }
  getRequestUrl(path: string): string {
    throw new Error("Method not implemented.");
  }
  getSchemaContext(): { env: string; schema_hash: string; user_branch: string } {
    throw new Error("Method not implemented.");
  }
}
