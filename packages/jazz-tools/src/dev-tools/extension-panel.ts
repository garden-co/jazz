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
import { releaseProxy, type Remote, wrap } from "comlink";
import { Db, DbConfig } from "../runtime/db.js";
import { createChromeRuntimePortEndpoint } from "./comlink-endpoint.js";
import { resolveLocalAuthDefaults } from "../runtime/local-auth.js";
import {
  DEVTOOLS_BRIDGE_CHANNEL,
  DEVTOOLS_COMMANDS,
  DEVTOOLS_CONTROL_MESSAGES,
  DEVTOOLS_EVENTS,
  DEVTOOLS_PORT_NAME,
  DevToolsBootstrap,
  DevtoolsBridgeApi,
  DevtoolsEventEnvelope,
  DevtoolsEventPayloadByEvent,
  isRecord,
  isSerializableDbConfig,
  sanitizeDbConfigForBridge,
} from "./protocol.js";

const REQUEST_TIMEOUT_MS = 15_000;
const ANNOUNCE_POLL_INTERVAL_MS = 500;
const ANNOUNCE_REQUEST_TIMEOUT_MS = 2_000;

type DevToolsPortListener = () => void;
type ActiveQuerySubscriptionsListener = (
  subscriptions: readonly ActiveQuerySubscriptionTrace[],
) => void;

const devtoolsPortDisconnectListeners = new Set<DevToolsPortListener>();
const devtoolsPortConnectListeners = new Set<DevToolsPortListener>();
const activeQuerySubscriptionsListeners = new Set<ActiveQuerySubscriptionsListener>();

let devtoolsPort: any | null = null;
let devtoolsBridge: Remote<DevtoolsBridgeApi> | null = null;
let announcedBootstrap: DevToolsBootstrap | null = null;
let announcePromise: Promise<DevToolsBootstrap> | null = null;
const pendingSubscriptionCallbacks = new Map<string, SubscriptionCallback>();
const pendingSubscriptionBridgeIds = new Map<number, string>();
let nextSubscriptionHandle = 1;
let activeQuerySubscriptions: ActiveQuerySubscriptionTrace[] = [];

function cloneActiveQuerySubscriptions(
  subscriptions: readonly ActiveQuerySubscriptionTrace[],
): ActiveQuerySubscriptionTrace[] {
  return subscriptions.map((subscription) => ({
    ...subscription,
    branches: [...subscription.branches],
  }));
}

function randomId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `${Date.now()}-${Math.random().toString(36).slice(2)}`;
}

function wait(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function notifyDevtoolsPortConnected(): void {
  for (const listener of devtoolsPortConnectListeners) {
    listener();
  }
}

function notifyDevtoolsPortDisconnected(): void {
  for (const listener of devtoolsPortDisconnectListeners) {
    listener();
  }
}

function notifyActiveQuerySubscriptionsChanged(): void {
  const snapshot = cloneActiveQuerySubscriptions(activeQuerySubscriptions);
  for (const listener of activeQuerySubscriptionsListeners) {
    listener(snapshot);
  }
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

function createBridgeInstallerScript(
  bridgeChannel: string,
  portName: string,
  comlinkConnectKind: string,
  comlinkReadyKind: string,
  connectTimeoutMs: number,
  retryIntervalMs: number,
): void {
  const globalWindow = window as unknown as Record<string, unknown>;
  const globalAny = globalThis as any;
  const chromeApi = globalAny?.chrome;
  if (!chromeApi?.runtime || typeof chromeApi.runtime.onConnect?.addListener !== "function") {
    return;
  }
  const installMarker = "__jazzDevtoolsBridgeInstalledV1";
  if (globalWindow[installMarker]) return;
  globalWindow[installMarker] = true;

  chromeApi.runtime.onConnect.addListener((port: any) => {
    if (port.name !== portName) return;

    let disposed = false;
    let pagePort: MessagePort | null = null;
    let connectPromise: Promise<MessagePort> | null = null;

    const waitForRetry = (ms: number) =>
      new Promise<void>((resolve) => {
        setTimeout(resolve, ms);
      });

    window.postMessage(
      {
        channel: bridgeChannel,
        kind: "event",
        event: DEVTOOLS_EVENTS.CONNECTED,
      },
      "*",
    );

    const onPagePortMessage = (event: MessageEvent) => {
      port.postMessage(event.data);
    };

    const detachPagePort = () => {
      if (!pagePort) {
        return;
      }

      pagePort.removeEventListener("message", onPagePortMessage);
      pagePort.close();
      pagePort = null;
    };

    const ensurePagePort = async (): Promise<MessagePort> => {
      if (pagePort) {
        return pagePort;
      }

      if (connectPromise) {
        return connectPromise;
      }

      connectPromise = (async () => {
        while (!disposed) {
          const channel = new MessageChannel();
          const relayPort = channel.port1;
          const exposedPort = channel.port2;

          const connected = await new Promise<boolean>((resolve) => {
            let settled = false;

            const cleanup = () => {
              relayPort.removeEventListener("message", onReadyMessage);
              clearTimeout(timeoutId);
            };

            const onReadyMessage = (event: MessageEvent) => {
              if (settled) return;
              const data = event.data as Record<string, unknown> | null;
              if (!data || typeof data !== "object") return;
              if (data.channel !== bridgeChannel || data.kind !== comlinkReadyKind) return;
              settled = true;
              cleanup();
              resolve(true);
            };

            const timeoutId = setTimeout(() => {
              if (settled) return;
              settled = true;
              cleanup();
              relayPort.close();
              resolve(false);
            }, connectTimeoutMs);

            relayPort.addEventListener("message", onReadyMessage);
            relayPort.start?.();
            window.postMessage(
              {
                channel: bridgeChannel,
                kind: comlinkConnectKind,
              },
              "*",
              [exposedPort],
            );
          });

          if (connected) {
            pagePort = relayPort;
            pagePort.addEventListener("message", onPagePortMessage);
            pagePort.start?.();
            return pagePort;
          }

          await waitForRetry(retryIntervalMs);
        }

        throw new Error("DevTools page bridge unavailable.");
      })().finally(() => {
        connectPromise = null;
      });

      return connectPromise;
    };

    const onWindowMessage = (event: MessageEvent) => {
      if (event.source !== window) return;
      const data = event.data;
      if (!data || typeof data !== "object") return;
      const envelope = data as Record<string, unknown>;
      if (envelope.channel !== bridgeChannel) return;
      if (envelope.kind !== "event") return;
      port.postMessage(data);
    };

    const onPortMessage = (message: unknown) => {
      void ensurePagePort()
        .then((connectedPagePort) => {
          connectedPagePort.postMessage(message);
        })
        .catch(() => undefined);
    };

    const dispose = () => {
      disposed = true;
      detachPagePort();
      window.postMessage(
        {
          channel: bridgeChannel,
          kind: "event",
          event: DEVTOOLS_EVENTS.DISCONNECTED,
        },
        "*",
      );
      window.removeEventListener("message", onWindowMessage);
      port.onMessage.removeListener(onPortMessage);
      port.onDisconnect.removeListener(dispose);
    };

    window.addEventListener("message", onWindowMessage);
    port.onMessage.addListener(onPortMessage);
    port.onDisconnect.addListener(dispose);
    void ensurePagePort().catch(() => undefined);
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
    args: [
      DEVTOOLS_BRIDGE_CHANNEL,
      DEVTOOLS_PORT_NAME,
      DEVTOOLS_CONTROL_MESSAGES.COMLINK_CONNECT,
      DEVTOOLS_CONTROL_MESSAGES.COMLINK_READY,
      ANNOUNCE_REQUEST_TIMEOUT_MS,
      ANNOUNCE_POLL_INTERVAL_MS,
    ],
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

async function ensureDevtoolsPort(): Promise<any> {
  if (devtoolsPort) {
    return devtoolsPort;
  }

  const global = globalThis as any;
  const chromeApi = global?.chrome;

  if (
    !chromeApi ||
    !chromeApi.devtools ||
    !chromeApi.devtools.inspectedWindow ||
    !chromeApi.tabs ||
    typeof chromeApi.tabs.connect !== "function"
  ) {
    throw new Error("Chrome DevTools API is not available.");
  }

  const tabId = chromeApi.devtools.inspectedWindow.tabId;
  try {
    devtoolsPort = await connectValidatedPort(chromeApi, tabId);
  } catch (error) {
    if (!isMissingReceivingEndError(error)) {
      throw error;
    }
    await installBridgeInInspectedTab(chromeApi, tabId);
    devtoolsPort = await connectValidatedPort(chromeApi, tabId);
  }

  const onMessage = (message: unknown) => {
    if (!message || typeof message !== "object") return;

    const eventEnvelope = message as Partial<DevtoolsEventEnvelope>;
    if (
      eventEnvelope.channel === DEVTOOLS_BRIDGE_CHANNEL &&
      eventEnvelope.kind === "event" &&
      eventEnvelope.event === DEVTOOLS_EVENTS.CLIENT_SUBSCRIPTION_DELTA
    ) {
      const payload = eventEnvelope.payload as
        | DevtoolsEventPayloadByEvent[typeof DEVTOOLS_EVENTS.CLIENT_SUBSCRIPTION_DELTA]
        | undefined;
      if (!payload) {
        return;
      }
      const bridgeSubscriptionId = payload.subscriptionId;
      if (typeof bridgeSubscriptionId !== "string") {
        return;
      }
      const callback = pendingSubscriptionCallbacks.get(bridgeSubscriptionId);
      if (!callback) {
        return;
      }
      const delta = payload.delta;
      if (!Array.isArray(delta)) {
        return;
      }
      callback(delta as Parameters<SubscriptionCallback>[0]);
      return;
    }

    if (
      eventEnvelope.channel === DEVTOOLS_BRIDGE_CHANNEL &&
      eventEnvelope.kind === "event" &&
      eventEnvelope.event === DEVTOOLS_EVENTS.CLIENT_ACTIVE_QUERY_SUBSCRIPTIONS_CHANGED
    ) {
      const payload = eventEnvelope.payload as
        | DevtoolsEventPayloadByEvent[typeof DEVTOOLS_EVENTS.CLIENT_ACTIVE_QUERY_SUBSCRIPTIONS_CHANGED]
        | undefined;
      if (!payload || !Array.isArray(payload.subscriptions)) {
        return;
      }
      activeQuerySubscriptions = cloneActiveQuerySubscriptions(payload.subscriptions);
      notifyActiveQuerySubscriptionsChanged();
      return;
    }
  };

  const onDisconnect = () => {
    const global = globalThis as any;

    if (devtoolsBridge) {
      try {
        devtoolsBridge[releaseProxy]();
      } catch {
        // Ignore proxy cleanup failures during bridge teardown.
      }
    }

    devtoolsBridge = null;
    pendingSubscriptionCallbacks.clear();
    pendingSubscriptionBridgeIds.clear();
    nextSubscriptionHandle = 1;
    activeQuerySubscriptions = [];
    devtoolsPort = null;
    announcedBootstrap = null;
    announcePromise = null;
    notifyActiveQuerySubscriptionsChanged();
    notifyDevtoolsPortDisconnected();
  };

  devtoolsPort.onMessage.addListener(onMessage);
  devtoolsPort.onDisconnect.addListener(onDisconnect);
  notifyDevtoolsPortConnected();

  return devtoolsPort;
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timeoutId = window.setTimeout(() => {
      reject(new Error(`DevTools bridge request timed out (${label}).`));
    }, timeoutMs);

    promise.then(
      (value) => {
        window.clearTimeout(timeoutId);
        resolve(value);
      },
      (error) => {
        window.clearTimeout(timeoutId);
        reject(error);
      },
    );
  });
}

async function ensureDevtoolsBridge(): Promise<Remote<DevtoolsBridgeApi>> {
  if (devtoolsBridge) {
    return devtoolsBridge;
  }

  const port = await ensureDevtoolsPort();
  devtoolsBridge = wrap<DevtoolsBridgeApi>(createChromeRuntimePortEndpoint(port));
  return devtoolsBridge;
}

async function callDevtoolsBridge<TResult>(
  label: string,
  invoke: (bridge: Remote<DevtoolsBridgeApi>) => Promise<TResult>,
  timeoutMs = REQUEST_TIMEOUT_MS,
): Promise<TResult> {
  const bridge = await ensureDevtoolsBridge();
  return withTimeout(invoke(bridge), timeoutMs, label);
}

async function ensureDevtoolsAnnounced(): Promise<DevToolsBootstrap> {
  if (announcedBootstrap) {
    return announcedBootstrap;
  }
  if (announcePromise) {
    return announcePromise;
  }

  announcePromise = (async () => {
    while (true) {
      try {
        const result = await callDevtoolsBridge(
          DEVTOOLS_COMMANDS.ANNOUNCE,
          (bridge) => bridge.announce(),
          ANNOUNCE_REQUEST_TIMEOUT_MS,
        );

        if (
          !isRecord(result) ||
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
          await callDevtoolsBridge(
            DEVTOOLS_COMMANDS.CLIENT_LIST_ACTIVE_QUERY_SUBSCRIPTIONS,
            (bridge) => bridge.listActiveQuerySubscriptions(),
          ),
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

  forSession(_session: Session): SessionClient {
    throw new Error("Method not implemented.");
  }
  forRequest(_request: RequestLike): SessionClient {
    throw new Error("Method not implemented.");
  }
  create(_table: string, _values: Value[]): Row {
    throw new Error("DevTools client does not support non-durable create().");
  }
  async createDurable(
    table: string,
    values: Value[],
    options?: { tier?: DurabilityTier },
  ): Promise<Row> {
    await ensureDevtoolsAnnounced();
    return await callDevtoolsBridge(DEVTOOLS_COMMANDS.CLIENT_INSERT_DURABLE, (bridge) =>
      bridge.insertDurable({
        table,
        values,
        tier: options?.tier,
      }),
    );
  }
  async query(query: string | QueryInput, options?: QueryExecutionOptions): Promise<Row[]> {
    await ensureDevtoolsAnnounced();
    const payload = { query, options, tier: options?.tier };
    return (await callDevtoolsBridge(DEVTOOLS_COMMANDS.CLIENT_QUERY, (bridge) =>
      bridge.query(payload),
    )) as Row[];
  }
  queryInternal(
    _queryJson: string,
    _session?: Session,
    _options?: QueryExecutionOptions,
  ): Promise<Row[]> {
    throw new Error("Method not implemented.");
  }
  update(
    _objectId: string,
    _updates: Record<string, Value>,
    _options?: { tier?: DurabilityTier },
  ): void {
    throw new Error("DevTools client does not support non-durable update().");
  }
  async updateDurable(
    objectId: string,
    updates: Record<string, Value>,
    options?: { tier?: DurabilityTier },
  ): Promise<void> {
    await ensureDevtoolsAnnounced();
    await callDevtoolsBridge(DEVTOOLS_COMMANDS.CLIENT_UPDATE_DURABLE, (bridge) =>
      bridge.updateDurable({
        objectId,
        updates,
        tier: options?.tier,
      }),
    );
  }
  delete(_objectId: string, _options?: { tier?: DurabilityTier }): void {
    throw new Error("DevTools client does not support non-durable delete().");
  }
  async deleteDurable(objectId: string, options?: { tier?: DurabilityTier }): Promise<void> {
    await ensureDevtoolsAnnounced();
    await callDevtoolsBridge(DEVTOOLS_COMMANDS.CLIENT_DELETE_DURABLE, (bridge) =>
      bridge.deleteDurable({
        objectId,
        tier: options?.tier,
      }),
    );
  }
  subscribe(
    query: string | QueryInput,
    callback: SubscriptionCallback,
    options?: QueryExecutionOptions,
  ): number {
    const handle = nextSubscriptionHandle++;
    const bridgeSubscriptionId = randomId();
    pendingSubscriptionCallbacks.set(bridgeSubscriptionId, callback);
    pendingSubscriptionBridgeIds.set(handle, bridgeSubscriptionId);

    void ensureDevtoolsAnnounced()
      .then(() =>
        callDevtoolsBridge(DEVTOOLS_COMMANDS.CLIENT_SUBSCRIBE, (bridge) =>
          bridge.subscribe({
            query,
            options,
            tier: options?.tier,
            subscriptionId: bridgeSubscriptionId,
          }),
        ),
      )
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
    if (session) {
      throw new Error("DevTools subscribe does not support session-scoped subscriptions.");
    }
    return this.subscribe(query, callback, options);
  }
  unsubscribe(subscriptionId: number): void {
    const bridgeSubscriptionId = pendingSubscriptionBridgeIds.get(subscriptionId);
    if (!bridgeSubscriptionId) {
      return;
    }

    pendingSubscriptionBridgeIds.delete(subscriptionId);
    pendingSubscriptionCallbacks.delete(bridgeSubscriptionId);

    void callDevtoolsBridge(DEVTOOLS_COMMANDS.CLIENT_UNSUBSCRIBE, (bridge) =>
      bridge.unsubscribe({
        subscriptionId: bridgeSubscriptionId,
      }),
    ).catch(() => undefined);
  }
  getSchema(): WasmSchema {
    if (announcedBootstrap?.wasmSchema) {
      return announcedBootstrap.wasmSchema;
    }
    return this.fallbackSchema;
  }
  getRuntime(): Runtime {
    throw new Error("Method not implemented.");
  }
  getServerUrl(): string | undefined {
    throw new Error("Method not implemented.");
  }
  getRequestUrl(_path: string): string {
    throw new Error("Method not implemented.");
  }
  getSchemaContext(): { env: string; schema_hash: string; user_branch: string } {
    throw new Error("Method not implemented.");
  }
}
