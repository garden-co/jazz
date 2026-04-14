import {
  ActiveQuerySubscriptionTrace,
  InsertValues,
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
import {
  DEVTOOLS_BRIDGE_CHANNEL,
  DEVTOOLS_COMMANDS,
  DEVTOOLS_EVENTS,
  DEVTOOLS_PORT_NAME,
  DevToolsBootstrap,
  DevtoolsBridgeCommand,
  DevtoolsEventEnvelope,
  DevtoolsEventPayloadByEvent,
  DevtoolsRequestPayloadByCommand,
  DevtoolsResponsePayloadByCommand,
  DevtoolsResponseEnvelope,
  isRecord,
  isSerializableDbConfig,
  sanitizeDbConfigForBridge,
} from "./protocol.js";

const REQUEST_TIMEOUT_MS = 15_000;
const ANNOUNCE_POLL_INTERVAL_MS = 500;
const ANNOUNCE_REQUEST_TIMEOUT_MS = 2_000;

type PendingRequest = {
  resolve: (value: unknown) => void;
  reject: (reason?: unknown) => void;
  timeoutId: number;
};

type DevToolsPortListener = () => void;
type ActiveQuerySubscriptionsListener = (
  subscriptions: readonly ActiveQuerySubscriptionTrace[],
) => void;

const devtoolsPortDisconnectListeners = new Set<DevToolsPortListener>();
const devtoolsPortConnectListeners = new Set<DevToolsPortListener>();
const activeQuerySubscriptionsListeners = new Set<ActiveQuerySubscriptionsListener>();

let devtoolsPort: any | null = null;
let announcedBootstrap: DevToolsBootstrap | null = null;
let announcePromise: Promise<DevToolsBootstrap> | null = null;
const pendingRequests = new Map<string, PendingRequest>();
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

function createBridgeInstallerScript(bridgeChannel: string, portName: string): void {
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

    window.postMessage(
      {
        channel: bridgeChannel,
        kind: "event",
        event: DEVTOOLS_EVENTS.CONNECTED,
      },
      "*",
    );

    const onWindowMessage = (event: MessageEvent) => {
      if (event.source !== window) return;
      const data = event.data;
      if (!data || typeof data !== "object") return;
      const envelope = data as Record<string, unknown>;
      if (envelope.channel !== bridgeChannel) return;
      if (envelope.kind !== "response" && envelope.kind !== "event") return;
      port.postMessage(data);
    };

    const onPortMessage = (message: unknown) => {
      if (!message || typeof message !== "object") return;
      const envelope = message as Record<string, unknown>;
      if (envelope.channel !== bridgeChannel) return;
      if (envelope.kind !== "request") return;
      window.postMessage(message, "*");
    };

    const dispose = () => {
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
    args: [DEVTOOLS_BRIDGE_CHANNEL, DEVTOOLS_PORT_NAME],
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

    const responseEnvelope = message as Partial<DevtoolsResponseEnvelope>;
    if (
      responseEnvelope.channel !== DEVTOOLS_BRIDGE_CHANNEL ||
      responseEnvelope.kind !== "response" ||
      typeof responseEnvelope.requestId !== "string"
    ) {
      return;
    }

    const pending = pendingRequests.get(responseEnvelope.requestId);
    if (!pending) return;
    pendingRequests.delete(responseEnvelope.requestId);
    window.clearTimeout(pending.timeoutId);

    if (!responseEnvelope.ok) {
      pending.reject(
        new Error(responseEnvelope.error?.message ?? "DevTools bridge request failed."),
      );
      return;
    }

    pending.resolve(responseEnvelope.payload);
  };

  const onDisconnect = () => {
    const global = globalThis as any;
    const runtimeLastErrorMessage = global?.chrome?.runtime?.lastError?.message;
    const lastErrorMessage =
      typeof runtimeLastErrorMessage === "string" ? ` (${runtimeLastErrorMessage})` : "";
    const error = new Error(`DevTools bridge disconnected${lastErrorMessage}.`);
    for (const pending of pendingRequests.values()) {
      window.clearTimeout(pending.timeoutId);
      pending.reject(error);
    }
    pendingRequests.clear();
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

async function sendDevtoolsRequest<TCommand extends DevtoolsBridgeCommand>(
  command: TCommand,
  payload: DevtoolsRequestPayloadByCommand[TCommand],
  timeoutMs = REQUEST_TIMEOUT_MS,
): Promise<DevtoolsResponsePayloadByCommand[TCommand]> {
  const port = await ensureDevtoolsPort();
  const requestId = randomId();
  const envelope = {
    channel: DEVTOOLS_BRIDGE_CHANNEL,
    kind: "request",
    requestId,
    command,
    payload,
  };

  return new Promise<DevtoolsResponsePayloadByCommand[TCommand]>((resolve, reject) => {
    const timeoutId = window.setTimeout(() => {
      pendingRequests.delete(requestId);
      reject(new Error(`DevTools bridge request timed out (${command}).`));
    }, timeoutMs);

    pendingRequests.set(requestId, {
      resolve: (value: unknown) => resolve(value as DevtoolsResponsePayloadByCommand[TCommand]),
      reject,
      timeoutId,
    });
    port.postMessage(envelope);
  });
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
        const result = await sendDevtoolsRequest(
          DEVTOOLS_COMMANDS.ANNOUNCE,
          {},
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
          await sendDevtoolsRequest(DEVTOOLS_COMMANDS.CLIENT_LIST_ACTIVE_QUERY_SUBSCRIPTIONS, {}),
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
  return new DevToolsDb(bootstrap.dbConfig, null);
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
  create(table: string, values: InsertValues): Row {
    throw new Error("DevTools client does not support non-durable create().");
  }
  async createDurable(
    table: string,
    values: InsertValues,
    options?: { tier?: DurabilityTier },
  ): Promise<Row> {
    await ensureDevtoolsAnnounced();
    return await sendDevtoolsRequest(DEVTOOLS_COMMANDS.CLIENT_INSERT_DURABLE, {
      table,
      values,
      tier: options?.tier,
    });
  }
  async query(query: string | QueryInput, options?: QueryExecutionOptions): Promise<Row[]> {
    await ensureDevtoolsAnnounced();
    const payload = { query, options, tier: options?.tier };
    return (await sendDevtoolsRequest(DEVTOOLS_COMMANDS.CLIENT_QUERY, payload)) as Row[];
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
    await sendDevtoolsRequest(DEVTOOLS_COMMANDS.CLIENT_UPDATE_DURABLE, {
      objectId,
      updates,
      tier: options?.tier,
    });
  }
  delete(objectId: string, options?: { tier?: DurabilityTier }): void {
    throw new Error("DevTools client does not support non-durable delete().");
  }
  async deleteDurable(objectId: string, options?: { tier?: DurabilityTier }): Promise<void> {
    await ensureDevtoolsAnnounced();
    await sendDevtoolsRequest(DEVTOOLS_COMMANDS.CLIENT_DELETE_DURABLE, {
      objectId,
      tier: options?.tier,
    });
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
        sendDevtoolsRequest(DEVTOOLS_COMMANDS.CLIENT_SUBSCRIBE, {
          query,
          options,
          tier: options?.tier,
          subscriptionId: bridgeSubscriptionId,
        }),
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

    void sendDevtoolsRequest(DEVTOOLS_COMMANDS.CLIENT_UNSUBSCRIBE, {
      subscriptionId: bridgeSubscriptionId,
    }).catch(() => undefined);
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
  getRequestUrl(path: string): string {
    throw new Error("Method not implemented.");
  }
  getSchemaContext(): { env: string; schema_hash: string; user_branch: string } {
    throw new Error("Method not implemented.");
  }
}
