import * as Comlink from "comlink";
import {
  JazzClient,
  DurabilityTier,
  QueryExecutionOptions,
  QueryInput,
  Value,
  WasmSchema,
} from "../index.js";
import { Db, DbConfig } from "../runtime/db.js";
import {
  DEVTOOLS_MC_CHANNEL,
  type DevtoolsEvent,
  type DevtoolsRuntimeAPI,
  isRecord,
  isSerializableDbConfig,
  sanitizeDbConfigForBridge,
} from "./protocol.js";

type DevToolsStateListener = (connected: boolean) => void;

type RuntimeBridgeState = {
  wasmSchema: WasmSchema | null;
  dbConfig: DbConfig | null;
  connected: boolean;
  listeners: Set<DevToolsStateListener>;
  activeSubscriptions: Map<string, { client: JazzClient; runtimeSubscriptionId: number }>;
  activeQuerySubscriptionsUnsubscribe: (() => void) | null;
  messagePort: MessagePort | null;
};

export interface DevToolsAttachment {
  isConnected(): boolean;
  onConnectionChange(listener: DevToolsStateListener): () => void;
  updateSchema(schema: WasmSchema): void;
}

const runtimeBridgeStateByDb = new WeakMap<Db, RuntimeBridgeState>();
const registeredRuntimeBridgeDbs = new WeakSet<Db>();

function notifyConnectionListeners(state: RuntimeBridgeState): void {
  for (const listener of state.listeners) {
    listener(state.connected);
  }
}

function setRuntimeBridgeConnected(db: Db, connected: boolean): void {
  const state = runtimeBridgeStateByDb.get(db);
  if (!state) return;
  if (state.connected === connected) return;
  state.connected = connected;
  notifyConnectionListeners(state);
}

function updateRuntimeBridgeSchema(db: Db, wasmSchema: WasmSchema): void {
  const state = runtimeBridgeStateByDb.get(db);
  if (!state) return;
  state.wasmSchema = wasmSchema;
}

function updateRuntimeBridgeConfig(db: Db, dbConfig: DbConfig): void {
  const state = runtimeBridgeStateByDb.get(db);
  if (!state) return;
  state.dbConfig = dbConfig;
}

function clearRuntimeBridgeSubscriptions(db: Db): void {
  const state = runtimeBridgeStateByDb.get(db);
  if (!state) return;
  for (const subscription of state.activeSubscriptions.values()) {
    try {
      subscription.client.unsubscribe(subscription.runtimeSubscriptionId);
    } catch {
      // Ignore cleanup failures during bridge teardown.
    }
  }
  state.activeSubscriptions.clear();
}

function getFirstDbClient(db: Db): JazzClient | null {
  const maybeClients = (db as unknown as { clients?: unknown }).clients;
  if (!(maybeClients instanceof Map)) return null;
  const firstClient = maybeClients.values().next().value;
  return firstClient ? (firstClient as JazzClient) : null;
}

function tryGetSchemaFromDb(db: Db): WasmSchema | null {
  const firstClient = getFirstDbClient(db);
  if (!firstClient) return null;
  return firstClient.getSchema();
}

function tryCreateClientForSchema(db: Db, schema: WasmSchema): JazzClient | null {
  const maybeGetClient = (db as unknown as { getClient?: (schemaArg: WasmSchema) => JazzClient })
    .getClient;
  if (typeof maybeGetClient !== "function") return null;
  try {
    return maybeGetClient.call(db, schema);
  } catch {
    return null;
  }
}

function resolveBridgeSchema(db: Db): WasmSchema | null {
  const state = runtimeBridgeStateByDb.get(db);
  return state?.wasmSchema ?? tryGetSchemaFromDb(db) ?? null;
}

function resolveBridgeDbConfig(db: Db): DbConfig | null {
  const state = runtimeBridgeStateByDb.get(db);
  const stateConfig = sanitizeDbConfigForBridge(state?.dbConfig ?? null);
  if (stateConfig) return stateConfig;
  const rawConfig = (db as unknown as { config?: unknown }).config;
  if (!isSerializableDbConfig(rawConfig)) return null;
  return sanitizeDbConfigForBridge(rawConfig);
}

async function resolveCommandClient(db: Db): Promise<JazzClient> {
  const schema = resolveBridgeSchema(db);
  if (schema) tryCreateClientForSchema(db, schema);
  const client = getFirstDbClient(db);
  if (!client) throw new Error("No Jazz runtime client is initialized yet.");
  const ensureBridgeReady = (db as unknown as { ensureBridgeReady?: () => Promise<void> })
    .ensureBridgeReady;
  if (typeof ensureBridgeReady === "function") await ensureBridgeReady.call(db);
  return client;
}

function sendEvent(port: MessagePort, event: DevtoolsEvent): void {
  port.postMessage(event);
}

function createRuntimeAPI(
  db: Db,
  state: RuntimeBridgeState,
  port: MessagePort,
): DevtoolsRuntimeAPI {
  return {
    async announce() {
      let schema = resolveBridgeSchema(db);
      if (!schema && state.wasmSchema) schema = state.wasmSchema;
      const dbConfig = resolveBridgeDbConfig(db);

      if (schema && dbConfig) {
        updateRuntimeBridgeSchema(db, schema);
        updateRuntimeBridgeConfig(db, dbConfig);
        tryCreateClientForSchema(db, schema);
        const runtimeReady = Boolean(getFirstDbClient(db));

        // Set up active query subscription push on first announce
        if (!state.activeQuerySubscriptionsUnsubscribe) {
          state.activeQuerySubscriptionsUnsubscribe = db.onActiveQuerySubscriptionsChange(
            (subscriptions) => {
              sendEvent(port, {
                type: "active-query-subscriptions-changed",
                subscriptions: subscriptions.map((s) => ({ ...s, branches: [...s.branches] })),
              });
            },
          );
        }

        setRuntimeBridgeConnected(db, true);
        return { ready: runtimeReady, wasmSchema: schema, dbConfig };
      }
      return { ready: false };
    },

    async query(query, options) {
      const client = await resolveCommandClient(db);
      return await client.query(query, options);
    },

    async insertDurable(table, values, tier) {
      const client = await resolveCommandClient(db);
      return await client.createDurable(table, values, tier ? { tier } : undefined);
    },

    async updateDurable(objectId, updates, tier) {
      const client = await resolveCommandClient(db);
      await client.updateDurable(objectId, updates, tier ? { tier } : undefined);
    },

    async deleteDurable(objectId, tier) {
      const client = await resolveCommandClient(db);
      await client.deleteDurable(objectId, tier ? { tier } : undefined);
    },

    async subscribe(query, subscriptionId, options) {
      const client = await resolveCommandClient(db);

      // Clean up prior subscription with the same ID
      const prior = state.activeSubscriptions.get(subscriptionId);
      if (prior) {
        prior.client.unsubscribe(prior.runtimeSubscriptionId);
        state.activeSubscriptions.delete(subscriptionId);
      }

      const runtimeSubscriptionId = client.subscribe(
        query,
        (delta) => {
          sendEvent(port, { type: "subscription-delta", subscriptionId, delta });
        },
        options,
      );
      state.activeSubscriptions.set(subscriptionId, { client, runtimeSubscriptionId });
    },

    async unsubscribe(subscriptionId) {
      const activeSubscription = state.activeSubscriptions.get(subscriptionId);
      if (activeSubscription) {
        activeSubscription.client.unsubscribe(activeSubscription.runtimeSubscriptionId);
        state.activeSubscriptions.delete(subscriptionId);
      }
    },

    async listActiveQuerySubscriptions() {
      return db.getActiveQuerySubscriptions();
    },
  };
}

function hookRegistration(
  db: Db,
  options?: { wasmSchema?: WasmSchema; dbConfig?: DbConfig },
): DevToolsAttachment {
  let state = runtimeBridgeStateByDb.get(db);
  if (!state) {
    state = {
      wasmSchema: options?.wasmSchema ?? null,
      dbConfig: sanitizeDbConfigForBridge(options?.dbConfig ?? null),
      connected: false,
      listeners: new Set(),
      activeSubscriptions: new Map(),
      activeQuerySubscriptionsUnsubscribe: null,
      messagePort: null,
    };
    runtimeBridgeStateByDb.set(db, state);
  } else {
    if (options?.wasmSchema) state.wasmSchema = options.wasmSchema;
    if (options?.dbConfig) state.dbConfig = sanitizeDbConfigForBridge(options.dbConfig);
  }

  if (!registeredRuntimeBridgeDbs.has(db) && typeof window !== "undefined") {
    registeredRuntimeBridgeDbs.add(db);

    const teardownComlink = () => {
      clearRuntimeBridgeSubscriptions(db);
      if (state!.activeQuerySubscriptionsUnsubscribe) {
        state!.activeQuerySubscriptionsUnsubscribe();
        state!.activeQuerySubscriptionsUnsubscribe = null;
      }
      if (state!.messagePort) {
        state!.messagePort.close();
        state!.messagePort = null;
      }
      setRuntimeBridgeConnected(db, false);
    };

    const setupComlink = () => {
      // Clean up previous connection
      teardownComlink();

      const channel = new MessageChannel();
      state!.messagePort = channel.port1;

      const api = createRuntimeAPI(db, state!, channel.port1);
      Comlink.expose(api, channel.port1);

      window.postMessage({ channel: DEVTOOLS_MC_CHANNEL }, "*", [channel.port2]);
    };

    // Listen for content script requesting a new connection
    window.addEventListener("message", (event) => {
      if (event.source !== window) return;
      const data = event.data;
      if (!isRecord(data)) return;
      if (data.channel === DEVTOOLS_MC_CHANNEL && data.kind === "request-port") {
        setupComlink();
      }
    });

    // Initial setup
    setupComlink();
  }

  return {
    isConnected() {
      return runtimeBridgeStateByDb.get(db)?.connected ?? false;
    },
    onConnectionChange(listener) {
      const runtimeState = runtimeBridgeStateByDb.get(db);
      if (!runtimeState) return () => undefined;
      runtimeState.listeners.add(listener);
      listener(runtimeState.connected);
      return () => {
        runtimeState.listeners.delete(listener);
      };
    },
    updateSchema(schema) {
      updateRuntimeBridgeSchema(db, schema);
      tryCreateClientForSchema(db, schema);
    },
  };
}

function resolveDb(input: Db | { db: Db }): Db {
  if (input instanceof Db) return input;
  return input.db;
}

export async function attachDevTools(
  clientOrDb: Promise<{ db: Db }> | { db: Db } | Db,
  wasmSchema: WasmSchema,
): Promise<DevToolsAttachment> {
  const resolved = await Promise.resolve(clientOrDb as Promise<{ db: Db }> | { db: Db } | Db);
  const db = resolveDb(resolved as Db | { db: Db });
  db.setDevMode(true);
  const dbConfig = resolveBridgeDbConfig(db);
  return hookRegistration(db, { wasmSchema, dbConfig: dbConfig ?? undefined });
}
