import {
  ActiveQuerySubscriptionTrace,
  JazzClient,
  DurabilityTier,
  QueryExecutionOptions,
  QueryInput,
  Value,
  WasmSchema,
} from "../index.js";
import { expose } from "comlink";
import { Db, DbConfig } from "../runtime/db.js";
import {
  DEVTOOLS_BRIDGE_CHANNEL,
  DEVTOOLS_CONTROL_MESSAGES,
  DEVTOOLS_EVENTS,
  DevtoolsBridgeApi,
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

function emitActiveQuerySubscriptionsChanged(
  subscriptions: readonly ActiveQuerySubscriptionTrace[],
): void {
  if (typeof window === "undefined") {
    return;
  }

  window.postMessage(
    {
      channel: DEVTOOLS_BRIDGE_CHANNEL,
      kind: "event",
      event: DEVTOOLS_EVENTS.CLIENT_ACTIVE_QUERY_SUBSCRIPTIONS_CHANGED,
      payload: {
        subscriptions: subscriptions.map((subscription) => ({
          ...subscription,
          branches: [...subscription.branches],
        })),
      },
    },
    "*",
  );
}

function ensureActiveQuerySubscriptionBridge(db: Db): void {
  const state = runtimeBridgeStateByDb.get(db);
  if (!state || state.activeQuerySubscriptionsUnsubscribe) {
    return;
  }

  state.activeQuerySubscriptionsUnsubscribe = db.onActiveQuerySubscriptionsChange(
    (subscriptions) => {
      emitActiveQuerySubscriptionsChanged(subscriptions);
    },
  );
}

function clearActiveQuerySubscriptionBridge(db: Db): void {
  const state = runtimeBridgeStateByDb.get(db);
  if (!state?.activeQuerySubscriptionsUnsubscribe) {
    return;
  }

  state.activeQuerySubscriptionsUnsubscribe();
  state.activeQuerySubscriptionsUnsubscribe = null;
}

function getFirstDbClient(db: Db): JazzClient | null {
  const maybeClients = (db as unknown as { clients?: unknown }).clients;
  if (!(maybeClients instanceof Map)) return null;
  const firstClient = maybeClients.values().next().value;
  return firstClient ? (firstClient as JazzClient) : null;
}

function tryGetSchemaFromDb(db: Db): WasmSchema | null {
  const firstClient = getFirstDbClient(db);
  if (!firstClient) {
    return null;
  }
  return firstClient.getSchema();
}

function tryCreateClientForSchema(db: Db, schema: WasmSchema): JazzClient | null {
  const maybeGetClient = (db as unknown as { getClient?: (schemaArg: WasmSchema) => JazzClient })
    .getClient;
  if (typeof maybeGetClient !== "function") {
    return null;
  }
  try {
    return maybeGetClient.call(db, schema);
  } catch {
    return null;
  }
}

function resolveBridgeSchema(db: Db): WasmSchema | null {
  const state = runtimeBridgeStateByDb.get(db);
  const stateSchema = state?.wasmSchema ?? null;
  if (stateSchema) {
    return stateSchema;
  }

  const dbSchema = tryGetSchemaFromDb(db);
  if (dbSchema) {
    return dbSchema;
  }

  return null;
}

function resolveBridgeDbConfig(db: Db): DbConfig | null {
  const state = runtimeBridgeStateByDb.get(db);
  const stateConfig = sanitizeDbConfigForBridge(state?.dbConfig ?? null);
  if (stateConfig) {
    return stateConfig;
  }

  const rawConfig = (db as unknown as { config?: unknown }).config;
  if (!isSerializableDbConfig(rawConfig)) {
    return null;
  }
  return sanitizeDbConfigForBridge(rawConfig);
}

function emitSubscriptionDelta(subscriptionId: string, delta: unknown): void {
  window.postMessage(
    {
      channel: DEVTOOLS_BRIDGE_CHANNEL,
      kind: "event",
      event: DEVTOOLS_EVENTS.CLIENT_SUBSCRIPTION_DELTA,
      payload: {
        subscriptionId,
        delta,
      },
    },
    "*",
  );
}

async function resolveCommandClient(db: Db): Promise<JazzClient> {
  const schema = resolveBridgeSchema(db);
  if (schema) {
    tryCreateClientForSchema(db, schema);
  }

  const client = getFirstDbClient(db);
  if (!client) {
    throw new Error("No Jazz runtime client is initialized yet.");
  }

  const ensureBridgeReady = (db as unknown as { ensureBridgeReady?: () => Promise<void> })
    .ensureBridgeReady;
  if (typeof ensureBridgeReady === "function") {
    await ensureBridgeReady.call(db);
  }

  return client;
}

function createDevtoolsBridgeApi(db: Db): DevtoolsBridgeApi {
  return {
    async handshake() {
      return { ready: true };
    },
    async announce() {
      const state = runtimeBridgeStateByDb.get(db);
      let schema = resolveBridgeSchema(db);
      if (!schema && state?.wasmSchema) {
        schema = state.wasmSchema;
      }
      const dbConfig = resolveBridgeDbConfig(db);

      if (schema && dbConfig) {
        updateRuntimeBridgeSchema(db, schema);
        updateRuntimeBridgeConfig(db, dbConfig);
        tryCreateClientForSchema(db, schema);
        const runtimeReady = Boolean(getFirstDbClient(db));
        ensureActiveQuerySubscriptionBridge(db);
        setRuntimeBridgeConnected(db, true);
        return { ready: runtimeReady, wasmSchema: schema, dbConfig };
      }

      ensureActiveQuerySubscriptionBridge(db);
      setRuntimeBridgeConnected(db, true);
      return { ready: false };
    },
    async query(payload) {
      const query = payload.query;
      const tier = payload.tier as DurabilityTier | undefined;
      const options = isRecord(payload.options)
        ? (payload.options as QueryExecutionOptions)
        : tier
          ? { tier }
          : undefined;

      if (typeof query !== "string" && !isRecord(query)) {
        throw new Error("Invalid payload for client.query.");
      }

      const client = await resolveCommandClient(db);
      return await client.query(query as string | QueryInput, options);
    },
    async insertDurable(payload) {
      const table = payload.table;
      const values = payload.values;
      const tier = payload.tier as DurabilityTier | undefined;
      if (typeof table !== "string" || !Array.isArray(values)) {
        throw new Error("Invalid payload for client.insertDurable.");
      }

      const client = await resolveCommandClient(db);
      return await client.createDurable(table, values, tier ? { tier } : undefined);
    },
    async updateDurable(payload) {
      const objectId = payload.objectId;
      const updates = payload.updates;
      const tier = payload.tier as DurabilityTier | undefined;
      if (typeof objectId !== "string" || !isRecord(updates)) {
        throw new Error("Invalid payload for client.updateDurable.");
      }

      const client = await resolveCommandClient(db);
      await client.updateDurable(
        objectId,
        updates as Record<string, Value>,
        tier ? { tier } : undefined,
      );
      return { updated: true };
    },
    async deleteDurable(payload) {
      const objectId = payload.objectId;
      const tier = payload.tier as DurabilityTier | undefined;
      if (typeof objectId !== "string") {
        throw new Error("Invalid payload for client.deleteDurable.");
      }

      const client = await resolveCommandClient(db);
      await client.deleteDurable(objectId, tier ? { tier } : undefined);
      return { deleted: true };
    },
    async subscribe(payload) {
      const query = payload.query;
      const bridgeSubscriptionId = payload.subscriptionId;
      const tier = payload.tier as DurabilityTier | undefined;
      const options = isRecord(payload.options)
        ? (payload.options as QueryExecutionOptions)
        : tier
          ? { tier }
          : undefined;

      if (typeof query !== "string" && !isRecord(query)) {
        throw new Error("Invalid payload for client.subscribe.");
      }
      if (typeof bridgeSubscriptionId !== "string") {
        throw new Error("Invalid payload for client.subscribe.");
      }

      const client = await resolveCommandClient(db);
      const state = runtimeBridgeStateByDb.get(db);
      const priorSubscription = state?.activeSubscriptions.get(bridgeSubscriptionId);
      if (priorSubscription) {
        priorSubscription.client.unsubscribe(priorSubscription.runtimeSubscriptionId);
        state?.activeSubscriptions.delete(bridgeSubscriptionId);
      }

      const runtimeSubscriptionId = client.subscribe(
        query as string | QueryInput,
        (delta) => {
          emitSubscriptionDelta(bridgeSubscriptionId, delta);
        },
        options,
      );

      state?.activeSubscriptions.set(bridgeSubscriptionId, {
        client,
        runtimeSubscriptionId,
      });

      return { subscribed: true };
    },
    async unsubscribe(payload) {
      const bridgeSubscriptionId = payload.subscriptionId;
      if (typeof bridgeSubscriptionId !== "string") {
        throw new Error("Invalid payload for client.unsubscribe.");
      }

      const state = runtimeBridgeStateByDb.get(db);
      const activeSubscription = state?.activeSubscriptions.get(bridgeSubscriptionId);
      if (activeSubscription) {
        activeSubscription.client.unsubscribe(activeSubscription.runtimeSubscriptionId);
        state?.activeSubscriptions.delete(bridgeSubscriptionId);
      }

      return { unsubscribed: true };
    },
    async listActiveQuerySubscriptions() {
      ensureActiveQuerySubscriptionBridge(db);
      return db.getActiveQuerySubscriptions();
    },
  };
}

function hookRegistration(
  db: Db,
  options?: {
    wasmSchema?: WasmSchema;
    dbConfig?: DbConfig;
  },
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
    };
    runtimeBridgeStateByDb.set(db, state);
  } else {
    if (options?.wasmSchema) {
      state.wasmSchema = options.wasmSchema;
    }
    if (options?.dbConfig) {
      state.dbConfig = sanitizeDbConfigForBridge(options.dbConfig);
    }
  }

  if (!registeredRuntimeBridgeDbs.has(db) && typeof window !== "undefined") {
    registeredRuntimeBridgeDbs.add(db);

    window.addEventListener("message", (event) => {
      if (event.source !== window) return;
      const rawMessage = event.data;
      if (!isRecord(rawMessage)) return;
      if (rawMessage.channel !== DEVTOOLS_BRIDGE_CHANNEL) return;

      if (rawMessage.kind === "event") {
        const eventEnvelope = rawMessage as Partial<{ event: string }>;
        if (eventEnvelope.event === DEVTOOLS_EVENTS.CONNECTED) {
          setRuntimeBridgeConnected(db, true);
        }
        if (eventEnvelope.event === DEVTOOLS_EVENTS.DISCONNECTED) {
          clearRuntimeBridgeSubscriptions(db);
          clearActiveQuerySubscriptionBridge(db);
          setRuntimeBridgeConnected(db, false);
        }
        return;
      }
      if (rawMessage.kind === DEVTOOLS_CONTROL_MESSAGES.COMLINK_CONNECT) {
        const bridgePort = event.ports[0];
        if (!bridgePort) {
          return;
        }

        bridgePort.postMessage({
          channel: DEVTOOLS_BRIDGE_CHANNEL,
          kind: DEVTOOLS_CONTROL_MESSAGES.COMLINK_READY,
        });
        expose(createDevtoolsBridgeApi(db), bridgePort);
      }
    });
  }

  return {
    isConnected() {
      return runtimeBridgeStateByDb.get(db)?.connected ?? false;
    },
    onConnectionChange(listener) {
      const runtimeState = runtimeBridgeStateByDb.get(db);
      if (!runtimeState) {
        return () => undefined;
      }
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
  if (input instanceof Db) {
    return input;
  }
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
