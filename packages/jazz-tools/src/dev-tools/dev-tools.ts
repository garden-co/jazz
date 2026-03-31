import {
  ActiveQuerySubscriptionTrace,
  JazzClient,
  DurabilityTier,
  InsertValues,
  QueryExecutionOptions,
  QueryInput,
  Value,
  WasmSchema,
} from "../index.js";
import { Db, DbConfig } from "../runtime/db.js";
import {
  DEVTOOLS_BRIDGE_CHANNEL,
  DEVTOOLS_COMMANDS,
  DEVTOOLS_EVENTS,
  DevtoolsRequestEnvelope,
  DevtoolsRequestPayloadByCommand,
  DevtoolsResponseEnvelope,
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

    window.addEventListener("message", async (event) => {
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

      const envelope = rawMessage as Partial<DevtoolsRequestEnvelope>;
      if (
        envelope.kind !== "request" ||
        typeof envelope.requestId !== "string" ||
        typeof envelope.command !== "string"
      ) {
        return;
      }
      const requestId = envelope.requestId;

      const respond = (
        response: Omit<DevtoolsResponseEnvelope, "channel" | "kind" | "requestId">,
      ) => {
        window.postMessage(
          {
            channel: DEVTOOLS_BRIDGE_CHANNEL,
            kind: "response",
            requestId,
            ...response,
          } satisfies DevtoolsResponseEnvelope,
          "*",
        );
      };

      try {
        if (envelope.command === DEVTOOLS_COMMANDS.BRIDGE_HANDSHAKE) {
          respond({ ok: true, payload: { ready: true } });
          return;
        }

        if (envelope.command === DEVTOOLS_COMMANDS.ANNOUNCE) {
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
            respond({
              ok: true,
              payload: { ready: runtimeReady, wasmSchema: schema, dbConfig },
            });
          } else {
            respond({ ok: true, payload: { ready: false } });
          }
          ensureActiveQuerySubscriptionBridge(db);
          setRuntimeBridgeConnected(db, true);
          return;
        }

        if (envelope.command === DEVTOOLS_COMMANDS.CLIENT_LIST_ACTIVE_QUERY_SUBSCRIPTIONS) {
          ensureActiveQuerySubscriptionBridge(db);
          respond({
            ok: true,
            payload: db.getActiveQuerySubscriptions(),
          });
          return;
        }

        if (envelope.command === DEVTOOLS_COMMANDS.CLIENT_UNSUBSCRIBE) {
          const payload = isRecord(envelope.payload)
            ? (envelope.payload as DevtoolsRequestPayloadByCommand[typeof DEVTOOLS_COMMANDS.CLIENT_UNSUBSCRIBE])
            : ({} as Partial<
                DevtoolsRequestPayloadByCommand[typeof DEVTOOLS_COMMANDS.CLIENT_UNSUBSCRIBE]
              >);
          const bridgeSubscriptionId = payload.subscriptionId;
          if (typeof bridgeSubscriptionId !== "string") {
            throw new Error("Invalid payload for client.unsubscribe.");
          }
          const activeSubscription = state?.activeSubscriptions.get(bridgeSubscriptionId);
          if (activeSubscription) {
            activeSubscription.client.unsubscribe(activeSubscription.runtimeSubscriptionId);
            state?.activeSubscriptions.delete(bridgeSubscriptionId);
          }
          respond({ ok: true, payload: { unsubscribed: true } });
          return;
        }

        const resolveCommandClient = async (): Promise<JazzClient> => {
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
        };

        if (envelope.command === DEVTOOLS_COMMANDS.CLIENT_INSERT_DURABLE) {
          const payload = isRecord(envelope.payload)
            ? (envelope.payload as Partial<
                DevtoolsRequestPayloadByCommand[typeof DEVTOOLS_COMMANDS.CLIENT_INSERT_DURABLE]
              >)
            : {};
          const table = payload.table;
          const values = payload.values;
          const tier = payload.tier as DurabilityTier | undefined;
          if (typeof table !== "string" || !isRecord(values)) {
            throw new Error("Invalid payload for client.insertDurable.");
          }

          const client = await resolveCommandClient();
          const row = await client.createDurable(
            table,
            values as InsertValues,
            tier ? { tier } : undefined,
          );
          respond({ ok: true, payload: row });
          return;
        }

        if (envelope.command === DEVTOOLS_COMMANDS.CLIENT_UPDATE_DURABLE) {
          const payload = isRecord(envelope.payload)
            ? (envelope.payload as Partial<
                DevtoolsRequestPayloadByCommand[typeof DEVTOOLS_COMMANDS.CLIENT_UPDATE_DURABLE]
              >)
            : {};
          const objectId = payload.objectId;
          const updates = payload.updates;
          const tier = payload.tier as DurabilityTier | undefined;
          if (typeof objectId !== "string" || !isRecord(updates)) {
            throw new Error("Invalid payload for client.updateDurable.");
          }

          const client = await resolveCommandClient();
          await client.updateDurable(
            objectId,
            updates as Record<string, Value>,
            tier ? { tier } : undefined,
          );
          respond({ ok: true, payload: { updated: true } });
          return;
        }

        if (envelope.command === DEVTOOLS_COMMANDS.CLIENT_DELETE_DURABLE) {
          const payload = isRecord(envelope.payload)
            ? (envelope.payload as Partial<
                DevtoolsRequestPayloadByCommand[typeof DEVTOOLS_COMMANDS.CLIENT_DELETE_DURABLE]
              >)
            : {};
          const objectId = payload.objectId;
          const tier = payload.tier as DurabilityTier | undefined;
          if (typeof objectId !== "string") {
            throw new Error("Invalid payload for client.deleteDurable.");
          }

          const client = await resolveCommandClient();
          await client.deleteDurable(objectId, tier ? { tier } : undefined);
          respond({ ok: true, payload: { deleted: true } });
          return;
        }

        if (
          envelope.command !== DEVTOOLS_COMMANDS.CLIENT_QUERY &&
          envelope.command !== DEVTOOLS_COMMANDS.CLIENT_SUBSCRIBE
        ) {
          respond({
            ok: false,
            error: { message: `Unsupported devtools command: ${envelope.command}` },
          });
          return;
        }

        const queryPayload = isRecord(envelope.payload)
          ? (envelope.payload as Partial<
              DevtoolsRequestPayloadByCommand[typeof DEVTOOLS_COMMANDS.CLIENT_QUERY] &
                DevtoolsRequestPayloadByCommand[typeof DEVTOOLS_COMMANDS.CLIENT_SUBSCRIBE]
            >)
          : {};
        const query = queryPayload.query;
        const tier = queryPayload.tier as DurabilityTier | undefined;
        const options = isRecord(queryPayload.options)
          ? (queryPayload.options as QueryExecutionOptions)
          : tier
            ? { tier }
            : undefined;

        if (typeof query !== "string" && !isRecord(query)) {
          throw new Error(
            envelope.command === DEVTOOLS_COMMANDS.CLIENT_SUBSCRIBE
              ? "Invalid payload for client.subscribe."
              : "Invalid payload for client.query.",
          );
        }

        const client = await resolveCommandClient();

        if (envelope.command === DEVTOOLS_COMMANDS.CLIENT_SUBSCRIBE) {
          const bridgeSubscriptionId = queryPayload.subscriptionId;
          if (typeof bridgeSubscriptionId !== "string") {
            throw new Error("Invalid payload for client.subscribe.");
          }

          const priorSubscription = state?.activeSubscriptions.get(bridgeSubscriptionId);
          if (priorSubscription) {
            priorSubscription.client.unsubscribe(priorSubscription.runtimeSubscriptionId);
            state?.activeSubscriptions.delete(bridgeSubscriptionId);
          }

          const runtimeSubscriptionId = client.subscribe(
            query as string | QueryInput,
            (delta) => {
              window.postMessage(
                {
                  channel: DEVTOOLS_BRIDGE_CHANNEL,
                  kind: "event",
                  event: DEVTOOLS_EVENTS.CLIENT_SUBSCRIPTION_DELTA,
                  payload: {
                    subscriptionId: bridgeSubscriptionId,
                    delta,
                  },
                },
                "*",
              );
            },
            options,
          );

          state?.activeSubscriptions.set(bridgeSubscriptionId, {
            client,
            runtimeSubscriptionId,
          });
          respond({ ok: true, payload: { subscribed: true } });
          return;
        }

        const rows = await client.query(query as string | QueryInput, options);
        respond({ ok: true, payload: rows });
      } catch (error) {
        const errorMessage =
          error instanceof Error ? error.message : "Unknown devtools bridge error";
        respond({ ok: false, error: { message: errorMessage } });
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
