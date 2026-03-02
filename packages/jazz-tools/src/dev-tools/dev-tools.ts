import { PersistenceTier, QueryExecutionOptions, WasmSchema } from "../index.js";
import { Db, DbConfig, QueryBuilder } from "../runtime/db.js";
import {
  DEVTOOLS_BRIDGE_CHANNEL,
  DEVTOOLS_COMMANDS,
  DEVTOOLS_EVENTS,
  DevtoolsRequestEnvelope,
  DevtoolsResponseEnvelope,
  isRecord,
} from "./protocol.js";

type DevToolsStateListener = (connected: boolean) => void;

type RuntimeBridgeState = {
  wasmSchema: WasmSchema | null;
  dbConfig: DbConfig | null;
  connected: boolean;
  listeners: Set<DevToolsStateListener>;
  activeSubscriptions: Map<string, () => void>;
};

export interface DevToolsAttachment {
  isConnected(): boolean;
  onConnectionChange(listener: DevToolsStateListener): () => void;
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

function clearRuntimeBridgeSubscriptions(db: Db): void {
  const state = runtimeBridgeStateByDb.get(db);
  if (!state) return;
  for (const unsubscription of state.activeSubscriptions.values()) {
    try {
      unsubscription();
    } catch {
      // Ignore cleanup failures during bridge teardown.
    }
  }
  state.activeSubscriptions.clear();
}

function hookRegistration(db: Db, wasmSchema: WasmSchema, dbConfig: DbConfig): DevToolsAttachment {
  let state = runtimeBridgeStateByDb.get(db);
  if (!state) {
    state = {
      wasmSchema,
      dbConfig: dbConfig,
      connected: false,
      listeners: new Set(),
      activeSubscriptions: new Map(),
    };
    runtimeBridgeStateByDb.set(db, state);
  } else {
    state.wasmSchema = wasmSchema;
    state.dbConfig = dbConfig;
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
          respond({
            ok: true,
            payload: { ready: true, wasmSchema, dbConfig },
          });
          setRuntimeBridgeConnected(db, true);
          return;
        }

        if (envelope.command === DEVTOOLS_COMMANDS.CLIENT_UNSUBSCRIBE) {
          const payload = isRecord(envelope.payload) ? envelope.payload : {};
          const bridgeSubscriptionId = payload.subscriptionId;
          if (typeof bridgeSubscriptionId !== "string") {
            throw new Error("Invalid payload for client.unsubscribe.");
          }
          const unsubActiveSubscription = state?.activeSubscriptions.get(bridgeSubscriptionId);
          if (unsubActiveSubscription) {
            unsubActiveSubscription();
            state?.activeSubscriptions.delete(bridgeSubscriptionId);
          }
          respond({ ok: true, payload: { unsubscribed: true } });
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

        const queryPayload = isRecord(envelope.payload) ? envelope.payload : {};
        const query = queryPayload.query;
        const settledTier = queryPayload.settledTier as PersistenceTier | undefined;
        const options = isRecord(queryPayload.options)
          ? (queryPayload.options as QueryExecutionOptions)
          : settledTier
            ? { settledTier }
            : undefined;

        if (!isRecord(query)) {
          throw new Error(
            envelope.command === DEVTOOLS_COMMANDS.CLIENT_SUBSCRIBE
              ? "Invalid payload for client.subscribe."
              : "Invalid payload for client.query.",
          );
        }

        if (envelope.command === DEVTOOLS_COMMANDS.CLIENT_SUBSCRIBE) {
          const bridgeSubscriptionId = queryPayload.subscriptionId;
          if (typeof bridgeSubscriptionId !== "string") {
            throw new Error("Invalid payload for client.subscribe.");
          }

          const unsubPriorSubscription = state?.activeSubscriptions.get(bridgeSubscriptionId);
          if (unsubPriorSubscription) {
            unsubPriorSubscription();
            state?.activeSubscriptions.delete(bridgeSubscriptionId);
          }

          const unsub = db.subscribeAll(
            translateQueryToBuilder(query as any),
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

          state?.activeSubscriptions.set(bridgeSubscriptionId, unsub);
          respond({ ok: true, payload: { subscribed: true } });
          return;
        }

        const rows = await db.all(translateQueryToBuilder(query as any), options);
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
  };
}

function translateQueryToBuilder(query: {
  _schema: WasmSchema;
  _table: string;
  _build: string;
}): QueryBuilder<{ id: string }> {
  return {
    _schema: query._schema,
    _table: query._table,
    _rowType: undefined as unknown as { id: string },
    _build: () => query._build,
  };
}

async function resolveDb(input: Promise<{ db: Db }> | Db | { db: Db }): Promise<Db> {
  const resolved = await Promise.resolve(input);

  if (resolved instanceof Db) {
    return resolved;
  }
  return resolved.db;
}

export async function attachDevTools(
  clientOrDb: Promise<{ db: Db }> | { db: Db } | Db,
  wasmSchema: WasmSchema,
): Promise<DevToolsAttachment> {
  const db = await resolveDb(clientOrDb);
  return hookRegistration(db, wasmSchema, db.getConfig());
}
