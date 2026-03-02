import { type WasmSchema } from "../index.js";
import { Db, DbConfig, QueryBuilder } from "../runtime/db.js";
import {
  DEVTOOLS_BRIDGE_CHANNEL,
  DEVTOOLS_COMMANDS,
  DEVTOOLS_EVENTS,
  DevtoolsBridgeCommand,
  DevtoolsEventEnvelope,
  DevtoolsRequestEnvelope,
  DevtoolsRequestPayloadByCommand,
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
const devtoolsCommandSet = new Set<string>(Object.values(DEVTOOLS_COMMANDS));

function isDevtoolsBridgeCommand(command: unknown): command is DevtoolsBridgeCommand {
  return typeof command === "string" && devtoolsCommandSet.has(command);
}

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
        const eventEnvelope = rawMessage as Partial<DevtoolsEventEnvelope>;
        if (eventEnvelope.event === DEVTOOLS_EVENTS.CONNECTED) {
          setRuntimeBridgeConnected(db, true);
        }
        if (eventEnvelope.event === DEVTOOLS_EVENTS.DISCONNECTED) {
          clearRuntimeBridgeSubscriptions(db);
          setRuntimeBridgeConnected(db, false);
        }
        return;
      }

      const envelope = rawMessage as DevtoolsRequestEnvelope;
      if (
        envelope.kind !== "request" ||
        typeof envelope.requestId !== "string" ||
        !isDevtoolsBridgeCommand(envelope.command)
      ) {
        return;
      }
      const requestId = envelope.requestId;

      const respond = (
        response: Omit<
          DevtoolsResponseEnvelope<DevtoolsBridgeCommand>,
          "channel" | "kind" | "requestId"
        >,
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
          const runtimeState = runtimeBridgeStateByDb.get(db);
          if (!runtimeState?.wasmSchema || !runtimeState.dbConfig) {
            throw new Error("DevTools bridge runtime state is not initialized.");
          }
          respond({
            ok: true,
            payload: {
              ready: true,
              wasmSchema: runtimeState.wasmSchema,
              dbConfig: runtimeState.dbConfig,
            },
          });
          setRuntimeBridgeConnected(db, true);
          return;
        }

        if (envelope.command === DEVTOOLS_COMMANDS.CLIENT_UNSUBSCRIBE) {
          const payload =
            envelope.payload as DevtoolsRequestPayloadByCommand[typeof DEVTOOLS_COMMANDS.CLIENT_UNSUBSCRIBE];
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

        if (envelope.command === DEVTOOLS_COMMANDS.CLIENT_QUERY) {
          const queryPayload = envelope.payload;
          const rows = await db.all(
            translateQueryToBuilder(queryPayload.query),
            queryPayload.options,
          );
          respond({ ok: true, payload: rows });
          return;
        }

        if (envelope.command === DEVTOOLS_COMMANDS.CLIENT_SUBSCRIBE) {
          const { subscriptionId, query, options } = envelope.payload;
          const unsubPriorSubscription = state?.activeSubscriptions.get(subscriptionId);
          if (unsubPriorSubscription) {
            unsubPriorSubscription();
            state?.activeSubscriptions.delete(subscriptionId);
          }

          const unsub = db.subscribeAll(
            translateQueryToBuilder(query),
            (delta) => {
              window.postMessage(
                {
                  channel: DEVTOOLS_BRIDGE_CHANNEL,
                  kind: "event",
                  event: DEVTOOLS_EVENTS.CLIENT_SUBSCRIPTION_DELTA,
                  payload: {
                    subscriptionId: subscriptionId,
                    delta,
                  },
                },
                "*",
              );
            },
            options,
          );

          state?.activeSubscriptions.set(subscriptionId, unsub);
          respond({ ok: true, payload: { subscribed: true } });
          return;
        }

        // @ts-expect-error - it should be impossible to get here
        console.error(`Unsupported devtools command: ${envelope.command}`);
        respond({
          ok: false,
          // @ts-expect-error - it should be impossible to get here
          error: { message: `Unsupported devtools command: ${envelope.command}` },
        });
        return;
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

function translateQueryToBuilder(
  query: Omit<QueryBuilder<unknown>, "_build"> & { _build: string },
): QueryBuilder<{ id: string }> {
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
