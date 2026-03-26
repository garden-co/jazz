import type {
  ActiveQuerySubscriptionTrace,
  DurabilityTier,
  InsertValues,
  QueryExecutionOptions,
  QueryInput,
  Row,
  Value,
  WasmSchema,
} from "../index.js";
import type { DbConfig } from "../runtime/db.js";

export const DEVTOOLS_BRIDGE_CHANNEL = "jazz-devtools-v1" as const;
export const DEVTOOLS_PORT_NAME = "jazz-inspector-devtools" as const;

export const DEVTOOLS_COMMANDS = {
  BRIDGE_HANDSHAKE: "bridge.handshake",
  ANNOUNCE: "devtools.announce",
  CLIENT_QUERY: "client.query",
  CLIENT_INSERT_DURABLE: "client.insertDurable",
  CLIENT_UPDATE_DURABLE: "client.updateDurable",
  CLIENT_DELETE_DURABLE: "client.deleteDurable",
  CLIENT_SUBSCRIBE: "client.subscribe",
  CLIENT_UNSUBSCRIBE: "client.unsubscribe",
  CLIENT_LIST_ACTIVE_QUERY_SUBSCRIPTIONS: "client.listActiveQuerySubscriptions",
} as const;

export const DEVTOOLS_EVENTS = {
  CONNECTED: "devtools.connected",
  DISCONNECTED: "devtools.disconnected",
  CLIENT_SUBSCRIPTION_DELTA: "client.subscription.delta",
  CLIENT_ACTIVE_QUERY_SUBSCRIPTIONS_CHANGED: "client.activeQuerySubscriptions.changed",
} as const;

export type DevtoolsBridgeCommand = (typeof DEVTOOLS_COMMANDS)[keyof typeof DEVTOOLS_COMMANDS];

export type DevtoolsBridgeEvent = (typeof DEVTOOLS_EVENTS)[keyof typeof DEVTOOLS_EVENTS];

export type DevtoolsBridgeHandshakeRequestPayload = Record<string, never>;
export type DevtoolsBridgeHandshakeResponsePayload = { ready: boolean };

export type DevtoolsAnnounceRequestPayload = Record<string, never>;
export type DevtoolsAnnounceResponsePayload = {
  ready: boolean;
  wasmSchema?: WasmSchema;
  dbConfig?: DbConfig;
};

export type DevtoolsClientQueryRequestPayload = {
  query: string | QueryInput;
  options?: QueryExecutionOptions;
  tier?: DurabilityTier;
};
export type DevtoolsClientQueryResponsePayload = unknown[];

export type DevtoolsClientInsertDurableRequestPayload = {
  table: string;
  values: InsertValues;
  tier?: DurabilityTier;
};
export type DevtoolsClientInsertDurableResponsePayload = Row;

export type DevtoolsClientUpdateDurableRequestPayload = {
  objectId: string;
  updates: Record<string, Value>;
  tier?: DurabilityTier;
};
export type DevtoolsClientUpdateDurableResponsePayload = { updated: true };

export type DevtoolsClientDeleteDurableRequestPayload = {
  objectId: string;
  tier?: DurabilityTier;
};
export type DevtoolsClientDeleteDurableResponsePayload = { deleted: true };

export type DevtoolsClientSubscribeRequestPayload = {
  query: string | QueryInput;
  options?: QueryExecutionOptions;
  tier?: DurabilityTier;
  subscriptionId: string;
};
export type DevtoolsClientSubscribeResponsePayload = { subscribed: true };

export type DevtoolsClientUnsubscribeRequestPayload = {
  subscriptionId: string;
};
export type DevtoolsClientUnsubscribeResponsePayload = { unsubscribed: true };

export type DevtoolsClientListActiveQuerySubscriptionsRequestPayload = Record<string, never>;
export type DevtoolsClientListActiveQuerySubscriptionsResponsePayload =
  ActiveQuerySubscriptionTrace[];

export interface DevtoolsRequestPayloadByCommand {
  [DEVTOOLS_COMMANDS.BRIDGE_HANDSHAKE]: DevtoolsBridgeHandshakeRequestPayload;
  [DEVTOOLS_COMMANDS.ANNOUNCE]: DevtoolsAnnounceRequestPayload;
  [DEVTOOLS_COMMANDS.CLIENT_QUERY]: DevtoolsClientQueryRequestPayload;
  [DEVTOOLS_COMMANDS.CLIENT_INSERT_DURABLE]: DevtoolsClientInsertDurableRequestPayload;
  [DEVTOOLS_COMMANDS.CLIENT_UPDATE_DURABLE]: DevtoolsClientUpdateDurableRequestPayload;
  [DEVTOOLS_COMMANDS.CLIENT_DELETE_DURABLE]: DevtoolsClientDeleteDurableRequestPayload;
  [DEVTOOLS_COMMANDS.CLIENT_SUBSCRIBE]: DevtoolsClientSubscribeRequestPayload;
  [DEVTOOLS_COMMANDS.CLIENT_UNSUBSCRIBE]: DevtoolsClientUnsubscribeRequestPayload;
  [DEVTOOLS_COMMANDS.CLIENT_LIST_ACTIVE_QUERY_SUBSCRIPTIONS]: DevtoolsClientListActiveQuerySubscriptionsRequestPayload;
}

export interface DevtoolsResponsePayloadByCommand {
  [DEVTOOLS_COMMANDS.BRIDGE_HANDSHAKE]: DevtoolsBridgeHandshakeResponsePayload;
  [DEVTOOLS_COMMANDS.ANNOUNCE]: DevtoolsAnnounceResponsePayload;
  [DEVTOOLS_COMMANDS.CLIENT_QUERY]: DevtoolsClientQueryResponsePayload;
  [DEVTOOLS_COMMANDS.CLIENT_INSERT_DURABLE]: DevtoolsClientInsertDurableResponsePayload;
  [DEVTOOLS_COMMANDS.CLIENT_UPDATE_DURABLE]: DevtoolsClientUpdateDurableResponsePayload;
  [DEVTOOLS_COMMANDS.CLIENT_DELETE_DURABLE]: DevtoolsClientDeleteDurableResponsePayload;
  [DEVTOOLS_COMMANDS.CLIENT_SUBSCRIBE]: DevtoolsClientSubscribeResponsePayload;
  [DEVTOOLS_COMMANDS.CLIENT_UNSUBSCRIBE]: DevtoolsClientUnsubscribeResponsePayload;
  [DEVTOOLS_COMMANDS.CLIENT_LIST_ACTIVE_QUERY_SUBSCRIPTIONS]: DevtoolsClientListActiveQuerySubscriptionsResponsePayload;
}

export type DevtoolsRequestEnvelope =
  | {
      channel: typeof DEVTOOLS_BRIDGE_CHANNEL;
      kind: "request";
      requestId: string;
      command: (typeof DEVTOOLS_COMMANDS)["BRIDGE_HANDSHAKE"];
      payload: DevtoolsBridgeHandshakeRequestPayload;
    }
  | {
      channel: typeof DEVTOOLS_BRIDGE_CHANNEL;
      kind: "request";
      requestId: string;
      command: (typeof DEVTOOLS_COMMANDS)["ANNOUNCE"];
      payload: DevtoolsAnnounceRequestPayload;
    }
  | {
      channel: typeof DEVTOOLS_BRIDGE_CHANNEL;
      kind: "request";
      requestId: string;
      command: (typeof DEVTOOLS_COMMANDS)["CLIENT_QUERY"];
      payload: DevtoolsClientQueryRequestPayload;
    }
  | {
      channel: typeof DEVTOOLS_BRIDGE_CHANNEL;
      kind: "request";
      requestId: string;
      command: (typeof DEVTOOLS_COMMANDS)["CLIENT_INSERT_DURABLE"];
      payload: DevtoolsClientInsertDurableRequestPayload;
    }
  | {
      channel: typeof DEVTOOLS_BRIDGE_CHANNEL;
      kind: "request";
      requestId: string;
      command: (typeof DEVTOOLS_COMMANDS)["CLIENT_UPDATE_DURABLE"];
      payload: DevtoolsClientUpdateDurableRequestPayload;
    }
  | {
      channel: typeof DEVTOOLS_BRIDGE_CHANNEL;
      kind: "request";
      requestId: string;
      command: (typeof DEVTOOLS_COMMANDS)["CLIENT_DELETE_DURABLE"];
      payload: DevtoolsClientDeleteDurableRequestPayload;
    }
  | {
      channel: typeof DEVTOOLS_BRIDGE_CHANNEL;
      kind: "request";
      requestId: string;
      command: (typeof DEVTOOLS_COMMANDS)["CLIENT_SUBSCRIBE"];
      payload: DevtoolsClientSubscribeRequestPayload;
    }
  | {
      channel: typeof DEVTOOLS_BRIDGE_CHANNEL;
      kind: "request";
      requestId: string;
      command: (typeof DEVTOOLS_COMMANDS)["CLIENT_UNSUBSCRIBE"];
      payload: DevtoolsClientUnsubscribeRequestPayload;
    }
  | {
      channel: typeof DEVTOOLS_BRIDGE_CHANNEL;
      kind: "request";
      requestId: string;
      command: (typeof DEVTOOLS_COMMANDS)["CLIENT_LIST_ACTIVE_QUERY_SUBSCRIPTIONS"];
      payload: DevtoolsClientListActiveQuerySubscriptionsRequestPayload;
    };

export type DevtoolsResponseEnvelope<
  TCommand extends DevtoolsBridgeCommand = DevtoolsBridgeCommand,
> = {
  channel: typeof DEVTOOLS_BRIDGE_CHANNEL;
  kind: "response";
  requestId: string;
  ok: boolean;
  payload?: DevtoolsResponsePayloadByCommand[TCommand];
  error?: { message?: string };
};

export type DevtoolsSubscriptionDeltaEventPayload = {
  subscriptionId: string;
  delta: unknown;
};

export type DevtoolsActiveQuerySubscriptionsChangedEventPayload = {
  subscriptions: ActiveQuerySubscriptionTrace[];
};

export interface DevtoolsEventPayloadByEvent {
  [DEVTOOLS_EVENTS.CONNECTED]: undefined;
  [DEVTOOLS_EVENTS.DISCONNECTED]: undefined;
  [DEVTOOLS_EVENTS.CLIENT_SUBSCRIPTION_DELTA]: DevtoolsSubscriptionDeltaEventPayload;
  [DEVTOOLS_EVENTS.CLIENT_ACTIVE_QUERY_SUBSCRIPTIONS_CHANGED]: DevtoolsActiveQuerySubscriptionsChangedEventPayload;
}

export type DevtoolsEventEnvelope<TEvent extends DevtoolsBridgeEvent = DevtoolsBridgeEvent> = {
  channel: typeof DEVTOOLS_BRIDGE_CHANNEL;
  kind: "event";
  event: TEvent;
  payload: DevtoolsEventPayloadByEvent[TEvent];
};

export interface DevToolsBootstrap {
  wasmSchema: WasmSchema;
  dbConfig: DbConfig;
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

export function isSerializableDbConfig(value: unknown): value is DbConfig {
  if (!isRecord(value)) return false;
  return typeof value.appId === "string";
}

export function sanitizeDbConfigForBridge(dbConfig: DbConfig | null): DbConfig | null {
  if (!dbConfig) {
    return null;
  }

  return {
    appId: dbConfig.appId,
    serverUrl: dbConfig.serverUrl,
    serverPathPrefix: dbConfig.serverPathPrefix,
    env: dbConfig.env,
    userBranch: dbConfig.userBranch,
    devMode: dbConfig.devMode,
    jwtToken: dbConfig.jwtToken,
    localAuthMode: dbConfig.localAuthMode,
    localAuthToken: dbConfig.localAuthToken,
    adminSecret: dbConfig.adminSecret,
    driver: dbConfig.driver,
  };
}
