import type { WasmSchema } from "../index.js";
import type { DbConfig } from "../runtime/db.js";

export const DEVTOOLS_BRIDGE_CHANNEL = "jazz-devtools-v1" as const;
export const DEVTOOLS_PORT_NAME = "jazz-inspector-devtools" as const;

export const DEVTOOLS_COMMANDS = {
  BRIDGE_HANDSHAKE: "bridge.handshake",
  ANNOUNCE: "devtools.announce",
  CLIENT_QUERY: "client.query",
  CLIENT_SUBSCRIBE: "client.subscribe",
  CLIENT_UNSUBSCRIBE: "client.unsubscribe",
} as const;

export const DEVTOOLS_EVENTS = {
  CONNECTED: "devtools.connected",
  DISCONNECTED: "devtools.disconnected",
  CLIENT_SUBSCRIPTION_DELTA: "client.subscription.delta",
} as const;

export type DevtoolsBridgeCommand = (typeof DEVTOOLS_COMMANDS)[keyof typeof DEVTOOLS_COMMANDS];

export type DevtoolsBridgeEvent = (typeof DEVTOOLS_EVENTS)[keyof typeof DEVTOOLS_EVENTS];

export type DevtoolsRequestEnvelope = {
  channel: typeof DEVTOOLS_BRIDGE_CHANNEL;
  kind: "request";
  requestId: string;
  command: DevtoolsBridgeCommand | string;
  payload?: unknown;
};

export type DevtoolsResponseEnvelope = {
  channel: typeof DEVTOOLS_BRIDGE_CHANNEL;
  kind: "response";
  requestId: string;
  ok: boolean;
  payload?: unknown;
  error?: { message?: string };
};

export type DevtoolsEventEnvelope = {
  channel: typeof DEVTOOLS_BRIDGE_CHANNEL;
  kind: "event";
  event: DevtoolsBridgeEvent | string;
  payload?: unknown;
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
    jwtToken: dbConfig.jwtToken,
    localAuthMode: dbConfig.localAuthMode,
    localAuthToken: dbConfig.localAuthToken,
    adminSecret: dbConfig.adminSecret,
    driver: dbConfig.driver,
  };
}
