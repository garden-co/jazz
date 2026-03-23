import type {
  ActiveQuerySubscriptionTrace,
  DurabilityTier,
  QueryExecutionOptions,
  QueryInput,
  Row,
  Value,
  WasmSchema,
} from "../index.js";
import type { DbConfig } from "../runtime/db.js";

export const DEVTOOLS_PORT_NAME = "jazz-inspector-devtools" as const;
export const DEVTOOLS_MC_CHANNEL = "jazz-devtools-comlink-v1" as const;

export interface DevtoolsRuntimeAPI {
  announce(): Promise<{
    ready: boolean;
    wasmSchema?: WasmSchema;
    dbConfig?: DbConfig;
  }>;
  query(query: string | QueryInput, options?: QueryExecutionOptions): Promise<unknown[]>;
  insertDurable(table: string, values: Value[], tier?: DurabilityTier): Promise<Row>;
  updateDurable(
    objectId: string,
    updates: Record<string, Value>,
    tier?: DurabilityTier,
  ): Promise<void>;
  deleteDurable(objectId: string, tier?: DurabilityTier): Promise<void>;
  subscribe(
    query: string | QueryInput,
    subscriptionId: string,
    options?: QueryExecutionOptions,
  ): Promise<void>;
  unsubscribe(subscriptionId: string): Promise<void>;
  listActiveQuerySubscriptions(): Promise<ActiveQuerySubscriptionTrace[]>;
}

export type DevtoolsEvent =
  | { type: "subscription-delta"; subscriptionId: string; delta: unknown }
  | {
      type: "active-query-subscriptions-changed";
      subscriptions: ActiveQuerySubscriptionTrace[];
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
