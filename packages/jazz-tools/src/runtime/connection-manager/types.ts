import type { WasmSchema } from "../../drivers/types.js";
import { JazzClient, type DurabilityTier, type QueryExecutionOptions } from "../client.js";
import type { DbConfig } from "../db.js";
import type { AuthFailureReason } from "../sync-transport.js";
import type { WorkerLifecycleEvent } from "../worker-bridge.js";

export interface ConnectionBridgeClientInput {
  schemaKey: string;
  schema: WasmSchema;
  client: JazzClient;
}

export interface ConnectionManager {
  readonly hasDurablePeer: boolean;
  start(): Promise<void>;
  onClientCreated(input: ConnectionBridgeClientInput): void;
  ensureReadyForQuery(options?: QueryExecutionOptions): Promise<void>;
  ensureReadyForWriteWait(tier: DurabilityTier): Promise<void>;
  updateAuth(auth: { jwtToken?: string }): void;
  sendLifecycleHint(event: WorkerLifecycleEvent): void;
  shouldDeferSubscriptionStart(): boolean;
  deleteClientStorage(): Promise<void>;
  shutdown(): Promise<void>;
}

export interface ConnectionManagerHost {
  readonly config: DbConfig;
  readonly isShuttingDown: boolean;
  markUnauthenticated(reason: AuthFailureReason): void;
  telemetryCollectorUrl(): string | undefined;
  firstClientEntry(): ConnectionBridgeClientInput | null;
  shutdownClientsForConnectionReset(): Promise<void>;
  recreateClientAfterConnectionReset(schema: WasmSchema): JazzClient;
}
