import type { DurabilityTier, QueryExecutionOptions } from "../../client.js";
import type { WorkerLifecycleEvent } from "../../worker-bridge.js";
import type { ConnectionBridgeClientInput } from "../types.js";

export interface BrowserConnectionRole {
  onClientCreated(input: ConnectionBridgeClientInput): void;
  ensureReadyForQuery(options?: QueryExecutionOptions): Promise<void>;
  ensureReadyForWriteWait(tier: DurabilityTier): Promise<void>;
  updateAuth(auth: { jwtToken?: string }): void;
  sendLifecycleHint(event: WorkerLifecycleEvent): void;
  shutdown(): Promise<void> | void;
}
