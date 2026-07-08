import type { DurabilityTier } from "../../client.js";
import type { WorkerLifecycleEvent } from "../../worker-bridge.js";
import type { ConnectionManagerClientInput } from "../types.js";

export interface BrowserConnectionRole {
  onClientCreated(input: ConnectionManagerClientInput): void;
  ensureReady(tier?: DurabilityTier): Promise<void>;
  disconnect(): Promise<void>;
  reconnect(): Promise<void>;
  updateAuth(auth: { jwtToken?: string }): void;
  sendLifecycleHint(event: WorkerLifecycleEvent): void;
  shutdown(): Promise<void> | void;
}
