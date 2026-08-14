import type { DurabilityTier } from "../../client.js";
import type { ConnectionManagerClientInput } from "../types.js";

export interface BrowserConnectionRole {
  onClientCreated(input: ConnectionManagerClientInput): void;
  ensureReady(tier?: DurabilityTier): Promise<void>;
  disconnect(): Promise<void>;
  reconnect(): Promise<void>;
  updateAuth(authJson: string, sessionClaims: Record<string, unknown>): void;
  shutdown(): Promise<void> | void;
}
