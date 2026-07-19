import type { AuthFailureReason } from "../../sync-transport.js";
import { MessagePortRuntimeBridge } from "../../worker-bridge.js";
import type { BrowserConnectionRole } from "./connection-role.js";
import type { ConnectionManagerClientInput } from "../types.js";

interface FollowerPortBridgeCallbacks {
  onReady(leadershipId: number): void;
  onAuthFailure(reason: AuthFailureReason): void;
}

export class FollowerPortConnectionRole implements BrowserConnectionRole {
  private followerPortBridge: MessagePortRuntimeBridge | null = null;
  private followerDataPort: MessagePort | null;
  private portReadyLeadershipId: number | null = null;
  private leaderReadyLeadershipId: number | null = null;

  constructor(
    private readonly leadershipId: number,
    port: MessagePort,
    private readonly callbacks: FollowerPortBridgeCallbacks,
    options: { preserveLeaderReadySignal?: boolean } = {},
  ) {
    this.followerDataPort = port;
    if (options.preserveLeaderReadySignal) {
      this.leaderReadyLeadershipId = leadershipId;
    }
  }

  onClientCreated({ client }: ConnectionManagerClientInput): void {
    if (this.followerPortBridge || !this.followerDataPort) return;

    const bridge = new MessagePortRuntimeBridge(this.followerDataPort, client.getRuntime());
    bridge.init();
    bridge.onAuthFailure((reason) => {
      this.callbacks.onAuthFailure(reason);
    });
    this.followerPortBridge = bridge;
    this.followerDataPort = null;
    this.portReadyLeadershipId = this.leadershipId;
    this.resolveIfReady();
  }

  markLeaderReady(leadershipId: number): void {
    if (leadershipId !== this.leadershipId) return;
    this.leaderReadyLeadershipId = leadershipId;
    this.resolveIfReady();
  }

  private resolveIfReady(): void {
    if (this.portReadyLeadershipId !== this.leadershipId) return;
    if (this.leaderReadyLeadershipId !== this.leadershipId) return;
    this.callbacks.onReady(this.leadershipId);
  }

  async ensureReady(): Promise<void> {}

  async disconnect(): Promise<void> {
    throw new Error("Db.disconnect() is only supported on the browser leader tab.");
  }

  async reconnect(): Promise<void> {
    throw new Error("Db.reconnect() is only supported on the browser leader tab.");
  }

  updateAuth(auth: { jwtToken?: string }): void {
    this.followerPortBridge?.updateAuth(auth);
  }

  sendLifecycleHint(): void {}

  detachForReconnect(): void {
    this.followerPortBridge?.detachForReconnect();
    this.followerPortBridge = null;
    this.portReadyLeadershipId = null;
    this.followerDataPort?.close();
    this.followerDataPort = null;
  }

  shutdown(): void {
    this.followerPortBridge?.shutdown();
    this.followerPortBridge = null;
    this.portReadyLeadershipId = null;
    this.followerDataPort?.close();
    this.followerDataPort = null;
  }
}
