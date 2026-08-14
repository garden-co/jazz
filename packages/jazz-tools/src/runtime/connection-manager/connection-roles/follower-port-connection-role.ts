import type { DurabilityTier } from "../../client.js";
import type { BrowserFollowerConnection } from "../../runtime-source.js";
import type { BrowserConnectionRole } from "./connection-role.js";
import type { ConnectionManagerClientInput, DbForConnection } from "../types.js";

interface FollowerPortRoleCallbacks {
  onReady(leadershipId: number): void;
  onFailure(error: unknown, role: FollowerPortConnectionRole, leadershipId: number): void;
}

export class FollowerPortConnectionRole implements BrowserConnectionRole {
  private followerPortBridge: BrowserFollowerConnection | null = null;
  private followerDataPort: MessagePort | null;
  private portReady = false;
  private leaderReady = false;

  constructor(
    private readonly host: DbForConnection,
    private readonly leadershipId: number,
    port: MessagePort,
    private readonly callbacks: FollowerPortRoleCallbacks,
    options: { preserveLeaderReadySignal?: boolean } = {},
  ) {
    this.followerDataPort = port;
    this.leaderReady = options.preserveLeaderReadySignal ?? false;
  }

  onClientCreated({ client }: ConnectionManagerClientInput): void {
    if (this.followerPortBridge || !this.followerDataPort) return;
    let bridge!: BrowserFollowerConnection;
    bridge = this.host.runtimeSource.createBrowserFollowerConnection({
      config: { ...this.host.config },
      client,
      leadershipId: this.leadershipId,
      port: this.followerDataPort,
      onAuthFailure: (reason) => this.host.markUnauthenticated(reason),
      onAuthRestored: () => this.host.clearAuthError(),
      onFailure: (error) => {
        if (this.followerPortBridge !== bridge) return;
        this.callbacks.onFailure(error, this, this.leadershipId);
      },
    });
    this.followerPortBridge = bridge;
    this.followerDataPort = null;
    void bridge.ready().then(
      () => {
        if (this.followerPortBridge !== bridge) return;
        this.portReady = true;
        this.resolveIfReady();
      },
      (error: unknown) => this.callbacks.onFailure(error, this, this.leadershipId),
    );
  }

  markLeaderReady(leadershipId: number): void {
    if (leadershipId !== this.leadershipId) return;
    this.leaderReady = true;
    this.resolveIfReady();
  }

  private resolveIfReady(): void {
    if (this.portReady && this.leaderReady) this.callbacks.onReady(this.leadershipId);
  }

  async ensureReady(tier?: DurabilityTier): Promise<void> {
    await this.followerPortBridge?.ready();
    if (!this.host.config.serverUrl || tier == null || tier === "local") return;
    await this.followerPortBridge?.waitForServerConnection();
  }

  async disconnect(): Promise<void> {
    throw new Error("Db.disconnect() is only supported on the browser leader tab.");
  }

  async reconnect(): Promise<void> {
    throw new Error("Db.reconnect() is only supported on the browser leader tab.");
  }

  updateAuth(authJson: string, sessionClaims: Record<string, unknown>): void {
    this.followerPortBridge?.updateAuth(authJson, sessionClaims);
  }

  detachForReconnect(): void {
    const bridge = this.followerPortBridge;
    this.followerPortBridge = null;
    this.portReady = false;
    this.followerDataPort?.close();
    this.followerDataPort = null;
    bridge?.detachForReconnect();
  }

  async shutdown(): Promise<void> {
    const bridge = this.followerPortBridge;
    this.followerPortBridge = null;
    this.followerDataPort?.close();
    this.followerDataPort = null;
    await bridge?.shutdown();
  }
}
