import type { DurabilityTier } from "../../client.js";
import type { BrowserWorkerConnection } from "../../runtime-source.js";
import type { BrowserConnectionRole } from "./connection-role.js";
import type { ConnectionManagerClientInput, DbForConnection } from "../types.js";

interface LeaderWorkerRoleCallbacks {
  onFollowerPortAttached(followerTabId: string, leadershipId: number): void;
  onFollowerPortClosed(followerTabId: string, leadershipId: number): void;
  onReady(leadershipId: number): void;
  onFailure(error: unknown, role: LeaderWorkerConnectionRole, leadershipId: number): void;
}

export class LeaderWorkerConnectionRole implements BrowserConnectionRole {
  private workerBridge: BrowserWorkerConnection | null = null;
  private bridgeReady: Promise<void> | null = null;
  private shouldBeConnected = true;
  private readonly pendingFollowerPorts = new Map<
    string,
    { followerTabId: string; leadershipId: number; port: MessagePort }
  >();

  constructor(
    private readonly host: DbForConnection,
    private readonly leadershipId: number,
    private readonly workerLockName: string,
    private readonly authJson: () => string,
    private readonly sessionClaims: () => Record<string, unknown>,
    private readonly callbacks: LeaderWorkerRoleCallbacks,
  ) {}

  onClientCreated({ schema, client }: ConnectionManagerClientInput): void {
    if (this.workerBridge) return;
    let bridge!: BrowserWorkerConnection;
    bridge = this.host.runtimeSource.createBrowserWorkerConnection({
      config: { ...this.host.config },
      schema,
      client,
      leadershipId: this.leadershipId,
      workerLockName: this.workerLockName,
      onAuthFailure: (reason) => this.host.markUnauthenticated(reason),
      onAuthRestored: () => this.host.clearAuthError(),
      onFailure: (error) => {
        if (this.workerBridge !== bridge) return;
        this.callbacks.onFailure(error, this, this.leadershipId);
      },
      onFollowerPortClosed: (followerTabId, leadershipId) => {
        if (this.workerBridge !== bridge) return;
        this.callbacks.onFollowerPortClosed(followerTabId, leadershipId);
      },
    });
    this.workerBridge = bridge;
    this.bridgeReady = bridge.ready().then(async () => {
      if (this.workerBridge !== bridge) return;
      if (!this.shouldBeConnected) await bridge.disconnect();
      this.flushPendingFollowerPorts();
      this.callbacks.onReady(this.leadershipId);
    });
    void this.bridgeReady.catch(() => undefined);
  }

  attachFollowerPort(followerTabId: string, leadershipId: number, port: MessagePort): void {
    if (leadershipId !== this.leadershipId) {
      port.close();
      return;
    }
    const bridge = this.workerBridge;
    if (!bridge) {
      this.pendingFollowerPorts.set(followerTabId, { followerTabId, leadershipId, port });
      return;
    }
    void bridge.attachFollowerPort(followerTabId, leadershipId, port).then(
      () => this.callbacks.onFollowerPortAttached(followerTabId, leadershipId),
      (error: unknown) => this.callbacks.onFailure(error, this, leadershipId),
    );
  }

  detachFollowerPort(followerTabId: string, leadershipId: number): void {
    const pending = this.pendingFollowerPorts.get(followerTabId);
    if (pending?.leadershipId === leadershipId) {
      pending.port.close();
      this.pendingFollowerPorts.delete(followerTabId);
    }
    void this.workerBridge?.detachFollowerPort(followerTabId, leadershipId).catch(() => undefined);
  }

  closePendingFollowerPorts(): void {
    for (const entry of this.pendingFollowerPorts.values()) entry.port.close();
    this.pendingFollowerPorts.clear();
  }

  private flushPendingFollowerPorts(): void {
    for (const [followerTabId, entry] of this.pendingFollowerPorts) {
      this.pendingFollowerPorts.delete(followerTabId);
      this.attachFollowerPort(entry.followerTabId, entry.leadershipId, entry.port);
    }
  }

  async ensureReady(tier?: DurabilityTier): Promise<void> {
    await this.bridgeReady;
    if (!this.workerBridge || !this.host.config.serverUrl) return;
    if (tier == null || tier === "local") return;
    await this.workerBridge.waitForServerConnection();
  }

  async disconnect(): Promise<void> {
    if (!this.host.config.serverUrl) {
      throw new Error("Db.disconnect() requires a configured serverUrl.");
    }
    this.shouldBeConnected = false;
    await this.bridgeReady;
    await this.workerBridge?.disconnect();
  }

  async reconnect(): Promise<void> {
    if (!this.host.config.serverUrl) {
      throw new Error("Db.reconnect() requires a configured serverUrl.");
    }
    this.shouldBeConnected = true;
    await this.bridgeReady;
    await this.workerBridge?.reconnect(this.authJson(), this.sessionClaims());
    await this.workerBridge?.waitForServerConnection();
  }

  updateAuth(authJson: string, sessionClaims: Record<string, unknown>): void {
    this.workerBridge?.updateAuth(authJson, sessionClaims);
  }

  /** @internal Test-only failover hook. */
  async simulateCrash(): Promise<void> {
    await this.workerBridge?.simulateCrash();
  }

  async shutdown(): Promise<void> {
    this.closePendingFollowerPorts();
    const bridge = this.workerBridge;
    this.workerBridge = null;
    this.bridgeReady = null;
    try {
      await bridge?.shutdown();
    } catch {
      // The role is also shut down after worker startup/crash failures. The
      // broker already owns recovery, so teardown must not leak that failure
      // as a second unhandled rejection.
    }
  }
}
