import { JazzClient, type DurabilityTier } from "../../client.js";
import {
  WorkerBridge,
  type WorkerBridgeOptions,
  type WorkerLifecycleEvent,
} from "../../worker-bridge.js";
import type { BrowserConnectionRole } from "./connection-role.js";
import type { ConnectionManagerClientInput, DbForConnection } from "../types.js";

interface LeaderWorkerBridgeCallbacks {
  onFollowerPortAttached(peerId: string, leadershipId: number): void;
  onFollowerPortClosed(peerId: string, leadershipId: number): void;
  onReady(leadershipId: number): void;
  onFailure(error: unknown, bridge: LeaderWorkerConnectionRole, leadershipId: number): void;
}

export class LeaderWorkerConnectionRole implements BrowserConnectionRole {
  private workerBridge: WorkerBridge | null = null;
  private bridgeReady: Promise<void> | null = null;
  private readonly pendingFollowerPorts = new Map<
    string,
    { followerTabId: string; leadershipId: number; port: MessagePort }
  >();

  constructor(
    private readonly host: DbForConnection,
    private readonly worker: Worker,
    private readonly leadershipId: number,
    private readonly buildWorkerBridgeOptions: (schemaJson: string) => WorkerBridgeOptions,
    private readonly callbacks: LeaderWorkerBridgeCallbacks,
  ) {}

  onClientCreated({ schemaKey, client }: ConnectionManagerClientInput): void {
    if (this.workerBridge) return;
    this.attachWorkerBridge(schemaKey, client);
  }

  private attachWorkerBridge(schemaJson: string, client: JazzClient): void {
    const bridge = new WorkerBridge(this.worker, client.getRuntime());
    bridge.setServerPayloadForwarder(null);
    bridge.onAuthFailure((reason) => {
      this.host.markUnauthenticated(reason);
    });
    bridge.onFollowerPortAttached((event) => {
      if (event.leadershipId !== this.leadershipId) return;
      if (this.workerBridge !== bridge) return;
      this.callbacks.onFollowerPortAttached(event.peerId, event.leadershipId);
      if (!this.host.config.serverUrl) return;
      void bridge.waitForUpstreamServerConnection().catch((error) => {
        if (this.workerBridge !== bridge) return;
        this.callbacks.onFailure(error, this, event.leadershipId);
      });
    });
    bridge.onFollowerPortClosed((event) => {
      if (event.leadershipId !== this.leadershipId) return;
      if (this.workerBridge !== bridge) return;
      this.callbacks.onFollowerPortClosed(event.peerId, event.leadershipId);
    });
    this.workerBridge = bridge;
    const bridgeReady = bridge
      .init(this.buildWorkerBridgeOptions(schemaJson))
      .then(() => {
        if (this.workerBridge !== bridge) return;
        this.flushPendingFollowerPorts();
        this.callbacks.onReady(this.leadershipId);
      })
      .then(() => undefined);
    bridgeReady.catch((error) => {
      if (this.workerBridge !== bridge) return;
      this.callbacks.onFailure(error, this, this.leadershipId);
    });
    this.bridgeReady = bridgeReady;
  }

  attachFollowerPort(followerTabId: string, leadershipId: number, port: MessagePort): void {
    if (leadershipId !== this.leadershipId) {
      port.close();
      return;
    }
    if (!this.workerBridge) {
      this.pendingFollowerPorts.set(followerTabId, { followerTabId, leadershipId, port });
      return;
    }
    this.workerBridge.attachFollowerPort(followerTabId, leadershipId, port);
  }

  detachFollowerPort(followerTabId: string, leadershipId: number): void {
    const pending = this.pendingFollowerPorts.get(followerTabId);
    if (pending?.leadershipId === leadershipId) {
      pending.port.close();
      this.pendingFollowerPorts.delete(followerTabId);
    }
    this.workerBridge?.detachFollowerPort(followerTabId, leadershipId);
  }

  closePendingFollowerPorts(): void {
    for (const entry of this.pendingFollowerPorts.values()) {
      entry.port.close();
    }
    this.pendingFollowerPorts.clear();
  }

  private flushPendingFollowerPorts(): void {
    if (!this.workerBridge) return;
    for (const [followerTabId, entry] of this.pendingFollowerPorts) {
      this.pendingFollowerPorts.delete(followerTabId);
      if (entry.leadershipId !== this.leadershipId) {
        entry.port.close();
        continue;
      }
      this.workerBridge.attachFollowerPort(entry.followerTabId, entry.leadershipId, entry.port);
    }
  }

  async ensureReady(tier?: DurabilityTier): Promise<void> {
    await this.bridgeReady;
    if (!this.workerBridge || !this.host.config.serverUrl) return;
    if (!tier || tier === "local") return;
    await this.workerBridge.waitForUpstreamServerConnection();
  }

  updateAuth(auth: { jwtToken?: string }): void {
    this.workerBridge?.updateAuth(auth);
  }

  sendLifecycleHint(event: WorkerLifecycleEvent): void {
    if (this.workerBridge) {
      this.workerBridge.sendLifecycleHint(event);
      return;
    }
    this.worker.postMessage({
      type: "lifecycle-hint",
      event,
      sentAtMs: Date.now(),
    });
  }

  async simulateCrash(): Promise<void> {
    await this.workerBridge?.simulateCrash();
  }

  replayServerConnection(): void {
    this.workerBridge?.replayServerConnection();
  }

  disconnectUpstream(): void {
    this.workerBridge?.disconnectUpstream();
  }

  reconnectUpstream(): void {
    this.workerBridge?.reconnectUpstream();
  }

  async shutdown(): Promise<void> {
    this.closePendingFollowerPorts();
    if (this.workerBridge) {
      try {
        await this.workerBridge.shutdown();
      } catch {
        // Best effort during broker demotion/shutdown.
      }
    }
    this.workerBridge = null;
    this.bridgeReady = null;
    this.worker.terminate();
  }

  async shutdownForStorageReset(): Promise<void> {
    this.closePendingFollowerPorts();
    if (this.workerBridge) {
      try {
        await this.workerBridge.shutdown();
      } catch {
        // Best effort: if the bridge shutdown times out, storage reset still terminates below.
      }
    }
    this.workerBridge = null;
    this.bridgeReady = null;
    this.worker.terminate();
  }
}
