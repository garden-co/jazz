import type { StorageDriver } from "../drivers/types.js";
import type { WorkerLifecycleEvent } from "../worker/worker-protocol.js";
import type { JazzClient } from "./client.js";
import { resolveClientSessionSync } from "./client-session.js";
import type { RuntimeSourcesConfig } from "./context.js";
import type { DbConfig } from "./db.js";
import type { DbBackend, DbBackendClientContext, DbBackendHost } from "./db-backend.js";
import {
  appendWorkerRuntimeWasmUrl,
  resolveRuntimeConfigSyncInitInput,
  resolveRuntimeConfigWorkerUrl,
  resolveWorkerBootstrapWasmUrl,
} from "./runtime-config.js";
import { StorageResetCoordinator, type StorageResetHost } from "./storage-reset-coordinator.js";
import { TabLeaderElection, type LeaderRole, type LeaderSnapshot } from "./tab-leader-election.js";
import {
  isTabSyncMessage,
  resolveBroadcastChannelCtor,
  type BroadcastChannelLike,
  type FollowerCloseMessage,
  type FollowerSyncMessage,
  type LeaderSyncMessage,
  type TabSyncMessage,
} from "./tab-sync-protocol.js";
import { WorkerBridge, type PeerSyncBatch, type WorkerBridgeOptions } from "./worker-bridge.js";

type BrowserWasmBackendClientFactory = (context: DbBackendClientContext<DbConfig>) => JazzClient;

export interface BrowserWasmBackendOptions {
  config: DbConfig;
  host: DbBackendHost;
  createClient: BrowserWasmBackendClientFactory;
}

function resolveStorageDriver(driver?: StorageDriver): StorageDriver {
  return driver ?? { type: "persistent" };
}

function trimOptionalString(value?: string | null): string | null {
  if (typeof value !== "string") {
    return null;
  }

  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function isBrowser(): boolean {
  return typeof Worker !== "undefined" && typeof window !== "undefined";
}

function resolveDefaultPersistentDbName(config: DbConfig): string {
  const driver = resolveStorageDriver(config.driver);
  const explicitDbName = trimOptionalString(
    (driver.type === "persistent" ? driver.dbName : undefined) ?? config.dbName,
  );
  if (explicitDbName) {
    return explicitDbName;
  }

  const session = resolveClientSessionSync({
    appId: config.appId,
    jwtToken: config.jwtToken,
  });

  if (!session?.user_id || session.authMode === "anonymous") {
    return config.appId;
  }

  return `${config.appId}::${encodeURIComponent(session.user_id)}`;
}

export function shouldUseBrowserWasmBackend(config: DbConfig): boolean {
  return isBrowser() && resolveStorageDriver(config.driver).type === "persistent";
}

export class BrowserWasmBackend implements DbBackend<DbConfig> {
  readonly hasWorker = true;

  private workerBridge: WorkerBridge | null = null;
  private worker: Worker | null = null;
  private attachedClient: { schemaJson: string; client: JazzClient } | null = null;
  private bridgeReady: Promise<void> | null = null;
  private primaryDbName: string | null = null;
  private workerDbName: string | null = null;
  private leaderElection: TabLeaderElection | null = null;
  private leaderElectionUnsubscribe: (() => void) | null = null;
  private tabRole: LeaderRole = "follower";
  private tabId: string | null = null;
  private currentLeaderTabId: string | null = null;
  private currentLeaderTerm = 0;
  private syncChannel: BroadcastChannelLike | null = null;
  private readonly leaderPeerIds = new Set<string>();
  private activeRemoteLeaderTabId: string | null = null;
  private workerReconfigure: Promise<void> = Promise.resolve();
  private storageReset: StorageResetCoordinator | null = null;
  private lifecycleHooksAttached = false;
  private isShuttingDown = false;

  private readonly onSyncChannelMessage = (event: MessageEvent): void => {
    this.handleSyncChannelMessage(event.data);
  };
  private readonly onVisibilityChange = (): void => {
    if (typeof document === "undefined") return;
    const hidden = document.visibilityState === "hidden";
    this.sendLifecycleHint(hidden ? "visibility-hidden" : "visibility-visible");
  };
  private readonly onPageHide = (): void => {
    this.sendLifecycleHint("pagehide");
  };
  private readonly onPageFreeze = (): void => {
    this.sendLifecycleHint("freeze");
  };
  private readonly onPageResume = (): void => {
    this.sendLifecycleHint("resume");
  };

  private constructor(
    private readonly config: DbConfig,
    private readonly host: DbBackendHost,
    private readonly createClientForBackend: BrowserWasmBackendClientFactory,
  ) {}

  static async create(options: BrowserWasmBackendOptions): Promise<BrowserWasmBackend> {
    const persistentDriver = resolveStorageDriver(options.config.driver);
    if (persistentDriver.type !== "persistent") {
      throw new Error("Browser WASM backend requires driver.type='persistent'");
    }

    const backend = new BrowserWasmBackend(options.config, options.host, options.createClient);
    backend.primaryDbName = resolveDefaultPersistentDbName(options.config);
    backend.workerDbName = backend.primaryDbName;

    try {
      const election = new TabLeaderElection({
        appId: options.config.appId,
        dbName: backend.primaryDbName,
      });
      backend.leaderElection = election;
      election.start();

      let initialLeader: LeaderSnapshot | null = null;
      try {
        initialLeader = await election.waitForInitialLeader(1600);
      } catch {
        initialLeader = election.snapshot();
      }
      backend.adoptLeaderSnapshot(initialLeader);
      backend.workerDbName = BrowserWasmBackend.resolveWorkerDbNameForSnapshot(
        backend.primaryDbName,
        initialLeader,
      );
      backend.openSyncChannel();
      backend.storageReset = new StorageResetCoordinator(backend.createStorageResetHost());
      backend.attachLifecycleHooks();
      backend.leaderElectionUnsubscribe = election.onChange((snapshot) => {
        backend.onLeaderElectionChange(snapshot);
      });

      backend.worker = await BrowserWasmBackend.spawnWorker(options.config.runtimeSources);

      return backend;
    } catch (error) {
      backend.closeSyncChannel();
      backend.detachLifecycleHooks();
      if (backend.leaderElectionUnsubscribe) {
        backend.leaderElectionUnsubscribe();
        backend.leaderElectionUnsubscribe = null;
      }
      if (backend.leaderElection) {
        backend.leaderElection.stop();
        backend.leaderElection = null;
      }
      throw error;
    }
  }

  createClient(context: DbBackendClientContext<DbConfig>): JazzClient {
    const client = this.createClientForBackend({
      ...context,
      hasWorker: true,
      useBinaryEncoding: true,
    });

    if (!this.workerBridge) {
      this.attachWorkerBridge(context.schemaJson, client);
    }
    this.attachedClient ??= { schemaJson: context.schemaJson, client };

    return client;
  }

  async ensureReady(): Promise<void> {
    await this.workerReconfigure;
    if (this.bridgeReady) {
      await this.bridgeReady;
    }
  }

  async waitForUpstreamServerConnection(): Promise<void> {
    await this.workerBridge?.waitForUpstreamServerConnection();
  }

  updateAuth(auth: { jwtToken?: string }): void {
    this.workerBridge?.updateAuth(auth);
  }

  acknowledgeRejectedBatch(batchId: string): void {
    this.workerBridge?.acknowledgeRejectedBatch(batchId);
  }

  async deleteClientStorage(): Promise<void> {
    const coordinator = this.storageReset;
    if (!coordinator) {
      throw new Error("deleteClientStorage() requires an initialized storage-reset coordinator.");
    }
    const operation = this.workerReconfigure.then(async () => {
      await coordinator.requestReset();
    });

    this.workerReconfigure = operation.then(
      () => undefined,
      () => undefined,
    );

    await operation;
  }

  async shutdown(): Promise<void> {
    this.isShuttingDown = true;
    this.sendFollowerClose(this.activeRemoteLeaderTabId, this.currentLeaderTerm);
    this.activeRemoteLeaderTabId = null;
    this.leaderPeerIds.clear();
    this.closeSyncChannel();
    this.detachLifecycleHooks();

    if (this.leaderElectionUnsubscribe) {
      this.leaderElectionUnsubscribe();
      this.leaderElectionUnsubscribe = null;
    }
    if (this.leaderElection) {
      this.leaderElection.stop();
      this.leaderElection = null;
    }

    await this.workerReconfigure;
    await this.ensureReady();

    if (this.workerBridge && this.worker) {
      await this.workerBridge.shutdown(this.worker);
      this.workerBridge = null;
    }

    if (this.worker) {
      this.worker.terminate();
      this.worker = null;
    }
  }

  private attachWorkerBridge(schemaJson: string, client: JazzClient): void {
    if (!this.worker) {
      throw new Error("Cannot attach worker bridge without an active worker");
    }

    const bridge = new WorkerBridge(this.worker, client.getRuntime());
    this.leaderPeerIds.clear();
    bridge.onPeerSync((batch) => {
      this.handleWorkerPeerSync(batch);
    });
    this.applyBridgeRoutingForCurrentLeader(bridge, false);
    bridge.onAuthFailure((reason) => {
      this.host.onAuthFailure(reason);
    });
    bridge.onLocalBatchRecordsSync((batches) => {
      client.hydrateLocalBatchRecords(batches);
    });
    bridge.onMutationErrorReplay((batch) => {
      this.host.onMutationErrorReplay(client, batch);
    });
    this.workerBridge = bridge;
    const bridgeReady = bridge
      .init(this.buildWorkerBridgeOptions(schemaJson))
      .then(() => undefined);
    bridgeReady.catch(() => undefined);
    this.bridgeReady = bridgeReady;
  }

  private buildWorkerBridgeOptions(schemaJson: string): WorkerBridgeOptions {
    const driver = resolveStorageDriver(this.config.driver);
    if (driver.type !== "persistent") {
      throw new Error("Worker bridge is only available for driver.type='persistent'");
    }

    const locationHref = typeof location !== "undefined" ? location.href : undefined;
    const configRuntimeSources = this.config.runtimeSources;
    const envWasmUrl =
      typeof process !== "undefined" && process.env
        ? process.env.NEXT_PUBLIC_JAZZ_WASM_URL
        : undefined;
    const hasConfiguredSource =
      !!configRuntimeSources?.wasmUrl ||
      !!configRuntimeSources?.baseUrl ||
      !!configRuntimeSources?.workerUrl ||
      !!resolveRuntimeConfigSyncInitInput(configRuntimeSources);
    const runtimeSources =
      hasConfiguredSource || !envWasmUrl || typeof location === "undefined"
        ? configRuntimeSources
        : {
            ...configRuntimeSources,
            wasmUrl: new URL(envWasmUrl, location.href).href,
          };

    let fallbackWasmUrl: string | undefined;
    if (!runtimeSources?.workerUrl && !runtimeSources?.baseUrl && !runtimeSources?.wasmUrl) {
      if (!resolveRuntimeConfigSyncInitInput(runtimeSources)) {
        fallbackWasmUrl =
          resolveWorkerBootstrapWasmUrl(import.meta.url, locationHref, runtimeSources) ?? undefined;
      }
    }

    return {
      schemaJson,
      appId: this.config.appId,
      env: this.config.env ?? "dev",
      userBranch: this.config.userBranch ?? "main",
      dbName: this.workerDbName ?? driver.dbName ?? this.config.appId,
      serverUrl: this.config.serverUrl,
      jwtToken: this.config.jwtToken,
      adminSecret: this.config.adminSecret,
      runtimeSources,
      fallbackWasmUrl,
      logLevel: this.config.logLevel,
      telemetryCollectorUrl: this.host.getTelemetryCollectorUrl(),
    };
  }

  private adoptLeaderSnapshot(snapshot: LeaderSnapshot): void {
    this.tabRole = snapshot.role;
    this.tabId = snapshot.tabId;
    this.currentLeaderTabId = snapshot.leaderTabId;
    this.currentLeaderTerm = snapshot.term;
  }

  private openSyncChannel(): void {
    if (this.syncChannel || !this.primaryDbName) return;
    const ChannelCtor = resolveBroadcastChannelCtor();
    if (!ChannelCtor) {
      return;
    }

    const channelName = `jazz-tab-sync:${this.config.appId}:${this.primaryDbName}`;
    this.syncChannel = new ChannelCtor(channelName);
    this.syncChannel.addEventListener("message", this.onSyncChannelMessage);
  }

  private closeSyncChannel(): void {
    if (!this.syncChannel) return;
    this.syncChannel.removeEventListener("message", this.onSyncChannelMessage);
    this.syncChannel.close();
    this.syncChannel = null;
  }

  private postSyncChannelMessage(message: TabSyncMessage): void {
    this.syncChannel?.postMessage(message);
  }

  private async resumeWorker(): Promise<void> {
    if (this.worker || this.isShuttingDown || this.host.isShuttingDown()) {
      return;
    }
    this.worker = await BrowserWasmBackend.spawnWorker(this.config.runtimeSources);
  }

  private attachLifecycleHooks(): void {
    if (this.lifecycleHooksAttached) return;
    if (typeof window === "undefined" || typeof document === "undefined") return;

    document.addEventListener("visibilitychange", this.onVisibilityChange);
    window.addEventListener("pagehide", this.onPageHide);
    document.addEventListener("freeze", this.onPageFreeze as EventListener);
    document.addEventListener("resume", this.onPageResume as EventListener);
    this.lifecycleHooksAttached = true;
  }

  private detachLifecycleHooks(): void {
    if (!this.lifecycleHooksAttached) return;
    if (typeof window === "undefined" || typeof document === "undefined") return;

    document.removeEventListener("visibilitychange", this.onVisibilityChange);
    window.removeEventListener("pagehide", this.onPageHide);
    document.removeEventListener("freeze", this.onPageFreeze as EventListener);
    document.removeEventListener("resume", this.onPageResume as EventListener);
    this.lifecycleHooksAttached = false;
  }

  private sendLifecycleHint(event: WorkerLifecycleEvent): void {
    if (this.isShuttingDown || this.host.isShuttingDown() || !this.worker) return;

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

  private handleSyncChannelMessage(raw: unknown): void {
    if (this.isShuttingDown || this.host.isShuttingDown() || !this.tabId) return;
    if (!isTabSyncMessage(raw)) return;

    if (this.storageReset?.handleSyncChannelMessage(raw)) {
      return;
    }

    switch (raw.type) {
      case "follower-sync":
        this.handleFollowerSync(raw);
        return;
      case "leader-sync":
        this.handleLeaderSync(raw);
        return;
      case "follower-close":
        this.handleFollowerClose(raw);
        return;
    }
  }

  private handleFollowerSync(message: FollowerSyncMessage): void {
    if (this.tabRole !== "leader") return;
    if (!this.workerBridge) return;
    if (!this.tabId || message.toLeaderTabId !== this.tabId) return;
    if (message.term !== this.currentLeaderTerm) return;

    if (!this.leaderPeerIds.has(message.fromTabId)) {
      this.leaderPeerIds.add(message.fromTabId);
      this.workerBridge.openPeer(message.fromTabId);
    }
    this.workerBridge.sendPeerSync(message.fromTabId, message.term, message.payload);
  }

  private handleLeaderSync(message: LeaderSyncMessage): void {
    if (this.tabRole !== "follower") return;
    if (!this.workerBridge) return;
    if (!this.tabId || message.toTabId !== this.tabId) return;
    if (!this.currentLeaderTabId || message.fromLeaderTabId !== this.currentLeaderTabId) return;
    if (message.term !== this.currentLeaderTerm) return;

    for (const payload of message.payload) {
      this.workerBridge.applyIncomingServerPayload(payload);
    }
  }

  private handleFollowerClose(message: FollowerCloseMessage): void {
    if (this.tabRole !== "leader") return;
    if (!this.workerBridge) return;
    if (!this.tabId || message.toLeaderTabId !== this.tabId) return;
    if (message.term !== this.currentLeaderTerm) return;
    if (!this.leaderPeerIds.has(message.fromTabId)) return;

    this.leaderPeerIds.delete(message.fromTabId);
    this.workerBridge.closePeer(message.fromTabId);
  }

  private handleWorkerPeerSync(batch: PeerSyncBatch): void {
    if (this.isShuttingDown || this.host.isShuttingDown()) return;
    if (this.tabRole !== "leader") return;
    if (!this.tabId) return;
    if (batch.term !== this.currentLeaderTerm) return;

    this.postSyncChannelMessage({
      type: "leader-sync",
      fromLeaderTabId: this.tabId,
      toTabId: batch.peerId,
      term: batch.term,
      payload: batch.payload,
    });
  }

  private sendFollowerClose(leaderTabId: string | null, term: number): void {
    if (!leaderTabId || !this.tabId) return;
    if (leaderTabId === this.tabId) return;

    this.postSyncChannelMessage({
      type: "follower-close",
      fromTabId: this.tabId,
      toLeaderTabId: leaderTabId,
      term,
    });
  }

  private applyBridgeRoutingForCurrentLeader(
    bridge: WorkerBridge,
    replayConnection: boolean,
  ): void {
    if (this.tabRole === "leader") {
      bridge.setServerPayloadForwarder(null);
      this.activeRemoteLeaderTabId = null;
    } else {
      bridge.setServerPayloadForwarder((payload) => {
        if (!this.tabId || !this.currentLeaderTabId) return;
        if (this.currentLeaderTabId === this.tabId) return;

        this.postSyncChannelMessage({
          type: "follower-sync",
          fromTabId: this.tabId,
          toLeaderTabId: this.currentLeaderTabId,
          term: this.currentLeaderTerm,
          payload: [payload],
        });
      });
      this.activeRemoteLeaderTabId = this.currentLeaderTabId;
    }

    if (replayConnection) {
      bridge.replayServerConnection();
    }
  }

  private onLeaderElectionChange(snapshot: LeaderSnapshot): void {
    if (this.isShuttingDown || this.host.isShuttingDown() || !this.primaryDbName) return;

    const previousRole = this.tabRole;
    const previousLeaderTabId = this.currentLeaderTabId;
    const previousTerm = this.currentLeaderTerm;
    this.adoptLeaderSnapshot(snapshot);

    if (previousRole === "follower" && previousLeaderTabId !== this.currentLeaderTabId) {
      this.sendFollowerClose(previousLeaderTabId, previousTerm);
    }

    const nextDbName = BrowserWasmBackend.resolveWorkerDbNameForSnapshot(
      this.primaryDbName,
      snapshot,
    );
    const dbNameChanged = nextDbName !== this.workerDbName;
    this.workerDbName = nextDbName;

    if (!this.workerBridge) return;

    this.enqueueWorkerReconfigure(async () => {
      if (this.isShuttingDown || this.host.isShuttingDown()) return;
      if (dbNameChanged) {
        await this.restartWorkerWithCurrentDbName();
        return;
      }

      if (this.workerBridge) {
        this.applyBridgeRoutingForCurrentLeader(this.workerBridge, true);
      }
    });
  }

  private enqueueWorkerReconfigure(task: () => Promise<void>): void {
    this.workerReconfigure = this.workerReconfigure.then(task).catch((error) => {
      console.error("[db] Worker reconfigure failed:", error);
    });
  }

  private async restartWorkerWithCurrentDbName(): Promise<void> {
    const currentWorker = this.worker;
    if (!currentWorker) return;

    if (this.bridgeReady) {
      await this.bridgeReady;
    }

    if (this.workerBridge) {
      try {
        await this.workerBridge.shutdown(currentWorker);
      } catch {
        // Best effort
      }
      this.workerBridge = null;
    }
    this.bridgeReady = null;

    currentWorker.terminate();
    this.worker = await BrowserWasmBackend.spawnWorker(this.config.runtimeSources);

    if (this.attachedClient) {
      const { schemaJson, client } = this.attachedClient;
      this.attachWorkerBridge(schemaJson, client);
      if (this.bridgeReady) {
        await this.bridgeReady;
      }
    }
  }

  private currentWorkerNamespace(): string {
    const driver = resolveStorageDriver(this.config.driver);
    if (driver.type !== "persistent") {
      throw new Error("Worker namespace is only available for driver.type='persistent'");
    }
    return this.workerDbName ?? driver.dbName ?? this.config.appId;
  }

  private async shutdownWorkerAndClientsForStorageReset(): Promise<void> {
    const currentWorker = this.worker;

    if (this.workerBridge && currentWorker) {
      try {
        await this.workerBridge.shutdown(currentWorker);
      } catch {
        // Best effort: if the bridge shutdown times out, still terminate below.
      }
    }
    this.workerBridge = null;
    this.bridgeReady = null;

    await this.host.shutdownClientsForStorageReset();
    this.attachedClient = null;
    this.leaderPeerIds.clear();
    this.activeRemoteLeaderTabId = null;

    if (currentWorker) {
      currentWorker.terminate();
    }
    this.worker = null;
  }

  private createStorageResetHost(): StorageResetHost {
    return {
      isShuttingDown: () => this.isShuttingDown || this.host.isShuttingDown(),
      getTabId: () => this.tabId,
      getTabRole: () => this.tabRole,
      getCurrentLeaderTabId: () => this.currentLeaderTabId,
      getCurrentLeaderTerm: () => this.currentLeaderTerm,
      hasSyncChannel: () => this.syncChannel !== null,
      getPrimaryDbName: () => this.primaryDbName,
      getCurrentWorkerNamespace: () => this.currentWorkerNamespace(),
      postSyncChannelMessage: (message) => this.postSyncChannelMessage(message),
      ensureBridgeReady: async () => {
        if (this.bridgeReady) await this.bridgeReady;
      },
      shutdownWorkerAndClients: () => this.shutdownWorkerAndClientsForStorageReset(),
      resumeWorker: () => this.resumeWorker(),
    };
  }

  private static resolveWorkerDbNameForSnapshot(
    primaryDbName: string,
    snapshot: LeaderSnapshot,
  ): string {
    if (snapshot.role === "leader") return primaryDbName;
    return `${primaryDbName}__fallback__${snapshot.tabId}`;
  }

  private static async spawnWorker(runtimeSources?: RuntimeSourcesConfig): Promise<Worker> {
    let worker: Worker;

    if (runtimeSources?.workerUrl || runtimeSources?.baseUrl) {
      const locationHref = typeof location !== "undefined" ? location.href : undefined;
      const syncInitInput = resolveRuntimeConfigSyncInitInput(runtimeSources);
      const wasmUrl = syncInitInput
        ? null
        : resolveWorkerBootstrapWasmUrl(import.meta.url, locationHref, runtimeSources);
      const workerUrl = appendWorkerRuntimeWasmUrl(
        resolveRuntimeConfigWorkerUrl(import.meta.url, locationHref, runtimeSources),
        wasmUrl,
      );
      worker = new Worker(workerUrl, { type: "module" });
    } else {
      worker = new Worker(new URL("../worker/jazz-worker.js", import.meta.url), {
        type: "module",
      });
    }

    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("Worker bootstrap timeout")), 15000);
      const handler = (event: MessageEvent) => {
        if (event.data.type === "ready") {
          clearTimeout(timeout);
          worker.removeEventListener("message", handler);
          resolve();
        } else if (event.data.type === "error") {
          clearTimeout(timeout);
          worker.removeEventListener("message", handler);
          reject(new Error(event.data.message));
        }
      };
      worker.addEventListener("message", handler);
      worker.addEventListener("error", (e) => {
        clearTimeout(timeout);
        reject(new Error(`Worker load error: ${e.message}`));
      });
    });

    return worker;
  }
}
