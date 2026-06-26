import type { WasmSchema } from "../../drivers/types.js";
import type { DurabilityTier, QueryExecutionOptions } from "../client.js";
import type { RuntimeSourcesConfig } from "../context.js";
import type { WorkerBridgeOptions, WorkerLifecycleEvent } from "../worker-bridge.js";
import { BrowserBrokerClient, type BrowserBrokerClientSnapshot } from "../browser-broker-client.js";
import {
  isStaleLeadershipId,
  stringifyError,
  type BrowserBrokerRole,
} from "../browser-broker-protocol.js";
import { acquireWebLockWithRetry, type LeaderLockLease } from "../leader-lock.js";
import {
  appendWorkerRuntimeWasmUrl,
  resolveRuntimeConfigSyncInitInput,
  resolveRuntimeConfigWorkerUrl,
  resolveWorkerBootstrapWasmUrl,
} from "../runtime-config.js";
import {
  installWasmTeardownTrapSuppressor,
  isWasmTeardownInProgress,
  isWasmTeardownTrap,
  markWasmTeardownInProgress,
} from "../wasm-teardown-trap-suppressor.js";
import { FollowerPortConnectionRole } from "./connection-roles/follower-port-connection-role.js";
import { LeaderWorkerConnectionRole } from "./connection-roles/leader-worker-connection-role.js";
import type { BrowserConnectionRole } from "./connection-roles/connection-role.js";
import type {
  ConnectionManager,
  ConnectionBridgeClientInput,
  ConnectionManagerHost,
} from "./types.js";
import {
  BROKER_STORAGE_DELETE_MAX_RETRIES,
  brokerStorageDeleteRetryDelayMs,
  createBrokerFingerprint,
  createBrowserTabId,
  currentBrokerVisibility,
  isBrokerStorageLockedError,
  resolveDefaultPersistentDbName,
  sleepMs,
} from "./browser-broker-utils.js";

interface BrokerPromotionState {
  leadershipId: number;
  cancelled: boolean;
}

function runtimeModuleUrlForWorkerAssets(): string {
  return new URL("../db.js", import.meta.url).href;
}

/**
 * Manages the connection of a browser DB. The connection depends on the tab's role
 * (see {@link BrowserBrokerRole}):
 * - the leader connects to a persistent DB in a dedicated worker
 * - followers connect to the leader via
 */
export class BrowserConnectionManager implements ConnectionManager {
  readonly hasDurablePeer = true;
  private brokerClient: BrowserBrokerClient | null = null;
  private brokerPromotion: Promise<void> | null = null;
  private activeBrokerPromotion: BrokerPromotionState | null = null;
  private tabLockLease: LeaderLockLease | null = null;
  private brokerLeaderReadyLeadershipId: number | null = null;
  private followerReady: Promise<void> | null = null;
  private resolveFollowerReady: (() => void) | null = null;
  private rejectFollowerReady: ((error: Error) => void) | null = null;
  private followerReadyResolved = false;
  private durablePathError: Error | null = null;
  private brokerSchemaFingerprint: string | null = null;
  private brokerResetSchema: WasmSchema | null = null;
  private activeRoleBridge: BrowserConnectionRole | null = null;
  private readonly dbName: string;
  private tabRole: BrowserBrokerRole = "follower";
  private tabId: string | null = null;
  private currentLeadershipId = 0;
  private workerReconfigure: Promise<void> = Promise.resolve();
  private lifecycleHooksAttached = false;
  private readonly onVisibilityChange = (): void => {
    if (typeof document === "undefined") return;
    const hidden = document.visibilityState === "hidden";
    this.brokerClient?.reportVisibility(hidden ? "hidden" : "visible");
    this.sendLifecycleHint(hidden ? "visibility-hidden" : "visibility-visible");
  };
  private readonly onPageHide = (): void => {
    markWasmTeardownInProgress();
    this.sendLifecycleHint("pagehide");
  };
  private readonly onPageFreeze = (): void => {
    this.sendLifecycleHint("freeze");
  };
  private readonly onPageResume = (): void => {
    this.sendLifecycleHint("resume");
  };

  constructor(private readonly host: ConnectionManagerHost) {
    this.dbName = resolveDefaultPersistentDbName(host.config);
  }

  async start(): Promise<void> {
    const { config } = this.host;
    this.tabId = createBrowserTabId();
    this.tabRole = "follower";

    try {
      this.attachLifecycleHooks();
      const broker = await BrowserBrokerClient.connect({
        appId: config.appId,
        dbName: this.dbName,
        tabId: this.tabId,
        fingerprint: createBrokerFingerprint(config, this.dbName),
        visibility: currentBrokerVisibility(),
        runtimeSources: config.runtimeSources,
        onBecomeLeader: (client, leadershipId, resetRequestId) => {
          this.brokerClient = client;
          const promotion = this.promoteViaBroker(leadershipId, resetRequestId);
          this.brokerPromotion = promotion;
          return promotion;
        },
        onDemote: (leadershipId) => this.demoteViaBroker(leadershipId),
        onAttachFollowerPort: (followerTabId, leadershipId, port) => {
          this.handleBrokerAttachFollowerPort(followerTabId, leadershipId, port);
        },
        onDetachFollowerPort: (followerTabId, leadershipId) => {
          this.handleBrokerDetachFollowerPort(followerTabId, leadershipId);
        },
        onUseFollowerPort: (leadershipId, port) => {
          this.handleBrokerUseFollowerPort(leadershipId, port);
        },
        onFollowerReady: (leadershipId) => {
          this.handleBrokerFollowerReady(leadershipId);
        },
        onCloseFollowerPort: (leadershipId) => {
          this.handleBrokerCloseFollowerPort(leadershipId);
        },
        onStorageResetBegin: (_requestId, leadershipId) =>
          this.prepareForBrokerStorageReset(leadershipId),
        onSchemaBlocked: (reason) => {
          this.handleBrokerSchemaBlocked(reason);
        },
        onReconnected: (client) => {
          this.handleBrokerReconnected(client);
        },
        onClosed: (error) => {
          this.handleBrokerClosed(error);
        },
      });
      this.brokerClient = broker;
      this.adoptBrokerSnapshot(broker.snapshot());
      await this.waitForInitialBrokerPromotion();
    } catch (error) {
      this.detachLifecycleHooks();
      this.releaseBrokerLeadershipResources();
      await this.brokerClient?.shutdown();
      this.brokerClient = null;
      throw error;
    }
  }

  onClientCreated(input: ConnectionBridgeClientInput): void {
    this.reportBrokerSchemaReady(input.schemaKey);
    this.activeRoleBridge?.onClientCreated(input);
  }

  async ensureReadyForQuery(options?: QueryExecutionOptions): Promise<void> {
    await this.ensureBridgeReady();
    await this.activeRoleBridge?.ensureReadyForQuery(options);
  }

  async ensureReadyForWriteWait(tier: DurabilityTier): Promise<void> {
    await this.ensureBridgeReady();
    await this.activeRoleBridge?.ensureReadyForWriteWait(tier);
  }

  updateAuth(auth: { jwtToken?: string }): void {
    this.activeRoleBridge?.updateAuth(auth);
  }

  sendLifecycleHint(event: WorkerLifecycleEvent): void {
    if (this.host.isShuttingDown) return;
    this.activeRoleBridge?.sendLifecycleHint(event);
  }

  shouldDeferSubscriptionStart(): boolean {
    return this.tabRole === "follower";
  }

  async deleteClientStorage(): Promise<void> {
    const brokerClient = this.brokerClient;
    if (!brokerClient) {
      throw new Error("deleteClientStorage() requires an initialized browser broker.");
    }
    const operation = this.workerReconfigure.then(async () => {
      await brokerClient.requestStorageReset(`storage-reset-${Date.now()}`);
    });

    this.workerReconfigure = operation.then(
      () => undefined,
      () => undefined,
    );

    await operation;
  }

  async shutdown(): Promise<void> {
    this.rejectDurablePathReady(new Error("Db shutdown"));
    this.detachLifecycleHooks();

    let shutdownError: unknown = null;

    try {
      await this.workerReconfigure;
    } catch (error) {
      shutdownError = error;
    }

    try {
      await this.activeRoleBridge?.shutdown();
    } catch (error) {
      shutdownError ??= error;
    }
    this.activeRoleBridge = null;

    this.releaseBrokerLeadershipResources();
    if (this.brokerClient) {
      try {
        await this.brokerClient.shutdown();
      } catch (error) {
        shutdownError ??= error;
      }
      this.brokerClient = null;
    }

    if (shutdownError) {
      throw shutdownError;
    }
  }

  private async ensureBridgeReady(): Promise<void> {
    await this.workerReconfigure;
    if (this.host.isShuttingDown) {
      return;
    }
    if (this.brokerClient && this.tabRole === "follower") {
      this.attachFollowerPortBridgeForExistingClient();
      await this.ensureDurablePathReadyPromise();
    } else if (this.brokerClient && this.activeBrokerPromotion) {
      await this.ensureDurablePathReadyPromise();
    }
  }

  private reportBrokerSchemaReady(schemaFingerprint: string): void {
    if (!this.brokerClient) return;

    if (this.brokerSchemaFingerprint && this.brokerSchemaFingerprint !== schemaFingerprint) {
      throw new Error(
        "Persistent browser broker mode does not support multiple schemas in one Db instance.",
      );
    }

    if (this.brokerSchemaFingerprint === schemaFingerprint) return;

    this.brokerSchemaFingerprint = schemaFingerprint;
    this.brokerClient.reportSchemaReady(schemaFingerprint);
  }

  private adoptBrokerSnapshot(snapshot: BrowserBrokerClientSnapshot): void {
    this.tabRole = snapshot.role;
    this.tabId = snapshot.tabId;
    this.currentLeadershipId = snapshot.leadershipId;
  }

  private brokerTabLockName(): string {
    return `jazz-leader-tab:${this.host.config.appId}:${this.dbName}`;
  }

  private brokerWorkerLockName(): string {
    return `jazz-leader-worker:${this.host.config.appId}:${this.dbName}`;
  }

  private async promoteViaBroker(leadershipId: number, resetRequestId?: string): Promise<void> {
    if (this.host.isShuttingDown) return;

    const promotion: BrokerPromotionState = { leadershipId, cancelled: false };
    this.activeBrokerPromotion = promotion;
    this.markDurablePathPending();

    this.closeActiveRoleBridge(undefined, {
      preserveOutbox: true,
    });
    this.currentLeadershipId = leadershipId;
    this.brokerLeaderReadyLeadershipId = null;

    try {
      const tabLockName = this.brokerTabLockName();
      const tabLockLease = await acquireWebLockWithRetry(tabLockName, {
        onLost: (reason) => {
          void this.handleBrokerLeaderLockLost(leadershipId, tabLockName, reason);
        },
      });
      if (!tabLockLease) {
        throw new Error(`Unable to acquire ${tabLockName}`);
      }
      this.tabLockLease = tabLockLease;
      if (await this.finishCancelledBrokerPromotion(promotion)) return;

      if (resetRequestId) {
        await this.deleteBrokerStorageFiles();
        if (await this.finishCancelledBrokerPromotion(promotion)) return;
      }

      const worker = await BrowserConnectionManager.spawnWorker(this.host.config.runtimeSources);
      if (await this.finishCancelledBrokerPromotion(promotion, worker)) return;
      const leaderBridge = new LeaderWorkerConnectionRole(
        this.host,
        worker,
        leadershipId,
        (schemaJson) => this.buildWorkerBridgeOptions(schemaJson),
        {
          onFollowerPortAttached: (peerId, eventLeadershipId) => {
            if (this.tabRole !== "leader") return;
            if (eventLeadershipId !== this.currentLeadershipId) return;
            if (this.activeRoleBridge !== leaderBridge) return;
            this.brokerClient?.reportFollowerPortAttached(peerId, eventLeadershipId);
          },
          onFollowerPortClosed: (peerId, eventLeadershipId) => {
            if (this.tabRole !== "leader") return;
            if (eventLeadershipId !== this.currentLeadershipId) return;
            if (this.activeRoleBridge !== leaderBridge) return;
            this.brokerClient?.reportFollowerPortClosed(peerId, eventLeadershipId);
          },
          onReady: (eventLeadershipId) => {
            if (this.activeRoleBridge !== leaderBridge) return;
            if (this.currentLeadershipId !== eventLeadershipId) return;
            this.reportBrokerLeaderReady();
            this.resolveDurablePathReady();
          },
          onFailure: (error, failedBridge, eventLeadershipId) => {
            void this.handleBrokerLeaderBridgeFailure(error, failedBridge, eventLeadershipId);
          },
        },
      );
      this.activeRoleBridge = leaderBridge;
      this.tabRole = "leader";
      if (await this.finishCancelledBrokerPromotion(promotion)) return;
      this.recreateClientAfterBrokerReset();
      this.attachActiveRoleBridgeForExistingClient();
      if (resetRequestId && !this.host.clientEntry()) {
        this.reportBrokerLeaderReady({ bridgelessStorageReset: true });
      }
    } catch (error) {
      if (await this.finishCancelledBrokerPromotion(promotion)) return;
      this.brokerClient?.reportLeaderFailed(leadershipId, stringifyError(error));
      await this.resignBrokerLeadership();
      throw error;
    } finally {
      if (this.activeBrokerPromotion === promotion) {
        this.activeBrokerPromotion = null;
      }
    }
  }

  private async waitForInitialBrokerPromotion(): Promise<void> {
    if (!this.brokerClient) return;

    if (this.brokerPromotion) {
      await this.brokerPromotion;
    }
  }

  private async resignBrokerLeadership(
    options: {
      closePendingFollowerPorts?: boolean;
      shutdown?: () => Promise<void>;
    } = {},
  ): Promise<void> {
    this.markDurablePathPending();
    this.tabRole = "follower";
    this.brokerLeaderReadyLeadershipId = null;
    const roleBridge = this.activeRoleBridge;
    if (options.closePendingFollowerPorts ?? true) {
      if (roleBridge instanceof LeaderWorkerConnectionRole) {
        roleBridge.closePendingFollowerPorts();
      }
    }
    await (options.shutdown ? options.shutdown() : this.shutdownLeaderWorker());
    this.releaseBrokerLeadershipResources();
  }

  private async demoteViaBroker(leadershipId: number): Promise<void> {
    const activePromotion = this.activeBrokerPromotion;
    const demotedActivePromotion = activePromotion?.leadershipId === leadershipId;
    if (!demotedActivePromotion && leadershipId !== this.currentLeadershipId) return;
    if (demotedActivePromotion) {
      activePromotion.cancelled = true;
    } else if (this.tabRole !== "leader") {
      return;
    }
    await this.resignBrokerLeadership();
  }

  private async handleBrokerLeaderLockLost(
    leadershipId: number,
    lockName: string,
    reason: unknown,
  ): Promise<void> {
    const activePromotion = this.activeBrokerPromotion;
    if (activePromotion?.leadershipId === leadershipId) {
      activePromotion.cancelled = true;
    } else if (leadershipId !== this.currentLeadershipId || this.tabRole !== "leader") {
      return;
    }

    const message = stringifyError(reason);
    this.brokerClient?.reportLeaderFailed(leadershipId, message || `${lockName} was lost`);
    await this.resignBrokerLeadership();
  }

  private async finishCancelledBrokerPromotion(
    promotion: BrokerPromotionState,
    worker?: Worker,
  ): Promise<boolean> {
    if (
      !promotion.cancelled &&
      !this.host.isShuttingDown &&
      this.currentLeadershipId === promotion.leadershipId
    ) {
      return false;
    }

    if (worker) {
      worker.terminate();
    }
    await this.resignBrokerLeadership();
    return true;
  }

  private async prepareForBrokerStorageReset(leadershipId: number): Promise<void> {
    if (this.host.isShuttingDown) return;
    if (leadershipId !== this.currentLeadershipId) return;

    const activePromotion = this.activeBrokerPromotion;
    if (activePromotion?.leadershipId === leadershipId) {
      activePromotion.cancelled = true;
      await this.brokerPromotion?.catch(() => undefined);
    }

    await this.resignBrokerLeadership({
      closePendingFollowerPorts: false,
      shutdown: () => this.shutdownWorkerAndClientsForStorageReset(),
    });
  }

  private reportBrokerLeaderReady(options?: { bridgelessStorageReset?: boolean }): void {
    if (!this.brokerClient || this.tabRole !== "leader") return;
    if (this.brokerLeaderReadyLeadershipId === this.currentLeadershipId) return;
    this.brokerLeaderReadyLeadershipId = this.currentLeadershipId;
    this.brokerClient.reportLeaderReady({
      leadershipId: this.currentLeadershipId,
      tabLockName: this.brokerTabLockName(),
      workerLockName: this.brokerWorkerLockName(),
      ...(options?.bridgelessStorageReset ? { bridgelessStorageReset: true } : {}),
    });
  }

  private handleBrokerAttachFollowerPort(
    followerTabId: string,
    leadershipId: number,
    port: MessagePort,
  ): void {
    if (this.tabRole !== "leader" || leadershipId !== this.currentLeadershipId) {
      port.close();
      return;
    }

    const roleBridge = this.activeRoleBridge;
    if (!(roleBridge instanceof LeaderWorkerConnectionRole)) {
      port.close();
      return;
    }
    roleBridge.attachFollowerPort(followerTabId, leadershipId, port);
  }

  private handleBrokerDetachFollowerPort(followerTabId: string, leadershipId: number): void {
    const roleBridge = this.activeRoleBridge;
    if (roleBridge instanceof LeaderWorkerConnectionRole) {
      roleBridge.detachFollowerPort(followerTabId, leadershipId);
    }
  }

  private handleBrokerUseFollowerPort(leadershipId: number, port: MessagePort): void {
    if (this.tabRole === "leader" || isStaleLeadershipId(leadershipId, this.currentLeadershipId)) {
      port.close();
      return;
    }

    this.markDurablePathPending();
    const previousFollower = this.activeRoleBridge;
    const preserveLeaderReadySignal =
      previousFollower instanceof FollowerPortConnectionRole &&
      this.currentLeadershipId === leadershipId;
    this.closeActiveRoleBridge(undefined, {
      preserveOutbox: true,
    });
    this.tabRole = "follower";
    this.currentLeadershipId = leadershipId;
    const followerBridge = new FollowerPortConnectionRole(
      leadershipId,
      port,
      {
        onReady: (eventLeadershipId) => {
          if (this.activeRoleBridge !== followerBridge) return;
          if (this.currentLeadershipId !== eventLeadershipId) return;
          this.resolveDurablePathReady();
        },
        onAuthFailure: (reason) => {
          this.host.markUnauthenticated(reason);
        },
      },
      { preserveLeaderReadySignal },
    );
    this.activeRoleBridge = followerBridge;
    this.ensureDurablePathReadyPromise();
    this.attachFollowerPortBridgeForExistingClient();
  }

  private handleBrokerFollowerReady(leadershipId: number): void {
    if (this.tabRole !== "follower") return;
    if (isStaleLeadershipId(leadershipId, this.currentLeadershipId)) return;
    this.currentLeadershipId = leadershipId;
    const roleBridge = this.activeRoleBridge;
    if (roleBridge instanceof FollowerPortConnectionRole) {
      roleBridge.markLeaderReady(leadershipId);
    }
  }

  private handleBrokerCloseFollowerPort(leadershipId: number): void {
    if (leadershipId !== this.currentLeadershipId) return;
    this.markDurablePathPending();
    this.closeActiveRoleBridge(undefined, {
      preserveOutbox: true,
    });
  }

  private ensureDurablePathReadyPromise(): Promise<void> {
    if (this.durablePathError) {
      return Promise.reject(this.durablePathError);
    }
    if (this.followerReadyResolved) {
      return Promise.resolve();
    }
    if (this.followerReady) {
      return this.followerReady;
    }

    this.followerReady = new Promise<void>((resolve, reject) => {
      this.resolveFollowerReady = resolve;
      this.rejectFollowerReady = reject;
    });
    return this.followerReady;
  }

  private markDurablePathPending(): void {
    this.durablePathError = null;
    if (this.followerReadyResolved) {
      this.followerReady = null;
      this.followerReadyResolved = false;
    }
  }

  private resolveDurablePathReady(): void {
    this.durablePathError = null;
    this.followerReadyResolved = true;
    this.resolveFollowerReady?.();
    this.followerReady = Promise.resolve();
    this.resolveFollowerReady = null;
    this.rejectFollowerReady = null;
  }

  private rejectDurablePathReady(error: Error): void {
    this.durablePathError = error;
    this.followerReadyResolved = false;
    this.rejectFollowerReady?.(error);
    this.followerReady = null;
    this.resolveFollowerReady = null;
    this.rejectFollowerReady = null;
  }

  private attachActiveRoleBridgeForExistingClient(): void {
    const clientEntry = this.host.clientEntry();
    if (!clientEntry) return;
    this.activeRoleBridge?.onClientCreated(clientEntry);
  }

  private attachFollowerPortBridgeForExistingClient(): void {
    this.attachActiveRoleBridgeForExistingClient();
  }

  private handleBrokerSchemaBlocked(reason: string): void {
    this.rejectDurablePathReady(new Error(reason));
  }

  private handleBrokerReconnected(client: BrowserBrokerClient): void {
    this.adoptBrokerSnapshot(client.snapshot());
    this.markDurablePathPending();
    if (this.brokerSchemaFingerprint) {
      client.reportSchemaReady(this.brokerSchemaFingerprint);
    }
  }

  private handleBrokerClosed(error: Error): void {
    this.rejectDurablePathReady(error);
  }

  private recreateClientAfterBrokerReset(): void {
    if (this.host.clientEntry() || !this.brokerResetSchema) return;
    const schema = this.brokerResetSchema;
    this.brokerResetSchema = null;
    this.host.recreateClient(schema);
  }

  private closeActiveRoleBridge(error?: Error, options: { preserveOutbox?: boolean } = {}): void {
    const roleBridge = this.activeRoleBridge;
    if (options.preserveOutbox && !error) {
      this.markDurablePathPending();
    }
    if (roleBridge instanceof FollowerPortConnectionRole && options.preserveOutbox) {
      roleBridge.detachForReconnect();
    } else {
      void roleBridge?.shutdown();
    }
    this.activeRoleBridge = null;

    if (error) {
      this.rejectDurablePathReady(error);
    }
  }

  private async shutdownLeaderWorker(): Promise<void> {
    const roleBridge = this.activeRoleBridge;
    if (roleBridge instanceof LeaderWorkerConnectionRole) {
      await roleBridge.shutdown();
    } else {
      await roleBridge?.shutdown();
    }
    this.activeRoleBridge = null;
  }

  private releaseBrokerLeadershipResources(): void {
    const tabLockLease = this.tabLockLease;
    this.tabLockLease = null;
    tabLockLease?.release();
  }

  private attachLifecycleHooks(): void {
    if (this.lifecycleHooksAttached) return;
    if (typeof window === "undefined" || typeof document === "undefined") return;

    installWasmTeardownTrapSuppressor();
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

  private async deleteBrokerStorageFiles(): Promise<void> {
    const rootDirectory = await navigator.storage.getDirectory();
    const namespaces = await this.collectBrokerStorageNamespaces(rootDirectory, this.dbName);
    for (const namespace of namespaces) {
      await this.removeBrokerStorageNamespace(rootDirectory, namespace);
    }
  }

  private async collectBrokerStorageNamespaces(
    rootDirectory: FileSystemDirectoryHandle,
    dbName: string,
  ): Promise<string[]> {
    const namespaces = new Set<string>([dbName]);
    const rootWithEntries = rootDirectory as FileSystemDirectoryHandle & {
      entries?: () => AsyncIterable<[string, FileSystemHandle]>;
    };
    if (typeof rootWithEntries.entries !== "function") {
      return [...namespaces];
    }

    const suffix = ".opfsbtree";
    const legacyFallbackPrefix = `${dbName}__fallback__`;
    for await (const [name] of rootWithEntries.entries()) {
      if (!name.endsWith(suffix)) continue;
      const namespace = name.slice(0, -suffix.length);
      if (namespace === dbName || namespace.startsWith(legacyFallbackPrefix)) {
        namespaces.add(namespace);
      }
    }

    return [...namespaces];
  }

  private async removeBrokerStorageNamespace(
    rootDirectory: FileSystemDirectoryHandle,
    namespace: string,
  ): Promise<void> {
    const fileName = `${namespace}.opfsbtree`;
    for (let attempt = 0; attempt <= BROKER_STORAGE_DELETE_MAX_RETRIES; attempt++) {
      try {
        await rootDirectory.removeEntry(fileName, { recursive: false });
        return;
      } catch (error) {
        const name = (error as { name?: string } | undefined)?.name;
        if (name === "NotFoundError") {
          return;
        }
        if (!isBrokerStorageLockedError(error)) {
          throw new Error(
            `Failed to delete browser storage for "${namespace}": ${stringifyError(error)}`,
          );
        }
        if (attempt === BROKER_STORAGE_DELETE_MAX_RETRIES) {
          throw new Error(
            `Failed to delete browser storage for "${namespace}" because OPFS is locked by another tab. Close other tabs and retry.`,
          );
        }
        await sleepMs(brokerStorageDeleteRetryDelayMs(attempt));
      }
    }
  }

  private async shutdownWorkerAndClientsForStorageReset(): Promise<void> {
    this.brokerResetSchema = this.host.clientEntry()?.schema ?? null;
    const roleBridge = this.activeRoleBridge;
    if (roleBridge instanceof LeaderWorkerConnectionRole) {
      await roleBridge.shutdownForStorageReset();
    } else {
      await roleBridge?.shutdown();
    }
    this.activeRoleBridge = null;
    this.brokerLeaderReadyLeadershipId = null;
    this.brokerSchemaFingerprint = null;
    await this.host.shutdownClient();
  }

  private buildWorkerBridgeOptions(schemaJson: string): WorkerBridgeOptions {
    const locationHref = typeof location !== "undefined" ? location.href : undefined;
    const runtimeModuleUrl = runtimeModuleUrlForWorkerAssets();
    const configRuntimeSources = this.host.config.runtimeSources;
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
          resolveWorkerBootstrapWasmUrl(runtimeModuleUrl, locationHref, runtimeSources) ??
          undefined;
      }
    }

    return {
      schemaJson,
      appId: this.host.config.appId,
      env: this.host.config.env ?? "dev",
      userBranch: this.host.config.userBranch ?? "main",
      dbName: this.dbName,
      serverUrl: this.host.config.serverUrl,
      jwtToken: this.host.config.jwtToken,
      adminSecret: this.host.config.adminSecret,
      runtimeSources,
      fallbackWasmUrl,
      workerLockName: this.tabRole === "leader" ? this.brokerWorkerLockName() : undefined,
      leadershipId: this.tabRole === "leader" ? this.currentLeadershipId : undefined,
      logLevel: this.host.config.logLevel,
      telemetryCollectorUrl: this.host.telemetryCollectorUrl(),
    };
  }

  private async handleBrokerLeaderBridgeFailure(
    error: unknown,
    failedBridge: LeaderWorkerConnectionRole,
    leadershipId: number,
  ): Promise<void> {
    if (this.activeRoleBridge !== failedBridge || this.currentLeadershipId !== leadershipId) return;
    if (this.brokerClient && this.tabRole === "leader") {
      this.brokerClient.reportLeaderFailed(leadershipId, stringifyError(error));
    }
    if (this.tabRole !== "leader") return;

    await this.shutdownLeaderWorker();
    this.releaseBrokerLeadershipResources();
    this.tabRole = "follower";
    this.brokerLeaderReadyLeadershipId = null;
  }

  private static async spawnWorker(runtimeSources?: RuntimeSourcesConfig): Promise<Worker> {
    let worker: Worker;
    const runtimeModuleUrl = runtimeModuleUrlForWorkerAssets();

    if (runtimeSources?.workerUrl || runtimeSources?.baseUrl) {
      const locationHref = typeof location !== "undefined" ? location.href : undefined;
      const syncInitInput = resolveRuntimeConfigSyncInitInput(runtimeSources);
      const wasmUrl = syncInitInput
        ? null
        : resolveWorkerBootstrapWasmUrl(runtimeModuleUrl, locationHref, runtimeSources);
      const workerUrl = appendWorkerRuntimeWasmUrl(
        resolveRuntimeConfigWorkerUrl(runtimeModuleUrl, locationHref, runtimeSources),
        wasmUrl,
      );
      worker = new Worker(workerUrl, { type: "module" });
    } else {
      worker = new Worker(new URL("../../worker/jazz-worker.js", import.meta.url), {
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

    worker.addEventListener("error", (e) => {
      if (!isWasmTeardownInProgress()) return;
      if (!isWasmTeardownTrap(e.message)) return;
      e.preventDefault();
    });

    return worker;
  }
}
