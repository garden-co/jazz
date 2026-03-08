/**
 * High-level database class for typed queries and mutations.
 *
 * Connects QueryBuilder to JazzClient for actual query execution.
 * Handles query translation, execution, and result transformation.
 *
 * Key design:
 * - createDb() is async (pre-loads WASM module)
 * - insert/update/delete are sync (local-first immediate writes, no durability wait)
 * - insertDurable/updateDurable/deleteDurable are async (durability-aware, return Promises)
 * - all/one are async (need storage I/O for queries)
 */

import type { WasmSchema, WasmRow, StorageDriver } from "../drivers/types.js";
import { normalizeRuntimeSchema, serializeRuntimeSchema } from "../drivers/schema-wire.js";
import type { Session } from "./context.js";
import {
  JazzClient,
  loadWasmModule,
  type WasmModule,
  type DurabilityTier,
  type QueryExecutionOptions,
  type QueryPropagation,
} from "./client.js";
import { WorkerBridge, type PeerSyncBatch, type WorkerBridgeOptions } from "./worker-bridge.js";
import { translateQuery } from "./query-adapter.js";
import { transformRow, transformRows } from "./row-transformer.js";
import { toValueArray, toUpdateRecord } from "./value-converter.js";
import { SubscriptionManager, type SubscriptionDelta } from "./subscription-manager.js";
import { resolveLocalAuthDefaults } from "./local-auth.js";
import { analyzeRelations } from "../codegen/relation-analyzer.js";
import { TabLeaderElection, type LeaderRole, type LeaderSnapshot } from "./tab-leader-election.js";
import type { WorkerLifecycleEvent } from "../worker/worker-protocol.js";
import { normalizeBuiltQuery } from "./query-builder-shape.js";

type WasmLogLevel = "error" | "warn" | "info" | "debug" | "trace";
const DEFAULT_WASM_LOG_LEVEL: WasmLogLevel = "warn";

function setGlobalWasmLogLevel(level?: WasmLogLevel): void {
  (globalThis as any).__JAZZ_WASM_LOG_LEVEL = level ?? DEFAULT_WASM_LOG_LEVEL;
}

/**
 * Configuration for creating a Db instance.
 */
export interface DbConfig {
  /** Application identifier (used for isolation) */
  appId: string;
  /** Storage driver mode (defaults to persistent). */
  driver?: StorageDriver;
  /** Optional server URL for sync */
  serverUrl?: string;
  /** Optional route prefix for multi-tenant servers (e.g. `/apps/<appId>`). */
  serverPathPrefix?: string;
  /** Environment (e.g., "dev", "prod") */
  env?: string;
  /** User branch name (default: "main") */
  userBranch?: string;
  /** JWT token for server authentication */
  jwtToken?: string;
  /**
   * Local auth mode for client-generated identities.
   *
   * Browser clients default to `"anonymous"` when no other auth is configured.
   */
  localAuthMode?: "anonymous" | "demo";
  /**
   * Client-generated auth token for anonymous/demo identity.
   *
   * If omitted while local auth is active in browser, Jazz generates and
   * persists a per-app device token in localStorage.
   */
  localAuthToken?: string;
  /** Admin secret for catalogue sync */
  adminSecret?: string;
  /** Database name for OPFS persistence (browser only, default: appId) */
  dbName?: string;
  /** Optional WASM tracing level for benchmark/debug scenarios (default: "warn"). */
  logLevel?: WasmLogLevel;
}

function resolveStorageDriver(driver?: StorageDriver): StorageDriver {
  return driver ?? { type: "persistent" };
}

/**
 * Interface that QueryBuilder classes implement.
 * Generated builders expose these internal properties for Db to use.
 */
export interface QueryBuilder<T> {
  /** Table name for this query */
  readonly _table: string;
  /** Schema reference for translation and transformation */
  readonly _schema: WasmSchema;
  /** Build and return the query as JSON */
  _build(): string;
  /** @internal Phantom brand — enables TypeScript to infer T from usage */
  readonly _rowType: T;
}

export interface QueryOptions extends QueryExecutionOptions {
  propagation?: QueryPropagation;
}

function resolveHopOutputTable(
  schema: WasmSchema,
  startTable: string,
  hops: readonly string[],
): string {
  if (hops.length === 0) {
    return startTable;
  }
  const relations = analyzeRelations(schema);
  let currentTable = startTable;
  for (const hopName of hops) {
    const candidates = relations.get(currentTable) ?? [];
    const relation = candidates.find((candidate) => candidate.name === hopName);
    if (!relation) {
      throw new Error(`Unknown relation "${hopName}" on table "${currentTable}"`);
    }
    currentTable = relation.toTable;
  }
  return currentTable;
}

function resolveSchemaWithTable(
  preferredSchema: WasmSchema,
  fallbackSchema: WasmSchema,
  tableName: string,
): WasmSchema {
  return preferredSchema[tableName] ? preferredSchema : fallbackSchema;
}

/**
 * Interface for table proxies used with mutations.
 * Generated table constants implement this interface.
 *
 * @typeParam T - The row type (e.g., `{ id: string; title: string; done: boolean }`)
 * @typeParam Init - The init type for inserts (e.g., `{ title: string; done: boolean }`)
 */
export interface TableProxy<T, Init> {
  /** Table name */
  readonly _table: string;
  /** Schema reference */
  readonly _schema: WasmSchema;
  /** @internal Phantom brand — enables TypeScript to infer T from usage */
  readonly _rowType: T;
  /** @internal Phantom brand — enables TypeScript to infer Init from usage */
  readonly _initType: Init;
}

interface BroadcastChannelLike {
  postMessage(data: unknown): void;
  addEventListener(type: "message", listener: (event: MessageEvent) => void): void;
  removeEventListener(type: "message", listener: (event: MessageEvent) => void): void;
  close(): void;
}

interface FollowerSyncMessage {
  type: "follower-sync";
  fromTabId: string;
  toLeaderTabId: string;
  term: number;
  payload: Uint8Array[];
}

interface LeaderSyncMessage {
  type: "leader-sync";
  fromLeaderTabId: string;
  toTabId: string;
  term: number;
  payload: Uint8Array[];
}

interface FollowerCloseMessage {
  type: "follower-close";
  fromTabId: string;
  toLeaderTabId: string;
  term: number;
}

type TabSyncMessage = FollowerSyncMessage | LeaderSyncMessage | FollowerCloseMessage;

function resolveBroadcastChannelCtor(): (new (name: string) => BroadcastChannelLike) | null {
  const ctor = (globalThis as { BroadcastChannel?: unknown }).BroadcastChannel;
  if (typeof ctor !== "function") return null;
  return ctor as new (name: string) => BroadcastChannelLike;
}

function isBinaryPayloadArray(value: unknown): value is Uint8Array[] {
  return Array.isArray(value) && value.every((entry) => entry instanceof Uint8Array);
}

function isTabSyncMessage(value: unknown): value is TabSyncMessage {
  if (typeof value !== "object" || value === null) return false;
  const message = value as Record<string, unknown>;

  if (message.type === "follower-sync") {
    return (
      typeof message.fromTabId === "string" &&
      typeof message.toLeaderTabId === "string" &&
      typeof message.term === "number" &&
      isBinaryPayloadArray(message.payload)
    );
  }

  if (message.type === "leader-sync") {
    return (
      typeof message.fromLeaderTabId === "string" &&
      typeof message.toTabId === "string" &&
      typeof message.term === "number" &&
      isBinaryPayloadArray(message.payload)
    );
  }

  if (message.type === "follower-close") {
    return (
      typeof message.fromTabId === "string" &&
      typeof message.toLeaderTabId === "string" &&
      typeof message.term === "number"
    );
  }

  return false;
}

function isLeaderDebugEnabled(): boolean {
  const globalFlag = (globalThis as { __JAZZ_LEADER_DEBUG__?: unknown }).__JAZZ_LEADER_DEBUG__;
  if (globalFlag === true) return true;

  try {
    if (typeof localStorage !== "undefined") {
      return localStorage.getItem("jazz:leader-debug") === "1";
    }
  } catch {
    // Ignore storage access errors (e.g. privacy mode / unavailable storage).
  }

  return false;
}

/**
 * High-level database interface for typed queries and mutations.
 *
 * Usage:
 * ```typescript
 * const db = await createDb({ appId: "my-app", driver });
 *
 * // Mutations
 * const inserted = db.insert(app.todos, { title: "Buy milk", done: false });
 * db.update(app.todos, inserted.id, { done: true });
 * db.delete(app.todos, inserted.id);
 *
 * // Async queries (need storage I/O)
 * const todos = await db.all(app.todos.where({ done: false }));
 * const todo = await db.one(app.todos.where({ id: inserted.id }));
 *
 * // Subscriptions
 * const unsubscribe = db.subscribeAll(app.todos, (delta) => {
 *   console.log("All todos:", delta.all);
 *   console.log("Changes:", delta.delta);
 * });
 * ```
 */
export class Db {
  private clients = new Map<string, JazzClient>();
  private config: DbConfig;
  private wasmModule: WasmModule | null;
  private workerBridge: WorkerBridge | null = null;
  private worker: Worker | null = null;
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
  private isShuttingDown = false;
  private lifecycleHooksAttached = false;
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

  /**
   * Protected constructor - use createDb() in regular app code.
   */
  protected constructor(config: DbConfig, wasmModule: WasmModule | null) {
    this.config = config;
    this.wasmModule = wasmModule;
  }

  /**
   * Create a Db instance with pre-loaded WASM module.
   * @internal Use createDb() instead.
   */
  static async create(config: DbConfig): Promise<Db> {
    const wasmModule = await loadWasmModule();
    return new Db(config, wasmModule);
  }

  /**
   * Create a Db instance backed by a dedicated worker with OPFS persistence.
   *
   * The main thread runs an in-memory WASM runtime.
   * The worker runs a persistent WASM runtime (OPFS).
   * WorkerBridge wires them together via postMessage.
   *
   * @internal Use createDb() instead — it auto-detects browser.
   */
  static async createWithWorker(config: DbConfig): Promise<Db> {
    const wasmModule = await loadWasmModule();
    const db = new Db(config, wasmModule);
    const persistentDriver = resolveStorageDriver(config.driver);
    if (persistentDriver.type !== "persistent") {
      throw new Error("Worker-backed Db requires driver.type='persistent'");
    }
    db.primaryDbName = persistentDriver.dbName ?? config.appId;
    db.workerDbName = db.primaryDbName;

    try {
      const election = new TabLeaderElection({
        appId: config.appId,
        dbName: db.primaryDbName,
      });
      db.leaderElection = election;
      election.start();

      let initialLeader: LeaderSnapshot | null = null;
      try {
        // Allow at least one startup election window with default heartbeat settings.
        initialLeader = await election.waitForInitialLeader(1600);
      } catch {
        // Fall back to whatever state election has reached so far.
        initialLeader = election.snapshot();
      }
      db.adoptLeaderSnapshot(initialLeader);
      db.workerDbName = Db.resolveWorkerDbNameForSnapshot(db.primaryDbName, initialLeader);
      db.logLeaderDebug("initial-election");
      db.openSyncChannel();
      db.attachLifecycleHooks();
      db.leaderElectionUnsubscribe = election.onChange((snapshot) => {
        db.onLeaderElectionChange(snapshot);
      });

      db.worker = await Db.spawnWorker();

      return db;
    } catch (error) {
      db.closeSyncChannel();
      db.detachLifecycleHooks();
      if (db.leaderElectionUnsubscribe) {
        db.leaderElectionUnsubscribe();
        db.leaderElectionUnsubscribe = null;
      }
      if (db.leaderElection) {
        db.leaderElection.stop();
        db.leaderElection = null;
      }
      throw error;
    }
  }

  /**
   * Get or create a JazzClient for the given schema.
   * Synchronous because WASM module is pre-loaded.
   *
   * In worker mode, the first call per schema also initializes the
   * WorkerBridge (async). Subsequent calls are sync.
   */
  protected getClient(schema: WasmSchema): JazzClient {
    if (!this.wasmModule) {
      throw new Error("Db runtime module is not initialized for this Db implementation");
    }

    // Use stringified schema as cache key
    const key = serializeRuntimeSchema(schema);

    if (!this.clients.has(key)) {
      setGlobalWasmLogLevel(this.config.logLevel);

      // Create in-memory runtime (works for both direct and worker mode)
      const client = JazzClient.connectSync(
        this.wasmModule,
        {
          appId: this.config.appId,
          schema,
          driver: this.config.driver,
          // In worker mode, don't connect to server directly — worker handles it
          serverUrl: this.worker ? undefined : this.config.serverUrl,
          serverPathPrefix: this.worker ? undefined : this.config.serverPathPrefix,
          env: this.config.env,
          userBranch: this.config.userBranch,
          jwtToken: this.config.jwtToken,
          localAuthMode: this.config.localAuthMode,
          localAuthToken: this.config.localAuthToken,
          adminSecret: this.config.adminSecret,
          tier: this.worker ? undefined : "worker",
          // Keep worker-bridged browser clients on worker durability by default.
          // For direct (non-worker) clients connected to a server, default to edge.
          defaultDurabilityTier: this.worker
            ? undefined
            : this.config.serverUrl
              ? "edge"
              : undefined,
        },
        {
          // Worker-bridged runtimes exchange postcard payloads with peers;
          // direct browser/server routing keeps JSON payloads.
          useBinaryEncoding: this.worker !== null,
        },
      );

      // In worker mode, set up the bridge for this client
      if (this.worker && !this.workerBridge) {
        this.attachWorkerBridge(key, client);
      }

      this.clients.set(key, client);
    }

    return this.clients.get(key)!;
  }

  /**
   * Wait for the worker bridge to be initialized (if in worker mode).
   * No-op if not using a worker.
   */
  private async ensureBridgeReady(): Promise<void> {
    await this.workerReconfigure;
    if (this.bridgeReady) {
      await this.bridgeReady;
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
    this.workerBridge = bridge;
    this.bridgeReady = bridge.init(this.buildWorkerBridgeOptions(schemaJson)).then(() => undefined);
  }

  private buildWorkerBridgeOptions(schemaJson: string): WorkerBridgeOptions {
    const driver = resolveStorageDriver(this.config.driver);
    if (driver.type !== "persistent") {
      throw new Error("Worker bridge is only available for driver.type='persistent'");
    }

    return {
      schemaJson,
      appId: this.config.appId,
      env: this.config.env ?? "dev",
      userBranch: this.config.userBranch ?? "main",
      dbName: this.workerDbName ?? driver.dbName ?? this.config.appId,
      serverUrl: this.config.serverUrl,
      serverPathPrefix: this.config.serverPathPrefix,
      jwtToken: this.config.jwtToken,
      localAuthMode: this.config.localAuthMode,
      localAuthToken: this.config.localAuthToken,
      adminSecret: this.config.adminSecret,
      logLevel: this.config.logLevel,
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
      this.logLeaderDebug("sync-channel-unavailable");
      return;
    }

    const channelName = `jazz-tab-sync:${this.config.appId}:${this.primaryDbName}`;
    this.syncChannel = new ChannelCtor(channelName);
    this.syncChannel.addEventListener("message", this.onSyncChannelMessage);
    this.logLeaderDebug("sync-channel-open", {
      channelName,
    });
  }

  private closeSyncChannel(): void {
    if (!this.syncChannel) return;
    this.syncChannel.removeEventListener("message", this.onSyncChannelMessage);
    this.syncChannel.close();
    this.syncChannel = null;
    this.logLeaderDebug("sync-channel-close");
  }

  private postSyncChannelMessage(message: TabSyncMessage): void {
    this.syncChannel?.postMessage(message);
  }

  private attachLifecycleHooks(): void {
    if (this.lifecycleHooksAttached) return;
    if (typeof window === "undefined" || typeof document === "undefined") return;

    document.addEventListener("visibilitychange", this.onVisibilityChange);
    window.addEventListener("pagehide", this.onPageHide);
    // "freeze"/"resume" are non-standard but available in Chromium lifecycle APIs.
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
    if (this.isShuttingDown || !this.worker) return;
    this.logLeaderDebug("lifecycle-hint", { event });

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

  private logLeaderDebug(event: string, extra?: Record<string, unknown>): void {
    if (!isLeaderDebugEnabled()) return;
    console.info("[db:leader]", event, {
      tabId: this.tabId,
      role: this.tabRole,
      term: this.currentLeaderTerm,
      leaderTabId: this.currentLeaderTabId,
      workerDbName: this.workerDbName,
      ...extra,
    });
  }

  private handleSyncChannelMessage(raw: unknown): void {
    if (this.isShuttingDown || !this.tabId) return;
    if (!isTabSyncMessage(raw)) return;

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
      this.logLeaderDebug("peer-open", {
        peerId: message.fromTabId,
      });
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
    this.logLeaderDebug("peer-close", {
      peerId: message.fromTabId,
    });
  }

  private handleWorkerPeerSync(batch: PeerSyncBatch): void {
    if (this.isShuttingDown) return;
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

    this.logLeaderDebug("follower-close", {
      toLeaderTabId: leaderTabId,
      closeTerm: term,
    });

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
      this.logLeaderDebug("upstream-mode", {
        mode: "leader-direct",
      });
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
      this.logLeaderDebug("upstream-mode", {
        mode: "follower-via-leader",
        upstreamLeaderTabId: this.currentLeaderTabId,
      });
    }

    if (replayConnection) {
      bridge.replayServerConnection();
      this.logLeaderDebug("upstream-replay");
    }
  }

  private onLeaderElectionChange(snapshot: LeaderSnapshot): void {
    if (this.isShuttingDown || !this.primaryDbName) return;

    const previousRole = this.tabRole;
    const previousLeaderTabId = this.currentLeaderTabId;
    const previousTerm = this.currentLeaderTerm;
    this.adoptLeaderSnapshot(snapshot);
    this.logLeaderDebug("leader-change", {
      previousRole,
      previousLeaderTabId,
      previousTerm,
    });

    if (previousRole === "follower" && previousLeaderTabId !== this.currentLeaderTabId) {
      this.sendFollowerClose(previousLeaderTabId, previousTerm);
    }

    const nextDbName = Db.resolveWorkerDbNameForSnapshot(this.primaryDbName, snapshot);
    const dbNameChanged = nextDbName !== this.workerDbName;
    this.workerDbName = nextDbName;

    // No bridge means no runtime server edge exists yet.
    if (!this.workerBridge) return;

    this.enqueueWorkerReconfigure(async () => {
      if (this.isShuttingDown) return;
      if (dbNameChanged) {
        this.logLeaderDebug("worker-restart", {
          reason: "db-name-change",
        });
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

    // If bridge init is in flight, wait before tearing down.
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
    this.worker = await Db.spawnWorker();

    // Re-attach immediately for existing client runtime(s) so subscriptions replay.
    const first = this.clients.entries().next();
    if (!first.done) {
      const [schemaJson, client] = first.value;
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
        // Best effort: if the bridge shutdown times out, we still terminate below.
      }
    }
    this.workerBridge = null;
    this.bridgeReady = null;

    for (const client of this.clients.values()) {
      await client.shutdown();
    }
    this.clients.clear();
    this.leaderPeerIds.clear();
    this.activeRemoteLeaderTabId = null;

    if (currentWorker) {
      currentWorker.terminate();
    }
    this.worker = null;
  }

  private async removeOpfsNamespaceFile(namespace: string): Promise<void> {
    const rootDirectory = await navigator.storage.getDirectory();
    const fileName = `${namespace}.opfsbtree`;
    try {
      await rootDirectory.removeEntry(fileName, { recursive: false });
    } catch (error) {
      const name = (error as { name?: string } | undefined)?.name;
      if (name === "NotFoundError") {
        return;
      }
      if (name === "NoModificationAllowedError" || name === "InvalidStateError") {
        throw new Error(
          `Failed to delete browser storage for "${namespace}" because OPFS is locked by another tab. Close other tabs and retry.`,
        );
      }
      throw new Error(
        `Failed to delete browser storage for "${namespace}": ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  }

  private static resolveWorkerDbNameForSnapshot(
    primaryDbName: string,
    snapshot: LeaderSnapshot,
  ): string {
    if (snapshot.role === "leader") return primaryDbName;
    return `${primaryDbName}__fallback__${snapshot.tabId}`;
  }

  private static async spawnWorker(): Promise<Worker> {
    const worker = new Worker(new URL("../worker/jazz-worker.js", import.meta.url), {
      type: "module",
    });

    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("Worker WASM load timeout")), 15000);
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

  getConfig(): DbConfig {
    // Return a copy of the config to avoid editing the original config.
    return structuredClone(this.config);
  }

  /**
   * Insert a new row into a table without waiting for durability.
   *
   * @param table Table proxy from generated app module
   * @param data Init object with column values
   * @returns Inserted row
   */
  insert<T, Init>(table: TableProxy<T, Init>, data: Init): T {
    const client = this.getClient(table._schema);
    // Don't wait for bridge to be ready in worker mode. Inserts will be propagated once the bridge is ready.
    // If the bridge fails to initialize, the insert will be lost on restart.
    const values = toValueArray(data as Record<string, unknown>, table._schema, table._table);
    const row = client.create(table._table, values);
    return transformRow(row, table._schema, table._table);
  }

  /**
   * Insert a new row into a table and wait for durability at the requested tier.
   *
   * @param table Table proxy from generated app module
   * @param data Init object with column values
   * @param options Durability tier
   * @returns Promise resolving to the inserted row
   */
  async insertDurable<T, Init>(
    table: TableProxy<T, Init>,
    data: Init,
    options: { tier: DurabilityTier },
  ): Promise<T> {
    const client = this.getClient(table._schema);
    const inputSchema = resolveSchemaWithTable(
      table._schema,
      normalizeRuntimeSchema(client.getSchema()),
      table._table,
    );
    await this.ensureBridgeReady();
    const values = toValueArray(data as Record<string, unknown>, inputSchema, table._table);
    const row = await client.createDurable(table._table, values, options);
    return transformRow(row, table._schema, table._table);
  }

  /**
   * Update an existing row without waiting for durability.
   */
  update<T, Init>(table: TableProxy<T, Init>, id: string, data: Partial<Init>): void {
    const client = this.getClient(table._schema);
    const updates = toUpdateRecord(data as Record<string, unknown>, table._schema, table._table);
    client.update(id, updates);
  }

  /**
   * Update an existing row and wait for durability at the requested tier.
   */
  async updateDurable<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
    options?: { tier?: DurabilityTier },
  ): Promise<void> {
    const client = this.getClient(table._schema);
    const inputSchema = resolveSchemaWithTable(
      table._schema,
      normalizeRuntimeSchema(client.getSchema()),
      table._table,
    );
    await this.ensureBridgeReady();
    const updates = toUpdateRecord(data as Record<string, unknown>, inputSchema, table._table);
    await client.updateDurable(id, updates, options);
  }

  /**
   * Delete a row without waiting for durability.
   */
  delete<T, Init>(table: TableProxy<T, Init>, id: string): void {
    const client = this.getClient(table._schema);
    client.delete(id);
  }

  /**
   * Delete a row and wait for durability at the requested tier.
   */
  async deleteDurable<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    options?: { tier?: DurabilityTier },
  ): Promise<void> {
    const client = this.getClient(table._schema);
    await this.ensureBridgeReady();
    await client.deleteDurable(id, options);
  }

  /**
   * Delete browser OPFS storage for this Db's active namespace and reopen a clean worker.
   *
   * This only deletes `${namespace}.opfsbtree` for the current namespace and does not touch
   * localStorage-based auth or synthetic-user state.
   *
   * Behavior:
   * - Browser worker-backed Db only (throws in non-browser/non-worker runtimes)
   * - Leader tab only (throws on follower tabs and asks to close other tabs)
   * - Serializes with worker reconfigure operations
   * - Tears down worker + clients, deletes OPFS file, respawns worker
   * - If file deletion fails, still respawns worker and then rethrows the deletion error
   */
  async deleteClientStorage(): Promise<void> {
    if (resolveStorageDriver(this.config.driver).type !== "persistent") {
      throw new Error("deleteClientStorage() is only available when driver.type='persistent'.");
    }

    if (!isBrowser()) {
      console.error(
        "deleteClientStorage() is only available on browser worker-backed Db instances.",
      );
      return;
    }

    const operation = this.workerReconfigure.then(async () => {
      if (this.tabRole !== "leader") {
        console.error(
          "deleteClientStorage() can only run from the leader tab. Close other tabs and retry.",
        );
        return;
      }

      const namespace = this.currentWorkerNamespace();

      // Wait for any in-flight bridge init before we tear down worker state.
      if (this.bridgeReady) {
        await this.bridgeReady;
      }

      await this.shutdownWorkerAndClientsForStorageReset();

      let deleteError: unknown = null;
      try {
        await this.removeOpfsNamespaceFile(namespace);
      } catch (error) {
        deleteError = error;
      }

      this.worker = await Db.spawnWorker();

      if (deleteError) {
        throw deleteError;
      }
    });

    this.workerReconfigure = operation.then(
      () => undefined,
      () => undefined,
    );

    await operation;
  }

  /**
   * Execute a query and return all matching rows as typed objects.
   *
   * @param query QueryBuilder instance (e.g., app.todos.where({done: false}))
   * @returns Array of typed objects matching the query
   */
  async all<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T[]> {
    const client = this.getClient(query._schema);
    const runtimeSchema = normalizeRuntimeSchema(client.getSchema());
    const builderJson = query._build();
    const builtQuery = normalizeBuiltQuery(JSON.parse(builderJson), query._table);
    const planningSchema = resolveSchemaWithTable(query._schema, runtimeSchema, builtQuery.table);
    const outputTable =
      builtQuery.hops.length > 0
        ? resolveHopOutputTable(planningSchema, builtQuery.table, builtQuery.hops)
        : query._table;
    const outputSchema = resolveSchemaWithTable(query._schema, runtimeSchema, outputTable);
    const rows = await client.query(translateQuery(builderJson, planningSchema), options);
    const outputIncludes = builtQuery.hops.length > 0 ? {} : builtQuery.includes;
    return transformRows<T>(rows, outputSchema, outputTable, outputIncludes, builtQuery.select);
  }

  /**
   * Execute a query and return the first matching row, or null.
   *
   * @param query QueryBuilder instance
   * @param options Optional read durability options
   * @returns First matching typed object, or null if none found
   */
  async one<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T | null> {
    const results = await this.all(query, options);
    return results[0] ?? null;
  }

  /**
   * Subscribe to a query and receive updates when results change.
   *
   * The callback receives a SubscriptionDelta with:
   * - `all`: Complete current result set
   * - `delta`: Ordered list of row-level changes
   *
   * @param query QueryBuilder instance
   * @param callback Called with delta whenever results change
   * @returns Unsubscribe function
   *
   * @example
   * ```typescript
   * const unsubscribe = db.subscribeAll(app.todos, (delta) => {
   *   setTodos(delta.all);
   *   for (const change of delta.delta) {
   *     if (change.kind === 0) {
   *       console.log("New row:", change.row);
   *     }
   *   }
   * });
   *
   * // Later: stop receiving updates
   * unsubscribe();
   * ```
   */
  subscribeAll<T extends { id: string }>(
    query: QueryBuilder<T>,
    callback: (delta: SubscriptionDelta<T>) => void,
    options?: QueryOptions,
    session?: Session,
  ): () => void {
    const manager = new SubscriptionManager<T>();
    const client = this.getClient(query._schema);
    const runtimeSchema = normalizeRuntimeSchema(client.getSchema());
    const builderJson = query._build();
    const builtQuery = normalizeBuiltQuery(JSON.parse(builderJson), query._table);
    const planningSchema = resolveSchemaWithTable(query._schema, runtimeSchema, builtQuery.table);
    const outputTable =
      builtQuery.hops.length > 0
        ? resolveHopOutputTable(planningSchema, builtQuery.table, builtQuery.hops)
        : query._table;
    const outputSchema = resolveSchemaWithTable(query._schema, runtimeSchema, outputTable);
    const outputIncludes = builtQuery.hops.length > 0 ? {} : builtQuery.includes;
    const wasmQuery = translateQuery(builderJson, planningSchema);

    const transform = (row: WasmRow): T => {
      return transformRows<T>(
        [row],
        outputSchema,
        outputTable,
        outputIncludes,
        builtQuery.select,
      )[0];
    };

    const subId = client.subscribeInternal(
      wasmQuery,
      (delta) => {
        const typedDelta = manager.handleDelta(delta, transform);
        callback(typedDelta);
      },
      session,
      options,
    );

    // Return unsubscribe function
    return () => {
      client.unsubscribe(subId);
      manager.clear();
    };
  }

  /**
   * Shutdown the Db and release all resources.
   * Closes all memoized JazzClient connections and the worker.
   */
  async shutdown(): Promise<void> {
    this.isShuttingDown = true;
    this.logLeaderDebug("shutdown");
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

    // Ensure bridge init has completed before sending shutdown —
    // otherwise the worker may still be opening OPFS handles
    await this.ensureBridgeReady();

    // Shutdown worker bridge — waits for OPFS handles to be released
    if (this.workerBridge && this.worker) {
      await this.workerBridge.shutdown(this.worker);
      this.workerBridge = null;
    }

    for (const client of this.clients.values()) {
      await client.shutdown();
    }
    this.clients.clear();

    if (this.worker) {
      this.worker.terminate();
      this.worker = null;
    }
  }
}

/**
 * Check if running in a browser environment with Worker support.
 */
function isBrowser(): boolean {
  return typeof Worker !== "undefined" && typeof window !== "undefined";
}

/**
 * Create a new Db instance with the given configuration.
 *
 * This is an **async** factory function that pre-loads the WASM module.
 * After creation, local-first mutations (`insert`/`update`/`delete`) are synchronous.
 * Use the `*Durable` variants when you need a Promise that resolves at a durability tier.
 *
 * In browser environments, automatically uses a dedicated worker for
 * OPFS persistence. In Node.js, uses in-memory storage.
 *
 * @param config Database configuration
 * @returns Promise resolving to Db instance ready for queries and mutations
 *
 * @example
 * ```typescript
 * const db = await createDb({
 *   appId: "my-app",
 *   schema: mySchema,
 * });
 * ```
 */
export async function createDb(config: DbConfig): Promise<Db> {
  const resolvedConfig = resolveLocalAuthDefaults(config);
  const driver = resolveStorageDriver(resolvedConfig.driver);

  if (driver.type === "memory" && !resolvedConfig.serverUrl) {
    throw new Error("driver.type='memory' requires serverUrl.");
  }

  if (isBrowser() && driver.type === "persistent") {
    return Db.createWithWorker(resolvedConfig);
  }
  return Db.create(resolvedConfig);
}
