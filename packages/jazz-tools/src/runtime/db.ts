/**
 * High-level database class for typed queries and mutations.
 *
 * Connects QueryBuilder to JazzClient for actual query execution.
 * Handles query translation, execution, and result transformation.
 *
 * Key design: Mutations are SYNC after WASM pre-loading.
 * - createDb() is async (pre-loads WASM module)
 * - insert/update/deleteFrom are sync (operate on in-memory WASM runtime)
 * - all/one are async (need storage I/O for queries)
 */

import type { WasmSchema, WasmRow, StorageDriver } from "../drivers/types.js";
import { JazzClient, loadWasmModule, type WasmModule, type PersistenceTier } from "./client.js";
import { WorkerBridge, type PeerSyncBatch, type WorkerBridgeOptions } from "./worker-bridge.js";
import { translateQuery } from "./query-adapter.js";
import { transformRows, type IncludeSpec } from "./row-transformer.js";
import { toValueArray, toUpdateRecord } from "./value-converter.js";
import { SubscriptionManager, type SubscriptionDelta } from "./subscription-manager.js";
import { resolveLocalAuthDefaults } from "./local-auth.js";
import { analyzeRelations } from "../codegen/relation-analyzer.js";
import { TabLeaderElection, type LeaderRole, type LeaderSnapshot } from "./tab-leader-election.js";

/**
 * Configuration for creating a Db instance.
 */
export interface DbConfig {
  /** Application identifier (used for isolation) */
  appId: string;
  /** Storage driver implementation (optional — storage is in-memory by default) */
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

interface BuiltQuery {
  table?: string;
  conditions?: Array<{ column: string; op: string; value: unknown }>;
  includes?: IncludeSpec;
  orderBy?: Array<[string, "asc" | "desc"]>;
  limit?: number;
  offset?: number;
  hops?: string[];
  gather?: {
    max_depth: number;
    step_table: string;
    step_current_column: string;
    step_conditions: Array<{ column: string; op: string; value: unknown }>;
    step_hops: string[];
  };
}

type NormalizedBuiltQuery = {
  table: string;
  conditions: Array<{ column: string; op: string; value: unknown }>;
  includes: IncludeSpec;
  orderBy: Array<[string, "asc" | "desc"]>;
  limit?: number;
  offset?: number;
  hops: string[];
  gather?:
    | {
        max_depth: number;
        step_table: string;
        step_current_column: string;
        step_conditions: Array<{ column: string; op: string; value: unknown }>;
        step_hops: string[];
      }
    | undefined;
};

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function normalizeBuiltQuery(raw: BuiltQuery, fallbackTable: string): NormalizedBuiltQuery {
  const table = typeof raw.table === "string" ? raw.table : fallbackTable;
  const conditions = Array.isArray(raw.conditions)
    ? raw.conditions.filter(
        (condition): condition is { column: string; op: string; value: unknown } =>
          isPlainObject(condition) &&
          typeof condition.column === "string" &&
          typeof condition.op === "string",
      )
    : [];
  const includes = isPlainObject(raw.includes) ? (raw.includes as IncludeSpec) : {};
  const orderBy = Array.isArray(raw.orderBy)
    ? raw.orderBy.filter(
        (entry): entry is [string, "asc" | "desc"] =>
          Array.isArray(entry) &&
          entry.length === 2 &&
          typeof entry[0] === "string" &&
          (entry[1] === "asc" || entry[1] === "desc"),
      )
    : [];
  const hops = Array.isArray(raw.hops)
    ? raw.hops.filter((hop): hop is string => typeof hop === "string")
    : [];
  const gather =
    isPlainObject(raw.gather) &&
    Number.isInteger(raw.gather.max_depth) &&
    raw.gather.max_depth > 0 &&
    typeof raw.gather.step_table === "string" &&
    typeof raw.gather.step_current_column === "string" &&
    Array.isArray(raw.gather.step_conditions) &&
    Array.isArray(raw.gather.step_hops)
      ? {
          max_depth: raw.gather.max_depth,
          step_table: raw.gather.step_table,
          step_current_column: raw.gather.step_current_column,
          step_conditions: raw.gather.step_conditions.filter(
            (condition): condition is { column: string; op: string; value: unknown } =>
              isPlainObject(condition) &&
              typeof condition.column === "string" &&
              typeof condition.op === "string",
          ),
          step_hops: raw.gather.step_hops.filter((hop): hop is string => typeof hop === "string"),
        }
      : undefined;

  return {
    table,
    conditions,
    includes,
    orderBy,
    limit: typeof raw.limit === "number" ? raw.limit : undefined,
    offset: typeof raw.offset === "number" ? raw.offset : undefined,
    hops,
    gather,
  };
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
  payload: string[];
}

interface LeaderSyncMessage {
  type: "leader-sync";
  fromLeaderTabId: string;
  toTabId: string;
  term: number;
  payload: string[];
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

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((entry) => typeof entry === "string");
}

function isTabSyncMessage(value: unknown): value is TabSyncMessage {
  if (typeof value !== "object" || value === null) return false;
  const message = value as Record<string, unknown>;

  if (message.type === "follower-sync") {
    return (
      typeof message.fromTabId === "string" &&
      typeof message.toLeaderTabId === "string" &&
      typeof message.term === "number" &&
      isStringArray(message.payload)
    );
  }

  if (message.type === "leader-sync") {
    return (
      typeof message.fromLeaderTabId === "string" &&
      typeof message.toTabId === "string" &&
      typeof message.term === "number" &&
      isStringArray(message.payload)
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

/**
 * High-level database interface for typed queries and mutations.
 *
 * Usage:
 * ```typescript
 * const db = await createDb({ appId: "my-app", driver });
 *
 * // Sync mutations (after WASM is pre-loaded)
 * const id = db.insert(app.todos, { title: "Buy milk", done: false });
 * db.update(app.todos, id, { done: true });
 * db.deleteFrom(app.todos, id);
 *
 * // Async queries (need storage I/O)
 * const todos = await db.all(app.todos.where({ done: false }));
 * const todo = await db.one(app.todos.where({ id }));
 *
 * // Subscriptions
 * const unsubscribe = db.subscribeAll(app.todos, (delta) => {
 *   console.log("All todos:", delta.all);
 *   console.log("Added:", delta.added.map(({ item, index }) => ({ item, index })));
 * });
 * ```
 */
export class Db {
  private clients = new Map<string, JazzClient>();
  private config: DbConfig;
  private wasmModule: WasmModule;
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
  private readonly onSyncChannelMessage = (event: MessageEvent): void => {
    this.handleSyncChannelMessage(event.data);
  };

  /**
   * Private constructor - use createDb() factory function.
   */
  private constructor(config: DbConfig, wasmModule: WasmModule) {
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
    db.primaryDbName = config.dbName ?? config.appId;
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
        initialLeader = await election.waitForInitialLeader(600);
      } catch {
        // Fall back to whatever state election has reached so far.
        initialLeader = election.snapshot();
      }
      db.adoptLeaderSnapshot(initialLeader);
      db.workerDbName = Db.resolveWorkerDbNameForSnapshot(db.primaryDbName, initialLeader);
      db.openSyncChannel();
      db.leaderElectionUnsubscribe = election.onChange((snapshot) => {
        db.onLeaderElectionChange(snapshot);
      });

      db.worker = await Db.spawnWorker();

      return db;
    } catch (error) {
      db.closeSyncChannel();
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
  private getClient(schema: WasmSchema): JazzClient {
    // Use stringified schema as cache key
    const key = JSON.stringify(schema);

    if (!this.clients.has(key)) {
      // Create in-memory runtime (works for both direct and worker mode)
      const client = JazzClient.connectSync(this.wasmModule, {
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
      });

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
    return {
      schemaJson,
      appId: this.config.appId,
      env: this.config.env ?? "dev",
      userBranch: this.config.userBranch ?? "main",
      dbName: this.workerDbName ?? this.config.dbName ?? this.config.appId,
      serverUrl: this.config.serverUrl,
      serverPathPrefix: this.config.serverPathPrefix,
      jwtToken: this.config.jwtToken,
      localAuthMode: this.config.localAuthMode,
      localAuthToken: this.config.localAuthToken,
      adminSecret: this.config.adminSecret,
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
    if (!ChannelCtor) return;

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
    if (this.isShuttingDown || !this.primaryDbName) return;

    const previousRole = this.tabRole;
    const previousLeaderTabId = this.currentLeaderTabId;
    const previousTerm = this.currentLeaderTerm;
    this.adoptLeaderSnapshot(snapshot);

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

  private static resolveWorkerDbNameForSnapshot(
    primaryDbName: string,
    snapshot: LeaderSnapshot,
  ): string {
    if (snapshot.role === "leader") return primaryDbName;
    return `${primaryDbName}__fallback__${snapshot.tabId}`;
  }

  private static async spawnWorker(): Promise<Worker> {
    const worker = new Worker(new URL("../worker/groove-worker.js", import.meta.url), {
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

  /**
   * Insert a new row into a table.
   *
   * This is a **synchronous** operation - the row is created immediately
   * in the local WASM runtime. Sync to server happens asynchronously.
   *
   * @param table Table proxy from generated app module
   * @param data Init object with column values
   * @returns The new row's ID (UUID string)
   *
   * @example
   * ```typescript
   * const id = db.insert(app.todos, { title: "Buy milk", done: false });
   * ```
   */
  insert<T, Init>(table: TableProxy<T, Init>, data: Init): string {
    const client = this.getClient(table._schema);
    const values = toValueArray(data as Record<string, unknown>, table._schema, table._table);
    return client.create(table._table, values);
  }

  /**
   * Insert a new row and wait for acknowledgement at the specified tier.
   *
   * @param table Table proxy from generated app module
   * @param data Init object with column values
   * @param tier Acknowledgement tier to wait for
   * @returns Promise resolving to the new row's ID when the tier acknowledges
   *
   * @example
   * ```typescript
   * const id = await db.insertWithAck(app.todos, { title: "Buy milk", done: false }, "edge");
   * ```
   */
  async insertWithAck<T, Init>(
    table: TableProxy<T, Init>,
    data: Init,
    tier: PersistenceTier,
  ): Promise<string> {
    const client = this.getClient(table._schema);
    await this.ensureBridgeReady();
    const values = toValueArray(data as Record<string, unknown>, table._schema, table._table);
    return client.createWithAck(table._table, values, tier);
  }

  /**
   * @deprecated Use insertWithAck().
   */
  async insertPersisted<T, Init>(
    table: TableProxy<T, Init>,
    data: Init,
    tier: PersistenceTier,
  ): Promise<string> {
    return this.insertWithAck(table, data, tier);
  }

  /**
   * Update an existing row.
   *
   * This is a **synchronous** operation - the row is updated immediately
   * in the local WASM runtime. Sync to server happens asynchronously.
   *
   * @param table Table proxy from generated app module
   * @param id Row ID to update
   * @param data Partial object with fields to update
   *
   * @example
   * ```typescript
   * db.update(app.todos, id, { done: true });
   * ```
   */
  update<T, Init>(table: TableProxy<T, Init>, id: string, data: Partial<Init>): void {
    const client = this.getClient(table._schema);
    const updates = toUpdateRecord(data as Record<string, unknown>, table._schema, table._table);
    client.update(id, updates);
  }

  /**
   * Update an existing row and wait for acknowledgement at the specified tier.
   *
   * @param table Table proxy from generated app module
   * @param id Row ID to update
   * @param data Partial object with fields to update
   * @param tier Acknowledgement tier to wait for
   */
  async updateWithAck<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
    tier: PersistenceTier,
  ): Promise<void> {
    const client = this.getClient(table._schema);
    await this.ensureBridgeReady();
    const updates = toUpdateRecord(data as Record<string, unknown>, table._schema, table._table);
    await client.updateWithAck(id, updates, tier);
  }

  /**
   * @deprecated Use updateWithAck().
   */
  async updatePersisted<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
    tier: PersistenceTier,
  ): Promise<void> {
    await this.updateWithAck(table, id, data, tier);
  }

  /**
   * Delete a row.
   *
   * This is a **synchronous** operation - the row is deleted immediately
   * in the local WASM runtime. Sync to server happens asynchronously.
   *
   * @param table Table proxy from generated app module
   * @param id Row ID to delete
   *
   * @example
   * ```typescript
   * db.deleteFrom(app.todos, id);
   * ```
   */
  deleteFrom<T, Init>(table: TableProxy<T, Init>, id: string): void {
    const client = this.getClient(table._schema);
    client.delete(id);
  }

  /**
   * Delete a row and wait for acknowledgement at the specified tier.
   *
   * @param table Table proxy from generated app module
   * @param id Row ID to delete
   * @param tier Acknowledgement tier to wait for
   */
  async deleteFromWithAck<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    tier: PersistenceTier,
  ): Promise<void> {
    const client = this.getClient(table._schema);
    await this.ensureBridgeReady();
    await client.deleteWithAck(id, tier);
  }

  /**
   * @deprecated Use deleteFromWithAck().
   */
  async deleteFromPersisted<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    tier: PersistenceTier,
  ): Promise<void> {
    await this.deleteFromWithAck(table, id, tier);
  }

  /**
   * Execute a query and return all matching rows as typed objects.
   *
   * @param query QueryBuilder instance (e.g., app.todos.where({done: false}))
   * @returns Array of typed objects matching the query
   */
  async all<T>(query: QueryBuilder<T>, settledTier?: PersistenceTier): Promise<T[]> {
    const client = this.getClient(query._schema);
    const builderJson = query._build();
    const builtQuery = normalizeBuiltQuery(JSON.parse(builderJson) as BuiltQuery, query._table);
    const rows = await client.query(translateQuery(builderJson, query._schema), settledTier);
    const outputTable =
      builtQuery.hops.length > 0
        ? resolveHopOutputTable(query._schema, builtQuery.table, builtQuery.hops)
        : query._table;
    const outputIncludes = builtQuery.hops.length > 0 ? {} : builtQuery.includes;
    return transformRows<T>(rows, query._schema, outputTable, outputIncludes);
  }

  /**
   * Execute a query and return the first matching row, or null.
   *
   * @param query QueryBuilder instance
   * @param settledTier Optional tier to hold delivery until confirmed
   * @returns First matching typed object, or null if none found
   */
  async one<T>(query: QueryBuilder<T>, settledTier?: PersistenceTier): Promise<T | null> {
    const results = await this.all(query, settledTier);
    return results[0] ?? null;
  }

  /**
   * Subscribe to a query and receive updates when results change.
   *
   * The callback receives a SubscriptionDelta with:
   * - `all`: Complete current result set
   * - `added`: Items added in this update
   * - `updated`: Items modified in this update
   * - `removed`: Items removed in this update
   *
   * @param query QueryBuilder instance
   * @param callback Called with delta whenever results change
   * @returns Unsubscribe function
   *
   * @example
   * ```typescript
   * const unsubscribe = db.subscribeAll(app.todos, (delta) => {
   *   setTodos(delta.all);
   *   if (delta.added.length > 0) {
   *     console.log("New todos:", delta.added.map(({ item }) => item));
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
    settledTier?: PersistenceTier,
  ): () => void {
    const manager = new SubscriptionManager<T>();
    const client = this.getClient(query._schema);
    const builderJson = query._build();
    const builtQuery = normalizeBuiltQuery(JSON.parse(builderJson) as BuiltQuery, query._table);
    const outputTable =
      builtQuery.hops.length > 0
        ? resolveHopOutputTable(query._schema, builtQuery.table, builtQuery.hops)
        : query._table;
    const outputIncludes = builtQuery.hops.length > 0 ? {} : builtQuery.includes;
    const wasmQuery = translateQuery(builderJson, query._schema);

    const transform = (row: WasmRow): T => {
      return transformRows<T>([row], query._schema, outputTable, outputIncludes)[0];
    };

    const subId = client.subscribe(
      wasmQuery,
      (delta) => {
        const typedDelta = manager.handleDelta(delta, transform);
        callback(typedDelta);
      },
      settledTier,
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
    this.sendFollowerClose(this.activeRemoteLeaderTabId, this.currentLeaderTerm);
    this.activeRemoteLeaderTabId = null;
    this.leaderPeerIds.clear();
    this.closeSyncChannel();

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
 * After creation, mutations (insert/update/deleteFrom) are synchronous.
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
  if (isBrowser()) {
    return Db.createWithWorker(resolvedConfig);
  }
  return Db.create(resolvedConfig);
}
