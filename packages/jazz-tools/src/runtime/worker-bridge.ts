/**
 * `WorkerBridge` — thin TS adapter over the Rust-owned `WasmWorkerBridge`.
 *
 * The bridge state machine, init/shutdown handshakes, listener slots, peer
 * routing, lifecycle hint forwarding, and outbox buffering all live in
 * `crates/jazz-wasm/src/worker_bridge.rs`. The Rust API takes options at
 * `attach()` time and `init()` is parameter-less. This adapter preserves
 * the historical TS surface (`new WorkerBridge(worker, runtime)` + listener
 * registration before `init(options)`) by deferring Rust attach until
 * `init()`.
 */

import type { Runtime } from "./client.js";
import type { RuntimeSourcesConfig } from "./context.js";
import type { AuthFailureReason } from "./sync-transport.js";

/** Page lifecycle hint forwarded to the worker runtime. */
export type WorkerLifecycleEvent =
  | "visibility-hidden"
  | "visibility-visible"
  | "pagehide"
  | "freeze"
  | "resume";

export interface WorkerBridgeOptions {
  schemaJson: string;
  appId: string;
  env: string;
  userBranch: string;
  dbName: string;
  serverUrl?: string;
  jwtToken?: string;
  adminSecret?: string;
  runtimeSources?: RuntimeSourcesConfig;
  fallbackWasmUrl?: string;
  logLevel?: "error" | "warn" | "info" | "debug" | "trace";
  telemetryCollectorUrl?: string;
}

export interface WorkerBridgeEndpoint {
  postMessage(message: unknown, transfer?: Transferable[]): void;
  addEventListener?(type: "message", listener: (event: MessageEvent) => void): void;
  removeEventListener?(type: "message", listener: (event: MessageEvent) => void): void;
  onmessage?: ((event: MessageEvent) => void) | null;
  start?(): void;
  close?(): void;
  terminate?(): void;
}

interface WasmBridgeHandle {
  init(): Promise<{ clientId: string }>;
  updateAuth(jwtToken?: string | null): void;
  sendLifecycleHint(event: string): void;
  setServerPayloadForwarder(
    callback:
      | ((payload: Uint8Array | string, isCatalogue: boolean, sequence: number | null) => void)
      | null,
  ): void;
  applyIncomingServerPayload(payload: Uint8Array): void;
  waitForUpstreamServerConnection(): Promise<void>;
  replayServerConnection(): void;
  disconnectUpstream(): void;
  reconnectUpstream(): void;
  simulateCrash(): Promise<void>;
  setListeners(listeners: ListenerSlots): void;
  shutdown(): Promise<void>;
  getWorkerClientId(): string | null;
}

interface RuntimeWithWorkerBridge extends Runtime {
  createWorkerBridge?(endpoint: WorkerBridgeEndpoint, options: object): WasmBridgeHandle;
}

interface ListenerSlots {
  onAuthFailure?: (reason: AuthFailureReason) => void;
}

type ServerPayloadForwarder = (payload: Uint8Array) => void;

/**
 * Thrown by `waitForUpstreamServerConnection` when
 * the bridge it was called on has been marked migrated — i.e. the leader
 * tab handed off to a different tab and the supervisor swapped the
 * underlying endpoint underneath this bridge.
 *
 * Callers see this synchronously (deterministic rejection) instead of
 * silently hanging until the Rust-side timeout silently resolves. The runtime
 * `Db` retries against the new bridge when appropriate; opaque callers
 * should re-issue the wait against the fresh bridge.
 *
 * The `code` field is stable and intended for programmatic checks; the
 * exception name and message are not.
 */
export class LeaderMigratedError extends Error {
  readonly code = "leader-migrated" as const;
  constructor(message?: string) {
    super(
      message ??
        "Worker bridge endpoint replaced (leader tab handed off). Retry against the new bridge.",
    );
    this.name = "LeaderMigratedError";
  }
}

export class WorkerBridge {
  private readonly endpoint: WorkerBridgeEndpoint;
  private readonly runtime: Runtime;
  private bridge: WasmBridgeHandle | null = null;
  private readonly listeners: ListenerSlots = {};
  private pendingForwarder: ServerPayloadForwarder | null = null;
  private clientIdPromise: Promise<string> | null = null;
  private workerClientId: string | null = null;
  private disposed = false;
  private migrated = false;
  /**
   * In-flight waiters that should reject with `LeaderMigratedError` when the
   * bridge is marked migrated. Each entry is the per-call rejector. The Rust
   * promise underlying the wait still settles internally (usually via the
   * ack timeout) and is left to garbage-collect; the JS-visible promise
   * surfaces the typed rejection immediately so callers don't hang waiting
   * on a leader that's no longer attached.
   */
  private readonly pendingWaiters = new Set<(error: LeaderMigratedError) => void>();

  constructor(endpoint: WorkerBridgeEndpoint, runtime: Runtime) {
    this.endpoint = endpoint;
    this.runtime = runtime;
  }

  init(options: WorkerBridgeOptions): Promise<string> {
    if (this.clientIdPromise) return this.clientIdPromise;
    if (this.disposed) {
      this.clientIdPromise = Promise.reject(new Error("WorkerBridge has been disposed"));
      return this.clientIdPromise;
    }

    const create = (this.runtime as RuntimeWithWorkerBridge).createWorkerBridge;
    if (typeof create !== "function") {
      this.clientIdPromise = Promise.reject(
        new Error("WorkerBridge requires a WasmRuntime with `createWorkerBridge`"),
      );
      return this.clientIdPromise;
    }

    let bridge: WasmBridgeHandle;
    try {
      bridge = create.call(this.runtime, this.endpoint, options as unknown as object);
    } catch (e: unknown) {
      const err = e instanceof Error ? e : new Error(String(e));
      this.clientIdPromise = Promise.reject(err);
      return this.clientIdPromise;
    }
    this.bridge = bridge;
    bridge.setListeners(this.listeners);
    if (this.pendingForwarder) {
      this.installForwarderInternal(this.pendingForwarder);
    }

    this.clientIdPromise = bridge
      .init()
      .then((result) => {
        this.workerClientId = result.clientId;
        return result.clientId;
      })
      .catch((error: unknown) => {
        if (error instanceof Error) throw error;
        if (typeof error === "string") throw new Error(error);
        throw new Error(String(error));
      });
    return this.clientIdPromise;
  }

  updateAuth(auth: { jwtToken?: string }): void {
    this.bridge?.updateAuth(auth.jwtToken ?? null);
  }

  sendLifecycleHint(event: WorkerLifecycleEvent): void {
    this.bridge?.sendLifecycleHint(event);
  }

  async shutdown(): Promise<void> {
    if (this.disposed) return;
    this.disposed = true;
    this.rejectPendingWaiters();
    if (this.bridge) {
      try {
        await this.bridge.shutdown();
      } finally {
        this.bridge = null;
      }
    }
  }

  /**
   * Mark this bridge as migrated (leader handoff). In-flight
   * `waitForUpstreamServerConnection` calls reject with
   * {@link LeaderMigratedError}; subsequent calls reject immediately with the
   * same error. Idempotent. Callers should retry against the fresh bridge
   * attached after the supervisor's endpoint swap.
   */
  notifyMigrated(): void {
    if (this.migrated) return;
    this.migrated = true;
    this.rejectPendingWaiters();
  }

  isMigrated(): boolean {
    return this.migrated;
  }

  private rejectPendingWaiters(): void {
    if (this.pendingWaiters.size === 0) return;
    const error = new LeaderMigratedError();
    const waiters = Array.from(this.pendingWaiters);
    this.pendingWaiters.clear();
    for (const reject of waiters) {
      try {
        reject(error);
      } catch {
        // Reject handlers run user code that shouldn't crash the loop.
      }
    }
  }

  private async raceMigration<T>(work: Promise<T>): Promise<T> {
    if (this.migrated) {
      throw new LeaderMigratedError();
    }
    let onMigrate!: (error: LeaderMigratedError) => void;
    const migration = new Promise<never>((_, reject) => {
      onMigrate = reject;
    });
    this.pendingWaiters.add(onMigrate);
    try {
      return await Promise.race([work, migration]);
    } finally {
      this.pendingWaiters.delete(onMigrate);
    }
  }

  getWorkerClientId(): string | null {
    if (this.bridge) return this.bridge.getWorkerClientId();
    return this.workerClientId;
  }

  setServerPayloadForwarder(forwarder: ServerPayloadForwarder | null): void {
    this.pendingForwarder = forwarder;
    if (!this.bridge) return;
    if (forwarder) this.installForwarderInternal(forwarder);
    else this.bridge.setServerPayloadForwarder(null);
  }

  private installForwarderInternal(forwarder: ServerPayloadForwarder): void {
    // Server-bound payloads are always binary postcard; the Rust outbox sender
    // calls the forwarder with a single `Uint8Array`.
    this.bridge?.setServerPayloadForwarder((payload) => {
      forwarder(payload as Uint8Array);
    });
  }

  async waitForUpstreamServerConnection(): Promise<void> {
    if (!this.bridge) return;
    if (this.migrated) throw new LeaderMigratedError();
    await this.raceMigration(this.bridge.waitForUpstreamServerConnection());
  }

  applyIncomingServerPayload(payload: Uint8Array): void {
    this.bridge?.applyIncomingServerPayload(payload);
  }

  replayServerConnection(): void {
    this.bridge?.replayServerConnection();
  }

  disconnectUpstream(): void {
    this.bridge?.disconnectUpstream();
  }

  reconnectUpstream(): void {
    this.bridge?.reconnectUpstream();
  }

  replayWorkerUpstreamConnection(): void {
    this.reconnectUpstream();
  }

  /** Test-only: posts `simulate-crash` so the worker releases OPFS handles
   * without a clean snapshot, and resolves on `shutdown-ok` (or after the
   * shutdown-ack timeout). Used to validate WAL replay. */
  async simulateCrash(): Promise<void> {
    if (!this.bridge) return;
    await this.bridge.simulateCrash();
  }

  onAuthFailure(listener: (reason: AuthFailureReason) => void): void {
    this.listeners.onAuthFailure = listener;
    this.bridge?.setListeners(this.listeners);
  }
}
