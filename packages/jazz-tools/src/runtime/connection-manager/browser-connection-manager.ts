import type { DurabilityTier } from "../client.js";
import { resolveClientInternalSessionSync } from "../client-session.js";
import type { Session } from "../context.js";
import { getTrustedReservedSession, setTrustedReservedSession } from "../db-internal-session.js";
import type { BrowserWorkerConnection } from "../runtime-source.js";
import { reloadAfterStorageInvalidation } from "../browser-storage-invalidation.js";
import { runCleanupSteps } from "../run-cleanup-steps.js";
import { NativeRuntimeAdapter } from "../native-runtime/native-runtime-adapter.js";
import {
  ConnectionManager,
  type ConnectionManagerClientInput,
  type DbForConnection,
} from "./types.js";
import { registerBrowserInspectorControl } from "../../dev/inspector-overlay/browser-control-registry.js";
import { assertBrowserStorageOwnerUnchanged } from "../browser-worker-config.js";

/**
 * Every persistent browser tab is an in-memory client of one SharedWorker
 * runtime. There are no tab roles, elections, ownership locks, or follower
 * handoffs; SharedWorker identity supplies the namespace-wide singleton.
 */
export class BrowserConnectionManager extends ConnectionManager {
  private connection: BrowserWorkerConnection | null = null;
  private connectionReady: Promise<void> | null = null;
  private initialExplicitOfflineStateKnown = false;
  private connectionError: Error | null = null;
  private disconnected = false;
  private readonly reconnectWaiters = new Set<() => void>();
  private transportTransition: Promise<void> = Promise.resolve();
  private storageReset: Promise<void> | null = null;
  private unregisterInspectorControl: (() => void) | null = null;

  constructor(host: DbForConnection) {
    super(host);
  }

  async start(): Promise<void> {
    // This resolves before public Db construction returns, preserving the
    // synchronous application mutation API while leasing the TxId node before
    // the foreground runtime can exist or mint a transaction.
    this.foregroundNodeLease = await this.host.runtimeSource.acquireBrowserForegroundNodeLease(
      this.host.config,
    );
  }

  protected override onClientCreated({ schema, client }: ConnectionManagerClientInput): void {
    const workerConfig = { ...this.host.config };
    setTrustedReservedSession(workerConfig, getTrustedReservedSession(this.host.config));
    const connection = this.host.runtimeSource.createBrowserWorkerConnection({
      config: workerConfig,
      schema,
      client,
      onAuthFailure: (reason) => this.host.markUnauthenticated(reason),
      onAuthRestored: () => this.host.clearAuthError(),
      onExplicitOfflineChange: (offline) => this.setExplicitOffline(connection, offline),
      onFailure: (error) => {
        if (this.connection !== connection) return;
        this.connectionError = asError(error);
      },
      onStorageReset: () => this.beginStorageReset(connection),
      onStorageInvalidated: () => this.reloadAfterStorageInvalidation(connection),
    });
    this.connection = connection;
    this.unregisterInspectorControl?.();
    this.unregisterInspectorControl = registerBrowserInspectorControl(() =>
      connection.openInspectorControlPort(),
    );
    this.initialExplicitOfflineStateKnown = false;
    this.connectionReady = connection.ready().then(
      () => {
        // The worker sends an initial transport-state event before resolving
        // follower init. Once this resolves, this manager has an authoritative
        // namespace-wide explicit-offline snapshot.
        this.initialExplicitOfflineStateKnown = true;
      },
      (error: unknown) => {
        this.connectionError = asError(error);
        // `connectionReady` is also observed by passive readiness consumers
        // (for example the initial offline-state probe). Keep it a settled
        // notification, not a detached rejected promise. Public operations
        // call ensureReady(), which rethrows this stored error below.
      },
    );
    if (this.disconnected) {
      const ready = this.connectionReady;
      void this.enqueueTransportTransition(async () => {
        await ready;
        await connection.disconnect();
      }).catch(() => undefined);
    }
  }

  async ensureReady(tier?: DurabilityTier, signal?: AbortSignal): Promise<void> {
    if (this.host.isShuttingDown) return;
    await this.storageReset;
    if (this.connectionError) throw this.connectionError;
    await this.connectionReady;
    if (this.host.isShuttingDown) return;
    if (this.connectionError) throw this.connectionError;
    if (tier !== "local") {
      for (;;) {
        while (this.disconnected) {
          await this.waitForReconnect(signal);
          if (this.host.isShuttingDown || signal?.aborted) return;
        }
        await this.transportTransition;
        if (!this.disconnected || this.host.isShuttingDown || signal?.aborted) break;
      }
    }
    if (this.host.config.serverUrl && tier !== "local") {
      await this.connection?.waitForServerConnection();
    }
  }

  shouldDeferSubscriptionStart(tier?: DurabilityTier): boolean {
    return tier === "edge" || tier === "global";
  }
  isExplicitlyOffline(): boolean {
    return this.disconnected;
  }
  override initialExplicitOfflineState(): Promise<void> | null {
    return this.initialExplicitOfflineStateKnown ? null : this.connectionReady;
  }
  async waitForReconnect(signal?: AbortSignal): Promise<void> {
    if (signal?.aborted) return;
    if (!this.disconnected) {
      await this.transportTransition;
      if (!this.disconnected) return;
    }
    await new Promise<void>((resolve) => {
      const finish = () => {
        this.reconnectWaiters.delete(finish);
        signal?.removeEventListener("abort", onAbort);
        resolve();
      };
      const onAbort = () => finish();
      signal?.addEventListener("abort", onAbort, { once: true });
      this.reconnectWaiters.add(finish);
    });
  }

  async disconnect(): Promise<void> {
    if (!this.host.config.serverUrl) {
      throw new Error("Db.disconnect() requires a configured serverUrl.");
    }
    await this.enqueueTransportTransition(async () => {
      await this.connectionReady;
      await this.connection?.disconnect();
      // Keep RemoteIfPossible strict until the worker confirms disconnect.
      this.disconnected = true;
    });
  }

  async reconnect(): Promise<void> {
    if (!this.host.config.serverUrl) {
      throw new Error("Db.reconnect() requires a configured serverUrl.");
    }
    await this.enqueueTransportTransition(async () => {
      await this.connectionReady;
      await this.connection?.reconnect(
        JSON.stringify(runtimeAuth(this.host.config)),
        runtimeSessionClaims(this.host.config),
      );
      this.disconnected = false;
    });
    if (!this.disconnected) this.resolveReconnectWaiters();
  }

  override updateAuth(auth: {
    jwtToken?: string;
    cookieSession?: Session;
    trustedReservedSession?: Session;
  }): void {
    // The persistent root belongs to the principal that opened it. Check
    // before mutating Db config or forwarding anything to the worker, so a
    // rejected Alice -> Bob switch cannot expose Alice's local rows to Bob.
    const nextConfig = { ...this.host.config, ...auth } as DbForConnection["config"];
    setTrustedReservedSession(
      nextConfig,
      "trustedReservedSession" in auth
        ? auth.trustedReservedSession
        : getTrustedReservedSession(this.host.config),
    );
    assertBrowserStorageOwnerUnchanged(this.host.config, nextConfig);
    super.updateAuth(auth);
    void this.connection?.updateAuth(
      JSON.stringify(runtimeAuth(nextConfig)),
      runtimeSessionClaims(nextConfig),
    );
  }

  async deleteClientStorage(): Promise<void> {
    await this.connectionReady;
    await this.connection?.deleteStorage();
    await this.storageReset;
  }

  override async openInspectorControlPort(): Promise<MessagePort> {
    await this.connectionReady;
    if (!this.connection) throw new Error("Shared browser runtime is not connected");
    return this.connection.openInspectorControlPort();
  }

  private beginStorageReset(connection: BrowserWorkerConnection): void {
    if (this.connection !== connection || this.storageReset) return;
    this.connection = null;
    this.connectionReady = null;
    this.initialExplicitOfflineStateKnown = false;
    const client = this.detachClient();
    client?.discard();
    this.storageReset = connection
      .shutdown()
      .then(() => undefined)
      .finally(() => {
        this.connectionError = null;
        this.storageReset = null;
      });
  }

  private reloadAfterStorageInvalidation(connection: BrowserWorkerConnection): void {
    if (this.connection !== connection) return;
    this.connectionError = new Error("IndexedDB storage was externally invalidated");
    reloadAfterStorageInvalidation();
  }

  override async shutdown(): Promise<void> {
    const connection = this.connection;
    const admissionFailed = this.connectionError !== null;
    this.connection = null;
    this.connectionReady = null;
    this.initialExplicitOfflineStateKnown = false;
    this.resolveReconnectWaiters();
    const unregisterInspectorControl = this.unregisterInspectorControl;
    this.unregisterInspectorControl = null;

    const lease = this.foregroundNodeLease;
    this.foregroundNodeLease = undefined;
    const client = this.getCurrentClient();
    await runCleanupSteps([
      () => unregisterInspectorControl?.(),
      // A rejected physical-root admission created no follower and therefore
      // has nothing to flush. Shutdown remains idempotent after a caller has
      // already received that operation-level error.
      () => (admissionFailed ? undefined : connection?.flushLocal()),
      async () => {
        if (!lease) return;
        // The native value includes identities minted for rolled-back and
        // unsubmitted writes. Any failure in this readout or its atomic worker
        // persistence retires the node rather than risking reuse.
        try {
          const runtime = client?.getRuntime();
          if (!runtime) {
            // No foreground client existed, hence no transaction identity was
            // minted after the lease was acquired.
            await lease.returnWithHighWater(lease.confirmedTxTime);
            return;
          } else if (!(runtime instanceof NativeRuntimeAdapter)) {
            await lease.retire();
            return;
          }
          await lease.returnWithHighWater(await runtime.quiesceForegroundTxTimeHighWater());
        } catch {
          await lease.retire().catch(() => undefined);
        }
      },
      () => {
        // The tab runtime is explicitly non-durable; once its worker peer has
        // flushed, graceful evaluator teardown cannot add durability and may wait
        // on suspended recursive/include work. Abandon that view and let the
        // durable worker own orderly persistence shutdown.
        this.detachClient()?.discard();
      },
      () => super.shutdown(),
      () => connection?.shutdown(),
    ]);
  }

  private resolveReconnectWaiters(): void {
    const waiters = [...this.reconnectWaiters];
    this.reconnectWaiters.clear();
    for (const resolve of waiters) resolve();
  }

  /**
   * A persistent browser namespace has one worker-owned upstream connection.
   * The initiating tab receives the RPC result too, but every attached tab
   * must make the same explicit-offline choice for RemoteIfPossible reads.
   */
  private setExplicitOffline(connection: BrowserWorkerConnection, offline: boolean): void {
    if (this.connection !== connection) return;
    this.disconnected = offline;
    if (!offline) this.resolveReconnectWaiters();
  }

  private enqueueTransportTransition(run: () => void | Promise<void>): Promise<void> {
    const transition = this.transportTransition.then(run, run);
    this.transportTransition = transition.catch(() => undefined);
    return transition;
  }
}

function runtimeAuth(config: DbForConnection["config"]): Record<string, unknown> {
  return {
    jwt_token: config.jwtToken ?? null,
    ...(config.adminSecret ? { admin_secret: config.adminSecret } : {}),
    ...(config.cookieSession ? { backend_session: config.cookieSession } : {}),
  };
}

function runtimeSessionClaims(config: DbForConnection["config"]): Record<string, unknown> {
  return (
    resolveClientInternalSessionSync({
      ...config,
      trustedReservedSession: getTrustedReservedSession(config),
    })?.claims ?? {}
  );
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}
