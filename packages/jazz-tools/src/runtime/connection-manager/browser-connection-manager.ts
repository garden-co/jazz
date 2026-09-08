import { copyAccountConfigAdmission } from "../../accounts/config-capability.js";
import type { WasmSchema } from "../../drivers/types.js";
import type { DurabilityTier, JazzClient } from "../client.js";
import { resolveClientInternalSessionSync } from "../client-session.js";
import type { Session } from "../context.js";
import { getTrustedReservedSession, setTrustedReservedSession } from "../db-internal-session.js";
import type { BrowserForegroundNodeLease, BrowserWorkerConnection } from "../runtime-source.js";
import { reloadAfterStorageInvalidation } from "../browser-storage-invalidation.js";
import { runCleanupSteps } from "../run-cleanup-steps.js";
import { NativeRuntimeAdapter } from "../native-runtime/native-runtime-adapter.js";
import { BrowserWorkerUnresponsiveError } from "../native-runtime/browser-worker-protocol.js";
import {
  ConnectionManager,
  type ConnectionManagerClientInput,
  type DbForConnection,
} from "./types.js";
import { registerBrowserInspectorControl } from "../../dev/inspector-overlay/browser-control-registry.js";
import { assertBrowserStorageOwnerUnchanged } from "../browser-worker-config.js";
import { waitForInspectorOpening } from "../native-runtime/inspector-control-lifecycle.js";

/**
 * Every persistent browser tab is an in-memory client of one SharedWorker
 * runtime. There are no tab roles, elections, or follower handoffs. SharedWorker
 * identity supplies the normal namespace-wide singleton, while a physical-root
 * Web Lock fences retry generations and separately loaded worker assets.
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
  private browserConnectionInput: ConnectionManagerClientInput | null = null;
  /** A failed follower owns no recoverable port; reconnect must mint a new one. */
  private recoverableConnectionFailure = false;
  private observedConfigurationAdmissionFailure: BrowserWorkerConnection | null = null;
  private configurationAdmissionRetry: Promise<BrowserWorkerConnection | null> | null = null;
  private readonly readinessByConnection = new WeakMap<BrowserWorkerConnection, Promise<void>>();
  declare protected foregroundNodeLease: BrowserForegroundNodeLease | undefined;
  private shutdownStarted = false;
  private shutdownFailure: {
    connection: BrowserWorkerConnection | null;
    abandon: (error: Error) => void;
  } | null = null;

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

  override getClient(schema: WasmSchema): JazzClient {
    if (this.shutdownStarted) throw new Error("Browser connection manager is shut down");
    return super.getClient(schema);
  }

  protected override onClientCreated(input: ConnectionManagerClientInput): void {
    this.browserConnectionInput = input;
    this.openBrowserWorkerConnection();
  }

  private openBrowserWorkerConnection(): BrowserWorkerConnection {
    if (this.shutdownStarted) throw new Error("Browser connection manager is shut down");
    const input = this.browserConnectionInput;
    if (!input) throw new Error("Browser worker connection requires an initialized client");
    // An Inspector receipt authenticates one worker connection, not a durable
    // database coordinate. Replacing a failed follower must therefore revoke
    // the old receipt before the new generation can begin serving reads.
    this.host.clearAuthenticatedInspectorLocalReads();
    const workerConfig = { ...this.host.config };
    copyAccountConfigAdmission(this.host.config, workerConfig);
    setTrustedReservedSession(workerConfig, getTrustedReservedSession(this.host.config));
    const connection = this.host.runtimeSource.createBrowserWorkerConnection({
      config: workerConfig,
      schema: input.schema,
      client: input.client,
      onAuthFailure: (reason) => this.host.markUnauthenticated(reason),
      onAuthRestored: () => this.host.clearAuthError(),
      onExplicitOfflineChange: (offline) => this.setExplicitOffline(connection, offline),
      onFailure: (error) => this.observeConnectionFailure(connection, asError(error)),
      onStorageReset: () => this.beginStorageReset(connection),
      onStorageInvalidated: () => this.reloadAfterStorageInvalidation(connection),
    });
    this.connection = connection;
    this.observedConfigurationAdmissionFailure = null;
    this.unregisterInspectorControl?.();
    this.unregisterInspectorControl = registerBrowserInspectorControl(
      (signal) => connection.openInspectorControlPort(signal),
      () => this.host.config,
    );
    this.initialExplicitOfflineStateKnown = false;
    const readiness = connection.ready();
    this.readinessByConnection.set(connection, readiness);
    this.connectionReady = readiness.then(
      () => {
        if (this.connection !== connection) return;
        const inspectorPhysicalDbName =
          connection.getAuthenticatedInspectorAttachmentPhysicalDbName?.();
        if (inspectorPhysicalDbName) {
          this.host.enableAuthenticatedInspectorLocalReads(inspectorPhysicalDbName);
        }
        // The worker sends an initial transport-state event before resolving
        // follower init. Once this resolves, this manager has an authoritative
        // namespace-wide explicit-offline snapshot.
        this.initialExplicitOfflineStateKnown = true;
        this.connectionError = null;
        this.recoverableConnectionFailure = false;
      },
      (error: unknown) => {
        this.observeConnectionFailure(connection, asError(error));
        if (this.connection !== connection) return;
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
    return connection;
  }

  private observeConnectionFailure(connection: BrowserWorkerConnection, error: Error): void {
    if (this.shutdownFailure?.connection === connection) {
      if (error instanceof BrowserWorkerUnresponsiveError) this.shutdownFailure.abandon(error);
      return;
    }
    if (this.connection !== connection) return;
    this.connectionError = error;
    this.recoverableConnectionFailure = true;
  }

  async ensureReady(tier?: DurabilityTier, signal?: AbortSignal): Promise<void> {
    if (this.host.isShuttingDown || signal?.aborted) return;
    const reset = this.storageReset;
    // Capture before yielding: callers already waiting on a rejected attempt
    // must see that rejection, even if a later API call starts a replacement.
    let connection = reset ? null : this.connection;
    const retry = connection !== null && this.observedConfigurationAdmissionFailure === connection;
    await reset;
    if (this.host.isShuttingDown || signal?.aborted) return;
    connection ??= this.connection;
    if (retry && connection) connection = await this.retryConfigurationAdmission(connection);
    if (this.host.isShuttingDown || signal?.aborted) return;
    try {
      await (connection ? this.readinessByConnection.get(connection) : this.connectionReady);
    } catch (error) {
      if (connection === this.connection && connection?.canRetryInitialConfigurationAdmission?.()) {
        this.observedConfigurationAdmissionFailure = connection;
      }
      throw error;
    }
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

  private retryConfigurationAdmission(
    failed: BrowserWorkerConnection,
  ): Promise<BrowserWorkerConnection | null> {
    if (this.configurationAdmissionRetry) return this.configurationAdmissionRetry;
    if (failed !== this.connection) return Promise.resolve(this.connection);
    const retry = (async () => {
      await failed.shutdown();
      if (this.host.isShuttingDown || failed !== this.connection) return null;
      this.connection = null;
      this.connectionReady = null;
      this.connectionError = null;
      this.observedConfigurationAdmissionFailure = null;
      return this.openBrowserWorkerConnection();
    })();
    this.configurationAdmissionRetry = retry;
    void retry
      .finally(() => {
        if (this.configurationAdmissionRetry === retry) this.configurationAdmissionRetry = null;
      })
      .catch(() => undefined);
    return retry;
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
      this.publishExplicitOfflineState();
    });
  }

  async reconnect(): Promise<void> {
    if (!this.host.config.serverUrl) {
      throw new Error("Db.reconnect() requires a configured serverUrl.");
    }
    await this.enqueueTransportTransition(async () => {
      if (this.shutdownStarted) throw new Error("Browser connection manager is shut down");
      if (this.recoverableConnectionFailure) this.reopenFailedFollower();
      await this.connectionReady;
      if (this.connectionError) throw this.connectionError;
      const connection = this.connection;
      if (!connection) throw new Error("Browser worker connection is unavailable");
      await connection.reconnect(
        JSON.stringify(runtimeAuth(this.host.config)),
        runtimeSessionClaims(this.host.config),
      );
      this.disconnected = false;
      this.publishExplicitOfflineState();
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

  override async openInspectorControlPort(signal?: AbortSignal): Promise<MessagePort> {
    await waitForInspectorOpening(this.connectionReady ?? Promise.resolve(), signal);
    if (!this.connection) throw new Error("Shared browser runtime is not connected");
    return this.connection.openInspectorControlPort(signal);
  }

  private beginStorageReset(connection: BrowserWorkerConnection): void {
    if (this.connection !== connection || this.storageReset) return;
    this.host.clearAuthenticatedInspectorLocalReads();
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

  /**
   * A follower transport failure closes its MessagePort by design. Reusing the
   * old wrapper would only repeat its stored failure, so explicit reconnect
   * acquires a new follower against the same durable SharedWorker namespace.
   */
  private reopenFailedFollower(): void {
    if (!this.recoverableConnectionFailure) return;
    this.host.clearAuthenticatedInspectorLocalReads();
    this.connection = null;
    this.connectionReady = null;
    this.connectionError = null;
    this.initialExplicitOfflineStateKnown = false;
    this.recoverableConnectionFailure = false;
    this.openBrowserWorkerConnection();
  }

  private reloadAfterStorageInvalidation(connection: BrowserWorkerConnection): void {
    if (this.connection !== connection) return;
    this.connectionError = new Error("IndexedDB storage was externally invalidated");
    reloadAfterStorageInvalidation();
  }

  override async waitForPendingWrites(): Promise<void> {
    await this.connection?.waitForPendingWrites();
  }

  override async shutdown(): Promise<void> {
    if (this.shutdownStarted) return;
    this.shutdownStarted = true;
    const connection = this.connection;
    const admissionError = this.connectionError;
    this.connection = null;
    this.connectionReady = null;
    this.browserConnectionInput = null;
    this.initialExplicitOfflineStateKnown = false;
    this.resolveReconnectWaiters();
    const unregisterInspectorControl = this.unregisterInspectorControl;
    this.unregisterInspectorControl = null;

    const lease = this.foregroundNodeLease;
    this.foregroundNodeLease = undefined;
    const client = this.getCurrentClient();
    let workerFailure: Error | null = null;
    let flushFailed = false;
    let notifyAbandoned!: () => void;
    const abandoned = new Promise<void>((resolve) => {
      notifyAbandoned = resolve;
    });
    const abandon = (error: Error) => {
      if (workerFailure) return;
      workerFailure = error;
      // Shutdown owns this lifetime now. Disable minting before even a best-effort
      // retirement can transfer ownership; a mere reconnect never reaches here.
      try {
        this.detachClient()?.discard();
      } catch {
        // The causal worker failure remains first even if local disposal fails.
      } finally {
        lease?.abandonAfterWorkerFailure(error);
        notifyAbandoned();
      }
    };
    this.shutdownFailure = { connection, abandon };
    if (admissionError instanceof BrowserWorkerUnresponsiveError) abandon(admissionError);

    try {
      await runCleanupSteps([
        () => {
          if (workerFailure) throw workerFailure;
        },
        () => unregisterInspectorControl?.(),
        async () => {
          if (workerFailure) throw workerFailure;
          // Configuration rejection is not a dead realm: skip the absent follower
          // flush, but retain the lease's ordinary clean-return semantics.
          if (admissionError) return;
          try {
            await connection?.flushLocal();
          } catch (error) {
            flushFailed = true;
            if (workerFailure) throw workerFailure;
            if (error instanceof BrowserWorkerUnresponsiveError) abandon(error);
            throw error;
          }
        },
        async () => {
          if (workerFailure) throw workerFailure;
          if (!lease) return;
          let finishFailed = false;
          let finishError: unknown;
          const finish = async () => {
            try {
              const runtime = client?.getRuntime();
              if (!runtime) {
                if (flushFailed) await lease.retire();
                else await lease.returnWithHighWater(lease.confirmedTxTime);
              } else if (!(runtime instanceof NativeRuntimeAdapter)) {
                await lease.retire();
              } else {
                // Even after an ordinary flush failure, close mutation admission
                // and drain started writes before releasing this identity. A
                // failed flush cannot establish a clean durable handoff.
                const highWater = await runtime.quiesceForegroundTxTimeHighWater();
                if (flushFailed) await lease.retire();
                else await lease.returnWithHighWater(highWater);
              }
            } catch (error) {
              finishFailed = true;
              finishError = workerFailure ?? error;
              // Abandonment rejects this fallback too; never replace the first
              // failure or retry an unknown clean return.
              await lease.retire().catch(() => undefined);
              throw finishError;
            }
          };
          // A causal failure may arrive while high-water capture or lease finish
          // is already in flight. Its background work cannot hold shutdown open.
          await Promise.race([
            finish(),
            abandoned.then(() => {
              throw finishFailed ? finishError : workerFailure;
            }),
          ]);
        },
        () => {
          // The tab view is non-durable; only its worker owns persistence shutdown.
          this.detachClient()?.discard();
        },
        () => super.shutdown(),
        () => connection?.shutdown(),
        () => {
          if (workerFailure) throw workerFailure;
        },
      ]);
    } finally {
      this.shutdownFailure = null;
    }
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
    this.publishExplicitOfflineState();
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
