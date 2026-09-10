import {
  createBrowserWorkerAssetScope,
  resolveBrowserWorkerRuntimeSources,
  resolveBrowserWorkerUrl,
} from "../browser-worker-config.js";
import {
  BrowserWorkerUnresponsiveError,
  deserializeBrowserRelayError,
  type BrowserSharedWorkerConnectRequest,
  type BrowserSharedWorkerConnectResponse,
  type BrowserSharedWorkerBootstrapCancelRequest,
  type BrowserForegroundNodeLeaseAcquireResponse,
  type BrowserForegroundNodeLeasePortEvent,
  type BrowserForegroundNodeLeasePortRequest,
  type BrowserFollowerPortEvent,
  type BrowserFollowerPortRequest,
  type BrowserInspectorControlEvent,
  type BrowserWorkerInitOptions,
} from "./browser-worker-protocol.js";
import type {
  BrowserForegroundNodeLease,
  BrowserWorkerConnection,
  BrowserWorkerConnectionContext,
} from "../runtime-source.js";
import { MessagePortBrowserFollowerConnection } from "./browser-follower-connection.js";
import type { NativeRuntimeAdapter } from "./native-runtime-adapter.js";
import { waitForInspectorOpening } from "./inspector-control-lifecycle.js";

export type BrowserForegroundNodeLeaseOptions = Pick<
  BrowserWorkerInitOptions,
  "runtimeSources" | "dbName" | "storageOwner"
>;

// Acquiring a lease is deliberately the first operation on a persistent
// browser root. On a cold browser profile that means starting the worker,
// acquiring the Web Lock, opening IndexedDB, checking the durable owner, and
// committing the lease receipt. This is I/O admission, not a heartbeat. Keep
// the complete probe-and-admission operation bounded so a genuinely wedged
// worker still fails visibly, but do not guess that a busy realm is dead from
// a shorter probe timeout. A realm that accepted termination explicitly says
// it is closing so the client can safely advance to another generation.
const FOREGROUND_NODE_LEASE_ADMISSION_TIMEOUT_MS = 10_000;
const MAX_SHARED_WORKER_GENERATION_ATTEMPTS = 8;
const MAX_FOREGROUND_NODE_LEASE_BUSY_ATTEMPTS = 8;
const FOREGROUND_NODE_LEASE_RETRY_INITIAL_DELAY_MS = 25;
const FOREGROUND_NODE_LEASE_RETRY_MAX_DELAY_MS = 250;
const RUNTIME_STARTUP_TIMEOUT_MS = 5 * 60_000;
const RUNTIME_STARTUP_CLOCK_INTERVAL_MS = 1_000;

type ForegroundNodeLeaseAttemptOutcome =
  | { type: "ready"; lease: SharedBrowserForegroundNodeLease }
  | { type: "worker-closing" }
  | { type: "busy"; message: string };

/**
 * A timed-out admission must keep its port alive until the worker retires a
 * possible late lease. Coalesce that background cleanup per physical worker
 * root so callers retrying a wedged open do not retain unbounded ports.
 */
const pendingForegroundLeaseCleanups = new Map<string, symbol>();

/**
 * The one physical SharedWorker realm that may own a browser persistence root.
 *
 * Authentication is intentionally not a part of this name. An explicitly
 * named IndexedDB root is durably bound to its owner at open time; putting the
 * owner in the worker name would instead make two workers race to own the same
 * root. Foreground lease acquisition happens before the complete runtime
 * configuration exists, so it must use precisely this same name.
 */
export function createBrowserSharedWorkerBaseName(
  runtimeSources: BrowserWorkerInitOptions["runtimeSources"],
  dbName: string,
): string {
  return ["jazz-runtime", dbName, createBrowserWorkerAssetScope(runtimeSources)].join(":");
}

function foregroundLeaseCleanupKey(workerName: string, storageOwner: string): string {
  return `${workerName}\u0000${storageOwner}`;
}

/**
 * Lease-only worker connection established before a foreground schema/runtime
 * exists. Its port stays open as the durable owner's liveness witness until
 * explicit clean return or retirement.
 */
export class SharedBrowserForegroundNodeLease implements BrowserForegroundNodeLease {
  private worker: Pick<SharedWorker, "port"> | null = null;
  private port: MessagePort | null = null;
  private closed = false;
  private terminalError: Error | null = null;
  private finishPromise: Promise<void> | null = null;
  private finishWaiter: { resolve: () => void; reject: (error: Error) => void } | null = null;
  private listeningForFinish = false;
  private terminalRequestSent = false;

  private constructor(
    readonly node: Uint8Array,
    readonly confirmedTxTime: bigint,
    private readonly leaseId: string,
  ) {}

  static async acquire(
    options: BrowserForegroundNodeLeaseOptions,
  ): Promise<SharedBrowserForegroundNodeLease> {
    const runtimeSources = resolveBrowserWorkerRuntimeSources(options.runtimeSources);
    const workerName = createBrowserSharedWorkerBaseName(runtimeSources, options.dbName);
    const cleanupKey = foregroundLeaseCleanupKey(workerName, options.storageOwner);
    if (pendingForegroundLeaseCleanups.has(cleanupKey)) {
      throw new Error(
        "Shared browser foreground lease cancellation cleanup is still pending for this database; wait for the previous worker admission to finish",
      );
    }
    const createWorker =
      runtimeSources?.brokerWorkerUrl || runtimeSources?.baseUrl || runtimeSources?.wasmVersion
        ? (name: string) =>
            new SharedWorker(resolveBrowserWorkerUrl(runtimeSources), { type: "module", name })
        : (name: string) =>
            new SharedWorker(new URL("../../worker/jazz-broker-worker.js", import.meta.url), {
              type: "module",
              name,
            });
    // A named SharedWorker constructor can still attach to a realm after that
    // realm has acknowledged termination but before the browser has finished
    // destroying it. Probe before sending an allocation request so a realm
    // that explicitly reports it is closing can be skipped without creating
    // an orphan-risking durable operation in it.
    const admissionDeadline = Date.now() + FOREGROUND_NODE_LEASE_ADMISSION_TIMEOUT_MS;
    let generation = readWorkerGeneration(workerName);
    let busyAttempt = 0;
    let lastBusyMessage: string | null = null;
    for (
      let generationAttempt = 0;
      generationAttempt < MAX_SHARED_WORKER_GENERATION_ATTEMPTS;
      generationAttempt += 1
    ) {
      while (true) {
        const remainingAdmissionMs = admissionDeadline - Date.now();
        if (remainingAdmissionMs <= 0) {
          throw new Error(
            lastBusyMessage ?? "Shared browser runtime did not issue a foreground node lease",
          );
        }
        const worker = createWorker(`${workerName}:generation-${generation}`);
        const outcome = await this.acquireFromWorkerGeneration(
          worker,
          options,
          cleanupKey,
          crypto.randomUUID(),
          remainingAdmissionMs,
        );
        if (outcome.type === "ready") return outcome.lease;
        if (outcome.type === "worker-closing") {
          // Only an explicitly terminating realm sends this receipt. Idle
          // close is still cancelable by its bootstrap reservation and never
          // forces a generation jump.
          generation = advanceWorkerGeneration(workerName, generation);
          break;
        }
        lastBusyMessage = outcome.message;
        busyAttempt += 1;
        if (busyAttempt >= MAX_FOREGROUND_NODE_LEASE_BUSY_ATTEMPTS) {
          throw new Error(lastBusyMessage);
        }
        const retryDelayMs = Math.min(
          FOREGROUND_NODE_LEASE_RETRY_INITIAL_DELAY_MS * 2 ** (busyAttempt - 1),
          FOREGROUND_NODE_LEASE_RETRY_MAX_DELAY_MS,
          Math.max(0, admissionDeadline - Date.now()),
        );
        if (retryDelayMs <= 0) throw new Error(lastBusyMessage);
        await new Promise<void>((resolve) => setTimeout(resolve, retryDelayMs));
      }
    }
    throw new Error(
      lastBusyMessage ?? "Shared browser foreground lease worker did not answer after closing",
    );
  }

  /** @internal A context-scoped lease port minted by Inspector control. */
  static async acquireFromPort(
    port: MessagePort,
    options: BrowserForegroundNodeLeaseOptions,
  ): Promise<SharedBrowserForegroundNodeLease> {
    const outcome = await this.acquireFromWorkerGeneration(
      { port },
      options,
      `inspector:${options.dbName}:${options.storageOwner}`,
      crypto.randomUUID(),
      FOREGROUND_NODE_LEASE_ADMISSION_TIMEOUT_MS,
    );
    if (outcome.type !== "ready") throw new Error("Inspector worker is no longer available");
    return outcome.lease;
  }

  private static acquireFromWorkerGeneration(
    worker: Pick<SharedWorker, "port">,
    options: BrowserForegroundNodeLeaseOptions,
    cleanupKey: string,
    attemptId: string,
    timeoutMs: number,
  ): Promise<ForegroundNodeLeaseAttemptOutcome> {
    const port = worker.port;
    return new Promise<ForegroundNodeLeaseAttemptOutcome>((resolve, reject) => {
      let cancellationRequested = false;
      let publicResultSettled = false;
      let cleanupToken: symbol | null = null;
      let admissionTimeout: ReturnType<typeof setTimeout> | null = null;
      const timeoutError = new Error(
        "Shared browser runtime did not issue a foreground node lease",
      );
      const rejectPublic = (error: Error) => {
        if (publicResultSettled) return;
        publicResultSettled = true;
        reject(error);
      };
      const beginAdmissionTimeout = () =>
        setTimeout(() => {
          // Do not close the port immediately. The worker might be between the
          // durable allocation and its ready reply; it must observe this cancel
          // and retire such a lease before this acquire rejects. Otherwise a
          // merely slow cold open would burn one node identity per timeout.
          cancellationRequested = true;
          cleanupToken = Symbol("foreground-lease-cleanup");
          pendingForegroundLeaseCleanups.set(cleanupKey, cleanupToken);
          port.postMessage({ type: "cancel-foreground-node-lease" });
          // Public startup remains bounded. The open port and listeners stay
          // alive in the background until the worker observes cancellation and
          // retires any late lease; closing them here would recreate the orphan
          // race this cancellation protocol exists to prevent.
          rejectPublic(timeoutError);
        }, timeoutMs);
      const cleanup = () => {
        if (admissionTimeout) clearTimeout(admissionTimeout);
        admissionTimeout = null;
        // A successful concurrent admission never owned this retained cleanup.
        // Only its timeout owner may clear the shared key; otherwise it could
        // let a third caller accumulate another orphan-risking port.
        if (
          cleanupToken !== null &&
          pendingForegroundLeaseCleanups.get(cleanupKey) === cleanupToken
        ) {
          pendingForegroundLeaseCleanups.delete(cleanupKey);
        }
        port.removeEventListener("message", onMessage);
        port.removeEventListener("messageerror", onMessageError);
      };
      const onMessage = (event: MessageEvent<BrowserForegroundNodeLeaseAcquireResponse>) => {
        const message = event.data;
        if (message?.type === "foreground-node-lease-worker-alive") {
          if (message.attemptId !== attemptId || admissionTimeout === null) return;
          port.postMessage({
            type: "acquire-foreground-node-lease",
            attemptId,
            dbName: options.dbName,
            storageOwner: options.storageOwner,
          });
          return;
        }
        if (message?.type === "foreground-node-lease-worker-closing") {
          if (message.attemptId !== attemptId) return;
          cleanup();
          port.close();
          publicResultSettled = true;
          resolve({ type: "worker-closing" });
          return;
        }
        if (message?.type === "foreground-node-lease-busy") {
          cleanup();
          port.close();
          publicResultSettled = true;
          resolve({ type: "busy", message: message.message });
          return;
        }
        if (message?.type === "foreground-node-lease-error") {
          cleanup();
          port.close();
          rejectPublic(deserializeBrowserRelayError(message.error));
          return;
        }
        if (message?.type === "foreground-node-lease-cancelled") {
          cleanup();
          port.close();
          rejectPublic(message.error ? deserializeBrowserRelayError(message.error) : timeoutError);
          return;
        }
        if (message?.type !== "foreground-node-lease-ready") return;
        // A cancel can race the ready message. Leave the port open until the
        // worker confirms it has retired this just-issued lease.
        if (cancellationRequested) return;
        if (!/^(0|[1-9][0-9]*)$/.test(message.confirmedTxTime)) {
          cleanup();
          rejectPublic(
            new Error("Shared browser runtime returned an invalid foreground lease high-water"),
          );
          return;
        }
        cleanup();
        const lease = new SharedBrowserForegroundNodeLease(
          message.node.slice(),
          BigInt(message.confirmedTxTime),
          message.leaseId,
        );
        lease.worker = worker;
        lease.port = port;
        publicResultSettled = true;
        resolve({ type: "ready", lease });
      };
      const onMessageError = () => {
        cleanup();
        port.close();
        rejectPublic(new Error("Shared browser foreground lease port message error"));
      };
      port.addEventListener("message", onMessage);
      port.addEventListener("messageerror", onMessageError);
      port.start();
      admissionTimeout = beginAdmissionTimeout();
      port.postMessage({
        type: "probe-foreground-node-lease-worker",
        attemptId,
      });
    });
  }

  async returnWithHighWater(highWater: bigint): Promise<void> {
    if (this.terminalError) throw this.terminalError;
    if (highWater < 0n) throw new Error("Invalid foreground transaction high-water");
    await this.finish({
      type: "return-foreground-node-lease",
      confirmedTxTime: highWater.toString(),
    });
  }

  async retire(): Promise<void> {
    if (this.terminalError) throw this.terminalError;
    if (this.closed) return;
    await this.finish({ type: "retire-foreground-node-lease" });
  }

  abandonAfterWorkerFailure(error: Error): void {
    if (this.terminalError) return;
    this.terminalError = error;
    this.finishWaiter?.reject(error);
    this.finishWaiter = null;
    if (this.closed || !this.port) return;
    this.listenForFinish();
    // A quiesced return may already have committed. Retain its one outcome;
    // another command cannot retrospectively retire an identity already reused.
    if (this.terminalRequestSent) return;
    this.terminalRequestSent = true;
    try {
      this.port.postMessage({ type: "retire-foreground-node-lease" });
    } catch {
      this.closeFinishPort();
    }
    // Do not close after posting: WebKit can discard a queued retirement.
    // The late result only releases this background witness, not durable success.
  }

  releaseAfterStorageReset(reason: Error): void {
    this.failFinish(reason);
  }

  private failFinish(error: Error): void {
    this.terminalError ??= error;
    this.finishWaiter?.reject(this.terminalError);
    this.closeFinishPort();
  }

  private finish(message: BrowserForegroundNodeLeasePortRequest): Promise<void> {
    if (this.terminalError) return Promise.reject(this.terminalError);
    if (this.closed)
      return Promise.reject(new Error("Shared browser foreground lease is already closed"));
    if (this.finishPromise) return this.finishPromise;
    const port = this.port;
    if (!port)
      return Promise.reject(new Error("Shared browser foreground lease port is unavailable"));
    this.listenForFinish();
    this.finishPromise = new Promise<void>((resolve, reject) => {
      this.finishWaiter = { resolve, reject };
    });
    this.terminalRequestSent = true;
    try {
      port.postMessage(message);
    } catch (error) {
      this.failFinish(error instanceof Error ? error : new Error(String(error)));
    }
    return this.finishPromise;
  }

  private listenForFinish(): void {
    if (this.listeningForFinish) return;
    this.listeningForFinish = true;
    this.port?.addEventListener("message", this.onFinishResult);
    this.port?.addEventListener("messageerror", this.onFinishError);
  }

  private readonly onFinishResult = (event: MessageEvent<BrowserForegroundNodeLeasePortEvent>) => {
    const result = event.data;
    if (result?.type !== "foreground-node-lease-result") return;
    if (result.error) this.failFinish(deserializeBrowserRelayError(result.error));
    else {
      this.finishWaiter?.resolve();
      this.closeFinishPort();
    }
  };

  private readonly onFinishError = () => {
    this.failFinish(new Error("Shared browser foreground lease port message error"));
  };

  private closeFinishPort(): void {
    this.finishWaiter = null;
    this.closed = true;
    this.port?.removeEventListener("message", this.onFinishResult);
    this.port?.removeEventListener("messageerror", this.onFinishError);
    this.worker?.port.close();
    this.port = null;
    this.worker = null;
  }
}

export class SharedBrowserWorkerConnection implements BrowserWorkerConnection {
  private worker: SharedWorker | null = null;
  private readonly readyPromise: Promise<void>;
  private initialConfigurationAdmissionRejected = false;
  private readyError: Error | null = null;
  private connection: MessagePortBrowserFollowerConnection | null = null;
  private closed = false;
  private readonly workerName: string;
  private connectedGeneration: number | null = null;
  private cancelBootstrap: (() => void) | null = null;

  constructor(
    runtime: NativeRuntimeAdapter,
    options: BrowserWorkerInitOptions,
    fingerprint: string,
    private readonly callbacks: Pick<
      BrowserWorkerConnectionContext,
      | "onAuthFailure"
      | "onAuthRestored"
      | "onExplicitOfflineChange"
      | "onFailure"
      | "onStorageReset"
      | "onStorageInvalidated"
    >,
  ) {
    // A physical database namespace has exactly one broker realm for a given
    // worker asset. The physical namespace already includes the complete
    // app/environment/auth scope, so different accounts intentionally reach
    // separate workers and caches while same-scope tabs share one realm.
    const runtimeSources = resolveBrowserWorkerRuntimeSources(options.runtimeSources);
    const workerName = createBrowserSharedWorkerBaseName(runtimeSources, options.dbName);
    this.workerName = workerName;
    const createWorker =
      runtimeSources?.brokerWorkerUrl || runtimeSources?.baseUrl || runtimeSources?.wasmVersion
        ? (name: string) =>
            new SharedWorker(resolveBrowserWorkerUrl(runtimeSources), {
              type: "module",
              name,
            })
        : (name: string) =>
            new SharedWorker(new URL("../../worker/jazz-broker-worker.js", import.meta.url), {
              type: "module",
              name,
            });
    // Retain admission failures as state rather than leaving a process-wide
    // bootstrap promise rejected. The manager converts this state back into
    // the caller's `all()`/`wait()` rejection at its operation boundary; a
    // tab which has not yet installed one must never produce an ambient
    // unhandled-rejection event merely because its durable root is owned by
    // another account.
    this.readyPromise = this.connect(
      runtime,
      { ...options, runtimeSources },
      fingerprint,
      workerName,
      createWorker,
    ).then(
      () => undefined,
      (error: unknown) => {
        this.readyError = error instanceof Error ? error : new Error(String(error));
        callbacks.onFailure(this.readyError);
      },
    );
  }

  private async connect(
    runtime: NativeRuntimeAdapter,
    options: BrowserWorkerInitOptions,
    fingerprint: string,
    workerName: string,
    createWorker: (name: string) => SharedWorker,
  ): Promise<void> {
    let generation = readWorkerGeneration(workerName);
    for (let attempt = 0; attempt < MAX_SHARED_WORKER_GENERATION_ATTEMPTS; attempt += 1) {
      if (this.closed) return;
      const generationName = `${workerName}:generation-${generation}`;
      const outcome = await this.connectOnce(
        runtime,
        options,
        fingerprint,
        generationName,
        createWorker,
      );
      if (outcome.error) throw outcome.error;
      if (this.closed) return;
      if (outcome.connected) {
        this.connectedGeneration = generation;
        return;
      }
      generation = advanceWorkerGeneration(workerName, generation);
    }
    throw new Error("Shared browser runtime did not answer after its previous realm closed");
  }

  private connectOnce(
    runtime: NativeRuntimeAdapter,
    options: BrowserWorkerInitOptions,
    fingerprint: string,
    workerName: string,
    createWorker: (name: string) => SharedWorker,
  ): Promise<{ connected: boolean; error?: Error }> {
    const worker = createWorker(workerName);
    this.worker = worker;
    const port = worker.port;
    port.start();
    return new Promise<{ connected: boolean; error?: Error }>((resolve) => {
      let settled = false;
      let cancelled = false;
      let closingLatePeer = false;
      let alive = false;
      let lastTick = Date.now();
      // A delayed alive reply is not evidence that this realm released its
      // physical database lock. Only worker-closing authorizes a generation
      // change; apply the bounded startup grace even before the first reply.
      let deadline = lastTick + RUNTIME_STARTUP_TIMEOUT_MS;
      let suspended = typeof document !== "undefined" && document.hidden;
      let bootstrapTimer: ReturnType<typeof setTimeout> | undefined;
      const settle = (outcome: { connected: boolean; error?: Error }) => {
        if (settled) return;
        settled = true;
        stopClock();
        if (this.cancelBootstrap === cancelForShutdown) this.cancelBootstrap = null;
        resolve(outcome);
      };
      const cancel = (outcome: { connected: boolean; error?: Error }) => {
        if (settled) return;
        cancelled = true;
        settle(outcome);
        // Keep the listener until the worker fences admission or publishes a
        // late peer. Public readiness/shutdown must not await this cleanup.
        if (this.worker === worker) this.worker = null;
        try {
          port.postMessage({
            type: "cancel-runtime-bootstrap",
          } satisfies BrowserSharedWorkerBootstrapCancelRequest);
        } catch {
          cleanup();
          port.close();
        }
      };
      const cancelForShutdown = () =>
        cancel({
          connected: false,
          error: new Error("Shared browser runtime connection is closed"),
        });
      this.cancelBootstrap = cancelForShutdown;
      const refreshGrace = () => {
        lastTick = Date.now();
        deadline = lastTick + RUNTIME_STARTUP_TIMEOUT_MS;
      };
      const onVisibilityChange = () => {
        const hidden = typeof document !== "undefined" && document.hidden;
        if (suspended && !hidden) refreshGrace();
        suspended = hidden;
      };
      const onPageShow = () => refreshGrace();
      const tick = () => {
        const now = Date.now();
        // A delayed task cannot distinguish a dead worker from a suspended
        // page. Give a resumed page one fresh complete admission grace.
        if (suspended || now - lastTick > RUNTIME_STARTUP_CLOCK_INTERVAL_MS * 2) refreshGrace();
        lastTick = now;
        if (now >= deadline) {
          cancel({
            connected: false,
            error: new BrowserWorkerUnresponsiveError(
              "Shared browser runtime did not finish initialization within five minutes",
            ),
          });
          return;
        }
        bootstrapTimer = setTimeout(
          tick,
          Math.min(RUNTIME_STARTUP_CLOCK_INTERVAL_MS, deadline - now),
        );
      };
      const stopClock = () => {
        clearTimeout(bootstrapTimer);
        bootstrapTimer = undefined;
        if (typeof document !== "undefined")
          document.removeEventListener("visibilitychange", onVisibilityChange);
        if (typeof window !== "undefined") window.removeEventListener("pageshow", onPageShow);
      };
      const onMessage = (
        event: MessageEvent<BrowserSharedWorkerConnectResponse | BrowserFollowerPortEvent>,
      ) => {
        if (cancelled) {
          if (event.data?.type === "runtime-ready") {
            if (closingLatePeer) return;
            closingLatePeer = true;
            port.postMessage({
              type: "close",
              id: 0,
              releaseContext: true,
            } satisfies BrowserFollowerPortRequest);
            return;
          } else if (
            event.data?.type !== "runtime-bootstrap-cancelled" &&
            event.data?.type !== "runtime-error" &&
            event.data?.type !== "worker-closing" &&
            !(event.data?.type === "result" && event.data.id === 0)
          )
            return;
          cleanup();
          port.close();
          return;
        }
        if (event.data?.type === "worker-alive") {
          if (!alive) {
            alive = true;
            refreshGrace();
          }
          return;
        }
        if (event.data?.type === "runtime-error") {
          cleanup();
          port.close();
          if (this.worker === worker) this.worker = null;
          const error = deserializeBrowserRelayError(event.data.error);
          this.initialConfigurationAdmissionRejected =
            error.message === "incompatible persistent browser configuration";
          settle({ connected: false, error });
          return;
        }
        if (event.data?.type === "worker-closing") {
          cleanup();
          port.close();
          if (this.worker === worker) this.worker = null;
          settle({ connected: false });
          return;
        }
        if (event.data?.type !== "runtime-ready") return;
        cleanup();
        // The installed follower owns liveness from this synchronous handoff.
        if (this.cancelBootstrap === cancelForShutdown) this.cancelBootstrap = null;
        const connection = new MessagePortBrowserFollowerConnection(
          runtime,
          port,
          options.sessionClaims,
          options.dbName,
          {
            onAuthFailure: this.callbacks.onAuthFailure,
            onAuthRestored: this.callbacks.onAuthRestored,
            onExplicitOfflineChange: this.callbacks.onExplicitOfflineChange,
            onFailure: this.callbacks.onFailure,
            onStorageReset: this.callbacks.onStorageReset,
            onStorageInvalidated: this.callbacks.onStorageInvalidated,
          },
          options.logLevel === "trace",
        );
        this.connection = connection;
        void connection.ready().then(
          () => settle({ connected: true }),
          (error: unknown) => {
            port.close();
            if (this.connection === connection) this.connection = null;
            if (this.worker === worker) this.worker = null;
            settle({
              connected: false,
              error: error instanceof Error ? error : new Error(String(error)),
            });
          },
        );
      };
      const onMessageError = () => {
        cleanup();
        port.close();
        if (this.worker === worker) this.worker = null;
        settle({
          connected: false,
          error: new Error("Shared browser runtime port message error"),
        });
      };
      const cleanup = () => {
        stopClock();
        port.removeEventListener("message", onMessage);
        port.removeEventListener("messageerror", onMessageError);
      };
      port.addEventListener("message", onMessage);
      port.addEventListener("messageerror", onMessageError);
      if (typeof document !== "undefined")
        document.addEventListener("visibilitychange", onVisibilityChange);
      if (typeof window !== "undefined") window.addEventListener("pageshow", onPageShow);
      bootstrapTimer = setTimeout(tick, 1_000);
      try {
        port.postMessage({
          type: "connect-runtime",
          tabId: crypto.randomUUID(),
          fingerprint,
          options,
        } satisfies BrowserSharedWorkerConnectRequest);
      } catch (error) {
        cleanup();
        port.close();
        if (this.worker === worker) this.worker = null;
        settle({
          connected: false,
          error: error instanceof Error ? error : new Error(String(error)),
        });
      }
    });
  }

  ready(): Promise<void> {
    // The constructor-owned promise always settles successfully; this is the
    // operation boundary that turns its retained admission failure back into
    // a normal caller-visible rejection.
    return this.readyPromise.then(() => {
      if (this.readyError) throw this.readyError;
    });
  }

  async waitForServerConnection(): Promise<void> {
    await this.ready();
    await this.connection?.waitForServerConnection();
  }

  async updateAuth(authJson: string, sessionClaims: Record<string, unknown>): Promise<void> {
    await this.ready();
    this.connection?.updateAuth(authJson, sessionClaims);
  }

  async disconnect(): Promise<void> {
    await this.ready();
    await this.connection?.disconnect();
  }

  async reconnect(authJson: string, sessionClaims: Record<string, unknown>): Promise<void> {
    await this.ready();
    await this.connection?.reconnect(authJson, sessionClaims);
  }

  canRetryInitialConfigurationAdmission(): boolean {
    return this.initialConfigurationAdmissionRejected;
  }

  async shutdown(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.cancelBootstrap?.();
    await this.connection?.shutdown(true);
    this.connection = null;
    this.worker?.port.close();
    this.worker = null;
  }

  async waitForPendingWrites(): Promise<void> {
    await this.ready();
    await this.connection?.waitForPendingWrites();
  }

  async flushLocal(): Promise<void> {
    await this.ready();
    await this.connection?.flushLocal();
  }

  async deleteStorage(): Promise<void> {
    await this.ready();
    await this.connection?.deleteStorage();
  }

  async openInspectorControlPort(signal?: AbortSignal): Promise<MessagePort> {
    await waitForInspectorOpening(this.ready(), signal);
    if (!this.connection) throw new Error("Shared browser runtime is not connected");
    const port = await this.connection.openInspectorControlPort(signal);
    const generation = this.connectedGeneration;
    if (generation !== null) {
      installWorkerTerminationGenerationHandoff(port, this.workerName, generation);
    }
    return port;
  }
}

/** @internal Keeps an acknowledged inspector restart out of its closing realm. */
export function installWorkerTerminationGenerationHandoff(
  port: MessagePort,
  workerName: string,
  generation: number,
): void {
  port.addEventListener("message", (event: MessageEvent<BrowserInspectorControlEvent>) => {
    if (event.data?.type !== "result" || event.data.workerTerminated !== true) return;
    advanceWorkerGeneration(workerName, generation);
  });
}

function readWorkerGeneration(workerName: string): number {
  try {
    const generation = Number.parseInt(
      localStorage.getItem(workerGenerationKey(workerName)) ?? "0",
      10,
    );
    return Number.isSafeInteger(generation) && generation >= 0 ? generation : 0;
  } catch {
    return 0;
  }
}

function advanceWorkerGeneration(workerName: string, failedGeneration: number): number {
  try {
    const key = workerGenerationKey(workerName);
    const current = readWorkerGeneration(workerName);
    if (current === failedGeneration) {
      localStorage.setItem(key, String(failedGeneration + 1));
    }
    return readWorkerGeneration(workerName);
  } catch {
    return failedGeneration + 1;
  }
}

function workerGenerationKey(workerName: string): string {
  return `jazz:shared-worker-generation:${workerName}`;
}
