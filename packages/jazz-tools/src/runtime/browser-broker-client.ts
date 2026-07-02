import {
  detectBrowserBrokerMissingCapabilities,
  formatUnsupportedBrowserBrokerError,
  stringifyError,
  type BrowserBrokerCapabilityGlobal,
  type BrowserBrokerControlMessage,
  type BrowserBrokerRole,
  type BrowserBrokerVisibility,
} from "./browser-broker-protocol.js";
import {
  createBrowserBrokerUnsupportedError,
  type BrowserBrokerUnsupportedCode,
} from "./browser-broker-errors.js";
import { loadWasmModule } from "./client.js";
import type { RuntimeSourcesConfig } from "./context.js";
import { resolveRuntimeConfigBrokerWorkerUrl } from "./runtime-config.js";

export interface BrowserBrokerClientSnapshot {
  brokerInstanceId: string | null;
  role: BrowserBrokerRole;
  tabId: string;
  leaderTabId: string | null;
  leadershipId: number;
}

export interface BrowserBrokerLeaderReadyInput {
  leadershipId: number;
  tabLockName: string;
  workerLockName: string;
  bridgelessStorageReset?: boolean;
}

export interface BrowserBrokerClientOptions {
  appId: string;
  dbName: string;
  tabId: string;
  fingerprint: string;
  visibility: BrowserBrokerVisibility;
  forceTakeoverTimeoutMs?: number;
  brokerPingIntervalMs?: number;
  brokerPongTimeoutMs?: number;
  runtimeSources?: RuntimeSourcesConfig;
  globalLike?: BrowserBrokerCapabilityGlobal;
  respondToBrokerPings?: boolean | (() => boolean);
  onBrokerPing?: () => void;
  onBecomeLeader?: (
    client: BrowserBrokerClient,
    leadershipId: number,
    resetRequestId?: string,
  ) => void | Promise<void>;
  onDemote?: (leadershipId: number) => void | Promise<void>;
  onAttachFollowerPort?: (followerTabId: string, leadershipId: number, port: MessagePort) => void;
  onDetachFollowerPort?: (followerTabId: string, leadershipId: number) => void;
  onUseFollowerPort?: (leadershipId: number, port: MessagePort) => void;
  onFollowerReady?: (leadershipId: number) => void;
  onCloseFollowerPort?: (leadershipId: number) => void;
  onStorageResetBegin?: (requestId: string, leadershipId: number) => void | Promise<void>;
  onSchemaBlocked?: (reason: string) => void;
  onReconnected?: (client: BrowserBrokerClient) => void;
  onClosed?: (error: Error) => void;
  storageResetTimeoutMs?: number;
}

type SharedWorkerConstructor = new (
  scriptURL: string | URL,
  options?: string | BrowserBrokerSharedWorkerOptions,
) => SharedWorker;

interface BrowserBrokerSharedWorkerOptions {
  type?: WorkerType;
  name?: string;
  credentials?: RequestCredentials;
}

/**
 * The state machine lives in Rust (`jazz-browser-broker` crate, exposed as
 * `WasmTabBrokerCore` from the jazz-wasm binary). This class is the I/O shell:
 * it owns the SharedWorker, the timers, the promises, and the MessagePorts,
 * and it executes the commands the core emits for every event.
 */
interface TabBrokerCore {
  handle(event: unknown): TabClientCommand[];
  snapshot(): TabCoreSnapshot;
}

interface TabCoreSnapshot {
  brokerInstanceId: string | null;
  role: BrowserBrokerRole;
  leaderTabId: string | null;
  leadershipId: number;
  closed: boolean;
  reconnecting: boolean;
}

type TabClientCommand = {
  kind: string;
  [key: string]: unknown;
};

type TabTimerKey = {
  kind: string;
  [key: string]: unknown;
};

type QueuedEvent = {
  event: Record<string, unknown>;
  heldPort: MessagePort | null;
};

type PendingWaiter = {
  resolve: () => void;
  reject: (error: Error) => void;
};

type WaiterRejection = { kind: "closedError" } | { kind: "message"; message: string };

export class BrowserBrokerClient {
  private readonly options: BrowserBrokerClientOptions;
  private worker: SharedWorker | null = null;
  private port: MessagePort | null = null;
  private core: TabBrokerCore | null = null;
  private corePromise: Promise<void> | null = null;
  private closedError: Error | null = null;
  private pendingReconnectCause: unknown = undefined;
  private reconnectDone: Promise<void> | null = null;
  private resolveReconnectDone: (() => void) | null = null;
  private visibility: BrowserBrokerVisibility;
  private nextWaiterId = 1;
  private readonly roleWaiters = new Map<number, PendingWaiter>();
  private readonly resetStartWaiters = new Map<number, PendingWaiter>();
  private readonly resetWaiters = new Map<number, PendingWaiter>();
  private readonly timers = new Map<string, ReturnType<typeof setTimeout>>();
  private readonly queuedEvents: QueuedEvent[] = [];
  private heldPort: MessagePort | null = null;

  private constructor(options: BrowserBrokerClientOptions) {
    this.options = options;
    this.visibility = options.visibility;
  }

  static async connect(options: BrowserBrokerClientOptions): Promise<BrowserBrokerClient> {
    const globalLike = options.globalLike ?? (globalThis as BrowserBrokerCapabilityGlobal);
    const missing = detectBrowserBrokerMissingCapabilities(globalLike);
    if (missing.length > 0) {
      throw new Error(formatUnsupportedBrowserBrokerError(missing));
    }

    const client = new BrowserBrokerClient(options);
    await client.connectToBroker();
    return client;
  }

  snapshot(): BrowserBrokerClientSnapshot {
    const snapshot = this.coreSnapshot();
    return {
      brokerInstanceId: snapshot.brokerInstanceId,
      role: snapshot.role,
      tabId: this.options.tabId,
      leaderTabId: snapshot.leaderTabId,
      leadershipId: snapshot.leadershipId,
    };
  }

  async waitForRole(role: BrowserBrokerRole, timeoutMs = 5_000): Promise<void> {
    const snapshot = this.coreSnapshot();
    if (snapshot.closed) {
      throw this.closedError ?? new Error("Browser broker client closed");
    }
    if (snapshot.role === role && snapshot.leaderTabId !== null) {
      return;
    }

    const waiterId = this.nextWaiterId++;
    await new Promise<void>((resolve, reject) => {
      this.roleWaiters.set(waiterId, { resolve, reject });
      this.dispatch({ kind: "roleWaiterAdded", waiterId, role, timeoutMs });
    });
  }

  reportLeaderReady(input: BrowserBrokerLeaderReadyInput): void {
    this.dispatch({
      kind: "sendRequested",
      message: {
        type: "leader-ready",
        leadershipId: input.leadershipId,
        tabLockName: input.tabLockName,
        workerLockName: input.workerLockName,
        ...(input.bridgelessStorageReset ? { bridgelessStorageReset: true } : {}),
      },
    });
  }

  reportLeaderFailed(leadershipId: number, reason: string): void {
    this.dispatch({
      kind: "sendRequested",
      message: { type: "leader-failed", leadershipId, reason },
    });
  }

  reportVisibility(visibility: BrowserBrokerVisibility): void {
    this.visibility = visibility;
    this.dispatch({ kind: "visibilityReported", visibility });
  }

  reportFollowerPortAttached(followerTabId: string, leadershipId: number): void {
    this.dispatch({
      kind: "sendRequested",
      message: { type: "follower-port-attached", followerTabId, leadershipId },
    });
  }

  reportFollowerPortClosed(followerTabId: string, leadershipId: number): void {
    this.dispatch({
      kind: "sendRequested",
      message: { type: "follower-port-closed", followerTabId, leadershipId },
    });
  }

  reportSchemaReady(schemaFingerprint: string): void {
    this.dispatch({
      kind: "sendRequested",
      message: { type: "schema-ready", schemaFingerprint },
    });
  }

  async requestStorageReset(requestId: string): Promise<void> {
    if (this.coreSnapshot().closed) {
      throw this.closedError ?? new Error("Browser broker client closed");
    }
    // A reconnect drops in-flight sends; wait for it to settle so the
    // reset request reaches the new broker instance instead of vanishing.
    while (this.coreSnapshot().reconnecting && this.reconnectDone) {
      await this.reconnectDone;
      if (this.coreSnapshot().closed) {
        throw this.closedError ?? new Error("Browser broker client closed");
      }
    }

    const startWaiterId = this.nextWaiterId++;
    const completionWaiterId = this.nextWaiterId++;
    const started = new Promise<void>((resolve, reject) => {
      this.resetStartWaiters.set(startWaiterId, { resolve, reject });
    });
    const completion = new Promise<void>((resolve, reject) => {
      this.resetWaiters.set(completionWaiterId, { resolve, reject });
    });
    this.dispatch({
      kind: "storageResetRequested",
      requestId,
      startWaiterId,
      completionWaiterId,
    });
    try {
      await started;
      await completion;
    } catch (error) {
      this.resetWaiters.delete(completionWaiterId);
      throw error;
    }
  }

  async shutdown(): Promise<void> {
    if (this.coreSnapshot().closed) return;
    this.closedError ??= new Error("Browser broker client closed");
    this.dispatch({ kind: "shutdownRequested" });
  }

  private async connectToBroker(): Promise<void> {
    const worker = this.createSharedWorker();
    const port = worker.port;
    this.worker = worker;
    this.port = port;

    port.addEventListener("message", this.onMessage);
    port.addEventListener("messageerror", this.onPortMessageError);
    port.start();
    this.dispatch({ kind: "portAttached" });

    const hello = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => {
        cleanup();
        reject(new Error("Timed out waiting for browser broker hello"));
      }, 5_000);

      const onWorkerError = (event: Event) => {
        cleanup();
        const detail =
          (event as ErrorEvent).message ||
          "worker error event (possible script URL or version mismatch)";
        reject(new Error(`Browser broker SharedWorker failed to start: ${detail}`));
      };

      const cleanup = () => {
        clearTimeout(timeout);
        port.removeEventListener("message", onHello);
        workerEvents?.removeEventListener("error", onWorkerError);
      };

      const onHello = (event: MessageEvent) => {
        const message = event.data as BrowserBrokerControlMessage;
        if (message?.type === "broker-hello") {
          cleanup();
          resolve();
          return;
        }
        if (message?.type === "unsupported") {
          cleanup();
          reject(createBrowserBrokerUnsupportedError(message.reason, message.code));
        }
      };

      // Fakes in unit tests (and exotic SharedWorker shims) may not be
      // EventTargets, so the listener is best-effort.
      const workerEvents =
        typeof (worker as Partial<EventTarget>).addEventListener === "function"
          ? (worker as unknown as EventTarget)
          : null;
      workerEvents?.addEventListener("error", onWorkerError);
      port.addEventListener("message", onHello);
    });
    // The rejection can land while we are still awaiting core init below;
    // pre-attach a handler so it is never reported as unhandled. The awaited
    // path further down still observes it.
    void hello.catch(() => undefined);

    port.postMessage({
      type: "hello",
      tabId: this.options.tabId,
      appId: this.options.appId,
      dbName: this.options.dbName,
      fingerprint: this.options.fingerprint,
      visibility: this.visibility,
      forceTakeoverTimeoutMs: this.options.forceTakeoverTimeoutMs,
      brokerPingIntervalMs: this.options.brokerPingIntervalMs,
      brokerPongTimeoutMs: this.options.brokerPongTimeoutMs,
    });

    try {
      await this.ensureCore();
      await hello;
      await this.waitForInitialLeadershipMessage(port);
      this.dispatch({ kind: "connectCompleted" });
    } catch (error) {
      if (this.port === port) {
        this.detachBrokerPort(port);
        this.port = null;
      }
      if (this.worker === worker) {
        this.worker = null;
      }
      throw error;
    }
  }

  private ensureCore(): Promise<void> {
    this.corePromise ??= (async () => {
      const wasmModule = (await loadWasmModule(this.options.runtimeSources)) as unknown as {
        WasmTabBrokerCore: new (options: unknown) => TabBrokerCore;
      };
      this.core = new wasmModule.WasmTabBrokerCore({
        tabId: this.options.tabId,
        brokerPingIntervalMs: this.options.brokerPingIntervalMs,
        brokerPongTimeoutMs: this.options.brokerPongTimeoutMs,
        storageResetTimeoutMs: this.options.storageResetTimeoutMs,
      });
      this.flushQueuedEvents();
    })();
    return this.corePromise;
  }

  private coreSnapshot(): TabCoreSnapshot {
    return (
      this.core?.snapshot() ?? {
        brokerInstanceId: null,
        role: "follower",
        leaderTabId: null,
        leadershipId: 0,
        closed: false,
        reconnecting: false,
      }
    );
  }

  private async waitForInitialLeadershipMessage(port: MessagePort): Promise<void> {
    if (this.coreSnapshot().leadershipId > 0 || this.coreSnapshot().closed || this.port !== port) {
      return;
    }

    await new Promise<void>((resolve) => {
      let timeout: ReturnType<typeof setTimeout>;
      const cleanup = () => {
        clearTimeout(timeout);
        port.removeEventListener("message", onMessage);
        resolve();
      };
      const onMessage = () => {
        const snapshot = this.coreSnapshot();
        if (snapshot.leadershipId > 0 || snapshot.closed || this.port !== port) {
          cleanup();
        }
      };
      timeout = setTimeout(cleanup, 100);
      port.addEventListener("message", onMessage);
    });
  }

  private createSharedWorker(): SharedWorker {
    const globalLike = this.options.globalLike ?? (globalThis as BrowserBrokerCapabilityGlobal);
    const SharedWorkerCtor = globalLike.SharedWorker as SharedWorkerConstructor;
    const workerUrl =
      this.options.runtimeSources?.brokerWorkerUrl || this.options.runtimeSources?.baseUrl
        ? resolveRuntimeConfigBrokerWorkerUrl(
            import.meta.url,
            typeof location !== "undefined" ? location.href : undefined,
            this.options.runtimeSources,
          )
        : new URL("../worker/jazz-broker-worker.js", import.meta.url);
    return new SharedWorkerCtor(workerUrl, {
      type: "module",
      name: `jazz-broker:${this.options.appId}:${this.options.dbName}`,
    });
  }

  private readonly onMessage = (event: MessageEvent): void => {
    const message = event.data as BrowserBrokerControlMessage;
    if (!message || typeof message !== "object") return;
    // Strip any transferred MessagePort before the message crosses into wasm;
    // the port is paired back on the matching Invoke* command. Events are
    // processed strictly one at a time, so a single slot suffices.
    const { port: transferredPort, ...portlessMessage } = message as BrowserBrokerControlMessage & {
      port?: MessagePort;
    };
    this.dispatch(
      {
        kind: "controlMessage",
        message: portlessMessage,
        stampedInstanceId: (message as { brokerInstanceId?: string }).brokerInstanceId ?? null,
      },
      transferredPort ?? null,
    );
  };

  private readonly onPortMessageError = (): void => {
    this.dispatch({ kind: "portMessageError" });
  };

  private dispatch(event: Record<string, unknown>, heldPort: MessagePort | null = null): void {
    if (!this.core) {
      this.queuedEvents.push({ event, heldPort });
      return;
    }
    this.heldPort = heldPort;
    try {
      this.executeCommands(this.core.handle(event));
    } finally {
      this.heldPort = null;
    }
  }

  private flushQueuedEvents(): void {
    while (this.core && this.queuedEvents.length > 0) {
      const queued = this.queuedEvents.shift();
      if (!queued) continue;
      this.dispatch(queued.event, queued.heldPort);
    }
  }

  private executeCommands(commands: TabClientCommand[]): void {
    for (const command of commands) {
      switch (command.kind) {
        case "postToBroker":
          this.port?.postMessage(command.message);
          break;
        case "setTimer":
          this.setCoreTimer(command.timer as TabTimerKey, command.delayMs as number);
          break;
        case "clearTimer":
          this.clearCoreTimer(command.timer as TabTimerKey);
          break;
        case "settleRoleWaiter":
          this.settleWaiter(
            this.roleWaiters,
            command.waiterId as number,
            command.rejection as WaiterRejection | null,
          );
          break;
        case "settleResetStartWaiters":
          for (const waiterId of command.waiterIds as number[]) {
            this.settleWaiter(
              this.resetStartWaiters,
              waiterId,
              command.rejection as WaiterRejection | null,
            );
          }
          break;
        case "settleResetWaiters":
          for (const waiterId of command.waiterIds as number[]) {
            this.settleWaiter(
              this.resetWaiters,
              waiterId,
              command.rejection as WaiterRejection | null,
            );
          }
          break;
        case "invokeOnBecomeLeader": {
          const leadershipId = command.leadershipId as number;
          void Promise.resolve(
            this.options.onBecomeLeader?.(
              this,
              leadershipId,
              (command.resetRequestId as string | undefined) ?? undefined,
            ),
          ).catch((error) => {
            this.reportLeaderFailed(leadershipId, stringifyError(error));
          });
          break;
        }
        case "invokeOnDemote":
          void this.options.onDemote?.(command.leadershipId as number);
          break;
        case "invokeOnAttachFollowerPort":
          this.options.onAttachFollowerPort?.(
            command.followerTabId as string,
            command.leadershipId as number,
            this.heldPort as MessagePort,
          );
          break;
        case "invokeOnUseFollowerPort":
          this.options.onUseFollowerPort?.(
            command.leadershipId as number,
            this.heldPort as MessagePort,
          );
          break;
        case "invokeOnFollowerReady":
          this.options.onFollowerReady?.(command.leadershipId as number);
          break;
        case "invokeOnCloseFollowerPort":
          this.options.onCloseFollowerPort?.(command.leadershipId as number);
          break;
        case "invokeOnDetachFollowerPort":
          this.options.onDetachFollowerPort?.(
            command.followerTabId as string,
            command.leadershipId as number,
          );
          break;
        case "invokeOnStorageResetBegin": {
          const requestId = command.requestId as string;
          void Promise.resolve(
            this.options.onStorageResetBegin?.(requestId, command.leadershipId as number),
          )
            .then(() => {
              this.reportStorageResetReady(requestId, true);
            })
            .catch((error) => {
              this.reportStorageResetReady(requestId, false, stringifyError(error));
            });
          break;
        }
        case "invokeOnSchemaBlocked":
          this.options.onSchemaBlocked?.(command.reason as string);
          break;
        case "invokeOnReconnected":
          this.options.onReconnected?.(this);
          break;
        case "handleBrokerPing":
          this.options.onBrokerPing?.();
          if (this.shouldRespondToBrokerPing()) {
            this.port?.postMessage({
              type: "broker-pong",
              brokerInstanceId: command.brokerInstanceId,
            });
          }
          break;
        case "detachPort":
          if (this.port) {
            this.detachBrokerPort(this.port);
            this.port = null;
          }
          break;
        case "startReconnect":
          void this.runReconnect(
            command.previousRole as BrowserBrokerRole,
            command.previousLeadershipId as number,
          );
          break;
        case "closeWithError": {
          const message = command.message as string;
          const code = command.code as string | null;
          if (command.fromReconnectFailure && this.pendingReconnectCause !== undefined) {
            this.closedError = new Error(message, { cause: this.pendingReconnectCause });
          } else if (code) {
            this.closedError = createBrowserBrokerUnsupportedError(
              message,
              code as BrowserBrokerUnsupportedCode,
            );
          } else {
            this.closedError = new Error(message);
          }
          this.pendingReconnectCause = undefined;
          break;
        }
        case "invokeOnClosed":
          this.options.onClosed?.(this.closedError ?? new Error("Browser broker client closed"));
          break;
      }
    }
  }

  private settleWaiter(
    waiters: Map<number, PendingWaiter>,
    waiterId: number,
    rejection: WaiterRejection | null,
  ): void {
    const waiter = waiters.get(waiterId);
    if (!waiter) return;
    waiters.delete(waiterId);
    if (!rejection) {
      waiter.resolve();
      return;
    }
    if (rejection.kind === "closedError") {
      waiter.reject(this.closedError ?? new Error("Browser broker client closed"));
      return;
    }
    waiter.reject(new Error(rejection.message));
  }

  private reportStorageResetReady(
    requestId: string,
    success: boolean,
    errorMessage?: string,
  ): void {
    this.dispatch({
      kind: "sendRequested",
      message: {
        type: "storage-reset-ready",
        requestId,
        success,
        ...(errorMessage ? { errorMessage } : {}),
      },
    });
  }

  private async runReconnect(
    previousRole: BrowserBrokerRole,
    previousLeadershipId: number,
  ): Promise<void> {
    this.reconnectDone = new Promise((resolve) => {
      this.resolveReconnectDone = resolve;
    });

    let reconnectError: unknown;
    try {
      if (previousLeadershipId > 0) {
        await this.options.onDemote?.(previousLeadershipId);
      }
      if (previousRole === "follower" && previousLeadershipId > 0) {
        this.options.onCloseFollowerPort?.(previousLeadershipId);
      }

      if (!this.coreSnapshot().closed) {
        await this.connectToBroker();
      }
    } catch (error) {
      reconnectError = error;
    }

    this.pendingReconnectCause = reconnectError;
    const finished = {
      kind: "reconnectFinished",
      ...(reconnectError !== undefined ? { error: stringifyError(reconnectError) } : {}),
    };
    this.resolveReconnectDone?.();
    this.resolveReconnectDone = null;
    this.reconnectDone = null;
    this.dispatch(finished);
    this.pendingReconnectCause = undefined;
  }

  private shouldRespondToBrokerPing(): boolean {
    const respond = this.options.respondToBrokerPings;
    if (typeof respond === "function") {
      return respond();
    }
    return respond !== false;
  }

  private setCoreTimer(timer: TabTimerKey, delayMs: number): void {
    const key = JSON.stringify(timer);
    clearTimeout(this.timers.get(key));
    this.timers.set(
      key,
      setTimeout(() => {
        this.timers.delete(key);
        this.dispatch({ kind: "timerFired", timer });
      }, delayMs),
    );
  }

  private clearCoreTimer(timer: TabTimerKey): void {
    const key = JSON.stringify(timer);
    const handle = this.timers.get(key);
    if (!handle) return;
    clearTimeout(handle);
    this.timers.delete(key);
  }

  private detachBrokerPort(port: MessagePort): void {
    port.removeEventListener("message", this.onMessage);
    port.removeEventListener("messageerror", this.onPortMessageError);
    port.close();
  }
}
