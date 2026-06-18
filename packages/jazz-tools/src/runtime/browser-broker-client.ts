import {
  detectBrowserBrokerMissingCapabilities,
  formatUnsupportedBrowserBrokerError,
  stringifyError,
  type BrowserBrokerCapabilityGlobal,
  type BrowserBrokerControlMessage,
  type BrowserBrokerRole,
  type BrowserBrokerTabMessage,
  type BrowserBrokerVisibility,
} from "./browser-broker-protocol.js";
import {
  createBrowserBrokerUnsupportedError,
  type BrowserBrokerUnsupportedCode,
} from "./browser-broker-errors.js";
import type { RuntimeSourcesConfig } from "./context.js";
import {
  resolveRuntimeConfigBrokerWorkerUrl,
  resolveRuntimeConfigSyncInitInput,
  resolveRuntimeConfigWasmUrl,
} from "./runtime-config.js";

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
  onUseFollowerPort?: (leaderTabId: string, leadershipId: number, port: MessagePort) => void;
  onFollowerReady?: (leaderTabId: string, leadershipId: number) => void;
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

type BrokerClientBinding = {
  handleEvent(event: BrokerClientEvent): BrokerClientEffect[];
  snapshot(): BrowserBrokerClientSnapshot & { closed?: boolean; reconnecting?: boolean };
};

type BrokerClientModule = {
  BrokerClient: new () => BrokerClientBinding;
  default?: (input?: unknown) => Promise<unknown>;
  initSync?: (input?: unknown) => unknown;
};

type BrokerClientEvent =
  | ({ type: "connectRequested" } & ConnectRequestedEvent)
  | { type: "publicCommand"; command: BrokerClientCommand }
  | {
      type: "brokerMessageReceived";
      message: BrokerControlMessageForCore;
      respondToBrokerPing: boolean;
      nowMs: number;
    }
  | { type: "timerFired"; timerId: number; kind: BrokerClientTimerKind; nowMs: number }
  | { type: "callbackResolved"; callbackId: number; nowMs: number }
  | { type: "callbackRejected"; callbackId: number; errorMessage: string; nowMs: number }
  | { type: "workerError"; workerId: number; errorMessage: string; nowMs: number }
  | { type: "portMessageError"; portId: number; nowMs: number };

interface ConnectRequestedEvent {
  appId: string;
  dbName: string;
  tabId: string;
  fingerprint: string;
  visibility: BrowserBrokerVisibility;
  forceTakeoverTimeoutMs?: number;
  brokerPingIntervalMs?: number;
  brokerPongTimeoutMs?: number;
  storageResetTimeoutMs?: number;
  nowMs: number;
}

type BrokerClientCommand =
  | { type: "waitForRole"; role: BrowserBrokerRole; promiseId: number; timeoutMs: number }
  | {
      type: "reportLeaderReady";
      leadershipId: number;
      tabLockName: string;
      workerLockName: string;
      bridgelessStorageReset: boolean;
    }
  | { type: "reportLeaderFailed"; leadershipId: number; reason: string }
  | { type: "reportVisibility"; visibility: BrowserBrokerVisibility }
  | { type: "reportFollowerPortAttached"; followerTabId: string; leadershipId: number }
  | { type: "reportFollowerPortClosed"; followerTabId: string; leadershipId: number }
  | { type: "reportSchemaReady"; schemaFingerprint: string }
  | {
      type: "requestStorageReset";
      requestId: string;
      startPromiseId: number;
      completionPromiseId: number;
    }
  | {
      type: "reportStorageResetReady";
      requestId: string;
      success: boolean;
      errorMessage?: string;
    }
  | { type: "shutdown" };

type BrokerClientTimerKind =
  | { type: "brokerHello" }
  | { type: "initialLeadership" }
  | { type: "brokerLiveness" }
  | { type: "roleWaiter"; promiseId: number }
  | { type: "storageResetStart"; requestId: string; promiseId: number };

// Mirrors the serde-wasm-bindgen event/effect/callback shapes in
// crates/jazz-wasm/src/broker_client.rs. Keep the Rust serialization tests and
// these unions in sync until the protocol declarations are generated.
type BrokerClientCallback =
  | { type: "brokerPing" }
  | { type: "becomeLeader"; leadershipId: number; resetRequestId?: string }
  | { type: "demote"; leadershipId: number }
  | {
      type: "attachFollowerPort";
      followerTabId: string;
      leadershipId: number;
      portId?: number;
    }
  | { type: "detachFollowerPort"; followerTabId: string; leadershipId: number }
  | { type: "useFollowerPort"; leaderTabId: string; leadershipId: number; portId?: number }
  | { type: "followerReady"; leaderTabId: string; leadershipId: number }
  | { type: "closeFollowerPort"; leadershipId: number }
  | { type: "storageResetBegin"; requestId: string; leadershipId: number }
  | { type: "schemaBlocked"; reason: string }
  | { type: "reconnected" };

type BrokerClientEffect =
  | { type: "createSharedWorker"; workerId: number; name: string }
  | { type: "attachPortListeners"; portId: number }
  | { type: "detachPort"; portId: number; close: boolean }
  | { type: "postToBroker"; portId: number; message: BrowserBrokerTabMessage }
  | { type: "armTimer"; timerId: number; kind: BrokerClientTimerKind; delayMs: number }
  | { type: "cancelTimer"; timerId: number }
  | { type: "releaseMessagePort"; portId: number }
  | { type: "invokeCallback"; callbackId: number; callback: BrokerClientCallback }
  | { type: "resolveConnect" }
  | { type: "rejectConnect"; reason: string; code?: BrowserBrokerUnsupportedCode }
  | { type: "resolvePublicPromise"; promiseId: number }
  | { type: "rejectPublicPromise"; promiseId: number; reason: string }
  | { type: "closeClient"; reason: string; code?: BrowserBrokerUnsupportedCode };

type BrokerControlMessageForCore =
  | Exclude<
      BrowserBrokerControlMessage,
      { type: "attach-follower-port" } | { type: "use-follower-port" }
    >
  | (Omit<Extract<BrowserBrokerControlMessage, { type: "attach-follower-port" }>, "port"> & {
      portId?: number;
    })
  | (Omit<Extract<BrowserBrokerControlMessage, { type: "use-follower-port" }>, "port"> & {
      portId?: number;
    });

type PublicPromise = {
  resolve: () => void;
  reject: (error: Error) => void;
};

type ConnectWaiter = PublicPromise;

type PreconnectedWorker = {
  workerId: number;
  portId: number;
  worker: SharedWorker;
  port: MessagePort;
  queuedMessages: BrowserBrokerControlMessage[];
  cleanup: () => void;
  helloPosted: boolean;
};

export class BrowserBrokerClient {
  private readonly options: BrowserBrokerClientOptions;
  private core: BrokerClientBinding | null = null;
  private worker: SharedWorker | null = null;
  private port: MessagePort | null = null;
  private preconnectedWorker: PreconnectedWorker | null = null;
  private pendingWorker: { workerId: number; worker: SharedWorker } | null = null;
  private readonly workers = new Map<number, SharedWorker>();
  private readonly workerCleanups = new Map<number, () => void>();
  private readonly ports = new Map<number, MessagePort>();
  private readonly portCleanups = new Map<number, () => void>();
  private readonly portToWorker = new Map<number, number>();
  private readonly timers = new Map<number, ReturnType<typeof setTimeout>>();
  private readonly publicPromises = new Map<number, PublicPromise>();
  private readonly messagePorts = new Map<number, MessagePort>();
  private readonly suppressHelloPortIds = new Set<number>();
  private nextPromiseId = 1;
  private nextMessagePortId = 1;
  private connectWaiter: ConnectWaiter | null = null;
  private closed = false;
  private closedError: Error | null = null;

  private constructor(options: BrowserBrokerClientOptions) {
    this.options = options;
  }

  static async connect(options: BrowserBrokerClientOptions): Promise<BrowserBrokerClient> {
    const globalLike = options.globalLike ?? (globalThis as BrowserBrokerCapabilityGlobal);
    const missing = detectBrowserBrokerMissingCapabilities(globalLike);
    if (missing.length > 0) {
      throw new Error(formatUnsupportedBrowserBrokerError(missing));
    }

    const wasmModule = loadBrokerClientWasmModule(options.runtimeSources);
    const client = new BrowserBrokerClient(options);
    await client.connectToBroker(wasmModule);
    return client;
  }

  snapshot(): BrowserBrokerClientSnapshot {
    const snapshot = this.core?.snapshot();
    return {
      brokerInstanceId: snapshot?.brokerInstanceId ?? null,
      role: snapshot?.role ?? "follower",
      tabId: snapshot?.tabId ?? this.options.tabId,
      leaderTabId: snapshot?.leaderTabId ?? null,
      leadershipId: snapshot?.leadershipId ?? 0,
    };
  }

  async waitForRole(role: BrowserBrokerRole, timeoutMs = 5_000): Promise<void> {
    if (this.closed) {
      throw this.closedError ?? new Error("Browser broker client closed");
    }

    const promiseId = this.nextPublicPromiseId();
    const promise = this.createPublicPromise(promiseId);
    this.runCoreEvent({
      type: "publicCommand",
      command: {
        type: "waitForRole",
        role,
        promiseId,
        timeoutMs: sanitizeOptionalTimeoutMs(timeoutMs) ?? 5_000,
      },
    });
    return await promise;
  }

  reportLeaderReady(input: BrowserBrokerLeaderReadyInput): void {
    this.runCoreEvent({
      type: "publicCommand",
      command: {
        type: "reportLeaderReady",
        leadershipId: input.leadershipId,
        tabLockName: input.tabLockName,
        workerLockName: input.workerLockName,
        bridgelessStorageReset: input.bridgelessStorageReset === true,
      },
    });
  }

  reportLeaderFailed(leadershipId: number, reason: string): void {
    this.runCoreEvent({
      type: "publicCommand",
      command: { type: "reportLeaderFailed", leadershipId, reason },
    });
  }

  reportVisibility(visibility: BrowserBrokerVisibility): void {
    this.runCoreEvent({
      type: "publicCommand",
      command: { type: "reportVisibility", visibility },
    });
  }

  reportFollowerPortAttached(followerTabId: string, leadershipId: number): void {
    this.runCoreEvent({
      type: "publicCommand",
      command: { type: "reportFollowerPortAttached", followerTabId, leadershipId },
    });
  }

  reportFollowerPortClosed(followerTabId: string, leadershipId: number): void {
    this.runCoreEvent({
      type: "publicCommand",
      command: { type: "reportFollowerPortClosed", followerTabId, leadershipId },
    });
  }

  reportSchemaReady(schemaFingerprint: string): void {
    this.runCoreEvent({
      type: "publicCommand",
      command: { type: "reportSchemaReady", schemaFingerprint },
    });
  }

  async requestStorageReset(requestId: string): Promise<void> {
    if (this.closed) {
      throw this.closedError ?? new Error("Browser broker client closed");
    }

    const startPromiseId = this.nextPublicPromiseId();
    const completionPromiseId = this.nextPublicPromiseId();
    const started = this.createPublicPromise(startPromiseId);
    const completion = this.createPublicPromise(completionPromiseId);

    this.runCoreEvent({
      type: "publicCommand",
      command: {
        type: "requestStorageReset",
        requestId,
        startPromiseId,
        completionPromiseId,
      },
    });

    try {
      await started;
      await completion;
    } catch (error) {
      this.publicPromises.delete(completionPromiseId);
      throw error;
    }
  }

  async shutdown(): Promise<void> {
    if (this.closed) return;
    this.runCoreEvent({ type: "publicCommand", command: { type: "shutdown" } });
    this.closed = true;
    const error = new Error("Browser broker client closed");
    this.closedError = error;
    this.preconnectedWorker?.cleanup();
    this.preconnectedWorker?.port.close();
    this.preconnectedWorker = null;
    this.connectWaiter?.reject(error);
    this.connectWaiter = null;
    this.rejectAllPublicPromises(this.closedError);
  }

  private connectToBroker(wasmModulePromise: Promise<BrokerClientModule>): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.connectWaiter = { resolve, reject };
      let preconnected: PreconnectedWorker;
      try {
        preconnected = this.startPreconnectedWorker();
        this.preconnectedWorker = preconnected;
      } catch (error) {
        this.failConnect(wrapUnknownError(error));
        return;
      }

      void wasmModulePromise.then(
        (wasmModule) => {
          if (this.closed) {
            preconnected.cleanup();
            preconnected.port.close();
            return;
          }
          this.core = new wasmModule.BrokerClient();
          this.runCoreEvent(this.connectRequestedEvent());
          this.replayPreconnectedMessages(preconnected);
        },
        (error) => {
          if (this.closed) return;
          this.failConnect(wrapUnknownError(error));
        },
      );
    });
  }

  private connectRequestedEvent(): BrokerClientEvent {
    return {
      type: "connectRequested",
      appId: this.options.appId,
      dbName: this.options.dbName,
      tabId: this.options.tabId,
      fingerprint: this.options.fingerprint,
      visibility: this.options.visibility,
      forceTakeoverTimeoutMs: sanitizeOptionalTimeoutMs(this.options.forceTakeoverTimeoutMs),
      brokerPingIntervalMs: sanitizeOptionalTimeoutMs(this.options.brokerPingIntervalMs),
      brokerPongTimeoutMs: sanitizeOptionalTimeoutMs(this.options.brokerPongTimeoutMs),
      storageResetTimeoutMs: sanitizeOptionalTimeoutMs(this.options.storageResetTimeoutMs),
      nowMs: Date.now(),
    };
  }

  private startPreconnectedWorker(): PreconnectedWorker {
    const workerId = 1;
    const portId = 1;
    const worker = this.createSharedWorker(this.brokerWorkerName());
    const port = worker.port;
    const queuedMessages: BrowserBrokerControlMessage[] = [];

    this.worker = worker;
    this.port = port;
    this.suppressHelloPortIds.add(portId);

    const onMessage = (event: MessageEvent) => {
      queuedMessages.push(event.data as BrowserBrokerControlMessage);
    };
    const onPortMessageError = () => {
      this.failConnect(new Error("Browser broker port message error"));
    };
    port.addEventListener("message", onMessage);
    port.addEventListener("messageerror", onPortMessageError);
    port.start();

    let workerCleanup = () => {};
    if (typeof (worker as Partial<EventTarget>).addEventListener === "function") {
      const workerEvents = worker as unknown as EventTarget;
      const onWorkerError = (event: Event) => {
        const detail =
          (event as ErrorEvent).message ||
          "worker error event (possible script URL or version mismatch)";
        this.failConnect(new Error(`Browser broker SharedWorker failed to start: ${detail}`));
      };
      workerEvents.addEventListener("error", onWorkerError);
      workerCleanup = () => workerEvents.removeEventListener("error", onWorkerError);
    }

    port.postMessage(this.helloMessage());

    return {
      workerId,
      portId,
      worker,
      port,
      queuedMessages,
      helloPosted: true,
      cleanup: () => {
        port.removeEventListener("message", onMessage);
        port.removeEventListener("messageerror", onPortMessageError);
        workerCleanup();
      },
    };
  }

  private replayPreconnectedMessages(preconnected: PreconnectedWorker): void {
    for (const message of preconnected.queuedMessages.splice(0)) {
      this.handleControlMessage(preconnected.portId, message);
    }
  }

  private runCoreEvent(event: BrokerClientEvent): void {
    if (this.closed && event.type !== "callbackResolved" && event.type !== "callbackRejected") {
      return;
    }

    let effects: BrokerClientEffect[];
    try {
      if (!this.core) return;
      effects = this.core.handleEvent(event);
    } catch (error) {
      this.handleEffectExecutionError(error);
      return;
    }
    this.executeEffects(effects);
  }

  private executeEffects(effects: BrokerClientEffect[]): void {
    for (const effect of effects) {
      try {
        this.executeEffect(effect);
      } catch (error) {
        this.handleEffectExecutionError(error);
        return;
      }
    }
  }

  private executeEffect(effect: BrokerClientEffect): void {
    switch (effect.type) {
      case "createSharedWorker": {
        const worker =
          this.preconnectedWorker?.workerId === effect.workerId
            ? this.preconnectedWorker.worker
            : this.createSharedWorker(effect.name);
        this.worker = worker;
        this.workers.set(effect.workerId, worker);
        this.pendingWorker = { workerId: effect.workerId, worker };
        if (this.preconnectedWorker?.workerId !== effect.workerId) {
          this.attachWorkerErrorListener(effect.workerId, worker);
        }
        return;
      }
      case "attachPortListeners":
        this.attachPort(effect.portId);
        return;
      case "detachPort":
        this.detachPort(effect.portId, effect.close);
        return;
      case "postToBroker": {
        if (effect.message.type === "hello" && this.suppressHelloPortIds.delete(effect.portId)) {
          return;
        }
        this.ports.get(effect.portId)?.postMessage(effect.message);
        return;
      }
      case "armTimer":
        this.armTimer(effect.timerId, effect.kind, effect.delayMs);
        return;
      case "cancelTimer":
        this.cancelTimer(effect.timerId);
        return;
      case "releaseMessagePort":
        this.releaseMessagePort(effect.portId);
        return;
      case "invokeCallback":
        this.invokeCallback(effect.callbackId, effect.callback);
        return;
      case "resolveConnect":
        this.connectWaiter?.resolve();
        this.connectWaiter = null;
        return;
      case "rejectConnect":
        this.failConnect(errorFromBroker(effect.reason, effect.code));
        return;
      case "resolvePublicPromise": {
        const promise = this.publicPromises.get(effect.promiseId);
        this.publicPromises.delete(effect.promiseId);
        promise?.resolve();
        return;
      }
      case "rejectPublicPromise": {
        const promise = this.publicPromises.get(effect.promiseId);
        this.publicPromises.delete(effect.promiseId);
        promise?.reject(new Error(effect.reason));
        return;
      }
      case "closeClient":
        this.closeWithError(errorFromBroker(effect.reason, effect.code));
        return;
    }
  }

  private createSharedWorker(name: string): SharedWorker {
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
    return new SharedWorkerCtor(workerUrl, { type: "module", name });
  }

  private brokerWorkerName(): string {
    return `jazz-broker:${this.options.appId}:${this.options.dbName}`;
  }

  private helloMessage(): BrowserBrokerTabMessage {
    return {
      type: "hello",
      tabId: this.options.tabId,
      appId: this.options.appId,
      dbName: this.options.dbName,
      fingerprint: this.options.fingerprint,
      visibility: this.options.visibility,
      forceTakeoverTimeoutMs: sanitizeOptionalTimeoutMs(this.options.forceTakeoverTimeoutMs),
      brokerPingIntervalMs: sanitizeOptionalTimeoutMs(this.options.brokerPingIntervalMs),
      brokerPongTimeoutMs: sanitizeOptionalTimeoutMs(this.options.brokerPongTimeoutMs),
    };
  }

  private attachWorkerErrorListener(workerId: number, worker: SharedWorker): void {
    if (typeof (worker as Partial<EventTarget>).addEventListener !== "function") return;

    const workerEvents = worker as unknown as EventTarget;
    const onWorkerError = (event: Event) => {
      const detail =
        (event as ErrorEvent).message ||
        "worker error event (possible script URL or version mismatch)";
      this.runCoreEvent({
        type: "workerError",
        workerId,
        errorMessage: detail,
        nowMs: Date.now(),
      });
    };
    workerEvents.addEventListener("error", onWorkerError);
    this.workerCleanups.set(workerId, () => {
      workerEvents.removeEventListener("error", onWorkerError);
    });
  }

  private attachPort(portId: number): void {
    const pending = this.pendingWorker;
    if (!pending) return;

    const port = pending.worker.port;
    if (this.preconnectedWorker?.portId === portId) {
      this.preconnectedWorker.cleanup();
      this.preconnectedWorker = null;
    }
    this.port = port;
    this.pendingWorker = null;
    this.ports.set(portId, port);
    this.portToWorker.set(portId, pending.workerId);

    const onMessage = (event: MessageEvent) => {
      this.handleControlMessage(portId, event.data as BrowserBrokerControlMessage);
    };
    const onPortMessageError = () => {
      this.runCoreEvent({ type: "portMessageError", portId, nowMs: Date.now() });
    };
    port.addEventListener("message", onMessage);
    port.addEventListener("messageerror", onPortMessageError);
    port.start();

    this.portCleanups.set(portId, () => {
      port.removeEventListener("message", onMessage);
      port.removeEventListener("messageerror", onPortMessageError);
    });
  }

  private detachPort(portId: number, close: boolean): void {
    const port = this.ports.get(portId);
    this.portCleanups.get(portId)?.();
    this.portCleanups.delete(portId);
    this.ports.delete(portId);

    const workerId = this.portToWorker.get(portId);
    this.portToWorker.delete(portId);
    if (workerId !== undefined) {
      this.workerCleanups.get(workerId)?.();
      this.workerCleanups.delete(workerId);
      this.workers.delete(workerId);
    }

    if (close) {
      port?.close();
    }
    if (this.port === port) {
      this.port = null;
    }
  }

  private handleControlMessage(_portId: number, rawMessage: BrowserBrokerControlMessage): void {
    if (!rawMessage || typeof rawMessage !== "object") return;
    const message = this.prepareControlMessage(rawMessage);
    this.runCoreEvent({
      type: "brokerMessageReceived",
      message,
      respondToBrokerPing: this.shouldRespondToBrokerPing(),
      nowMs: Date.now(),
    });
  }

  private prepareControlMessage(message: BrowserBrokerControlMessage): BrokerControlMessageForCore {
    if (message.type === "attach-follower-port" || message.type === "use-follower-port") {
      const portId = this.nextMessagePortId++;
      this.messagePorts.set(portId, message.port);
      const { port: _port, ...rest } = message;
      return { ...rest, portId } as BrokerControlMessageForCore;
    }
    return message;
  }

  private armTimer(timerId: number, kind: BrokerClientTimerKind, delayMs: number): void {
    this.cancelTimer(timerId);
    const timeout = setTimeout(() => {
      this.timers.delete(timerId);
      this.runCoreEvent({ type: "timerFired", timerId, kind, nowMs: Date.now() });
    }, delayMs);
    this.timers.set(timerId, timeout);
  }

  private cancelTimer(timerId: number): void {
    const timer = this.timers.get(timerId);
    if (!timer) return;
    clearTimeout(timer);
    this.timers.delete(timerId);
  }

  private invokeCallback(callbackId: number, callback: BrokerClientCallback): void {
    let result: void | Promise<void>;
    try {
      result = this.callCallback(callback);
    } catch (error) {
      this.runCoreEvent({
        type: "callbackRejected",
        callbackId,
        errorMessage: stringifyError(error),
        nowMs: Date.now(),
      });
      return;
    }

    void Promise.resolve(result).then(
      () => {
        this.runCoreEvent({ type: "callbackResolved", callbackId, nowMs: Date.now() });
      },
      (error) => {
        this.runCoreEvent({
          type: "callbackRejected",
          callbackId,
          errorMessage: stringifyError(error),
          nowMs: Date.now(),
        });
      },
    );
  }

  private callCallback(callback: BrokerClientCallback): void | Promise<void> {
    switch (callback.type) {
      case "brokerPing":
        return this.options.onBrokerPing?.();
      case "becomeLeader":
        return this.options.onBecomeLeader?.(this, callback.leadershipId, callback.resetRequestId);
      case "demote":
        return this.options.onDemote?.(callback.leadershipId);
      case "attachFollowerPort": {
        if (!this.options.onAttachFollowerPort) {
          this.releaseMessagePort(callback.portId);
          return;
        }
        const port = this.takeMessagePort(callback.portId);
        if (!port) return;
        return this.options.onAttachFollowerPort(
          callback.followerTabId,
          callback.leadershipId,
          port,
        );
      }
      case "detachFollowerPort":
        return this.options.onDetachFollowerPort?.(callback.followerTabId, callback.leadershipId);
      case "useFollowerPort": {
        if (!this.options.onUseFollowerPort) {
          this.releaseMessagePort(callback.portId);
          return;
        }
        const port = this.takeMessagePort(callback.portId);
        if (!port) return;
        return this.options.onUseFollowerPort(callback.leaderTabId, callback.leadershipId, port);
      }
      case "followerReady":
        return this.options.onFollowerReady?.(callback.leaderTabId, callback.leadershipId);
      case "closeFollowerPort":
        return this.options.onCloseFollowerPort?.(callback.leadershipId);
      case "storageResetBegin":
        return this.options.onStorageResetBegin?.(callback.requestId, callback.leadershipId);
      case "schemaBlocked":
        return this.options.onSchemaBlocked?.(callback.reason);
      case "reconnected":
        return this.options.onReconnected?.(this);
    }
  }

  private takeMessagePort(portId: number | undefined): MessagePort | null {
    if (portId === undefined) return null;
    const port = this.messagePorts.get(portId) ?? null;
    this.messagePorts.delete(portId);
    return port;
  }

  private releaseMessagePort(portId: number | undefined): void {
    if (portId === undefined) return;
    const port = this.messagePorts.get(portId);
    this.messagePorts.delete(portId);
    port?.close();
  }

  private shouldRespondToBrokerPing(): boolean {
    const respond = this.options.respondToBrokerPings;
    if (typeof respond === "function") {
      return respond();
    }
    return respond !== false;
  }

  private nextPublicPromiseId(): number {
    return this.nextPromiseId++;
  }

  private createPublicPromise(promiseId: number): Promise<void> {
    return new Promise((resolve, reject) => {
      this.publicPromises.set(promiseId, { resolve, reject });
    });
  }

  private failConnect(error: Error): void {
    this.closed = true;
    this.closedError = error;
    this.preconnectedWorker?.cleanup();
    this.preconnectedWorker?.port.close();
    this.preconnectedWorker = null;
    this.connectWaiter?.reject(error);
    this.connectWaiter = null;
    this.rejectAllPublicPromises(error);
  }

  private closeWithError(error: Error): void {
    if (this.closed) return;
    this.closed = true;
    this.closedError = error;
    this.clearAllTimers();
    for (const portId of [...this.ports.keys()]) {
      this.detachPort(portId, true);
    }
    this.preconnectedWorker?.cleanup();
    this.preconnectedWorker?.port.close();
    this.preconnectedWorker = null;
    this.rejectAllPublicPromises(error);
    this.connectWaiter?.reject(error);
    this.connectWaiter = null;
    this.options.onClosed?.(error);
  }

  private handleEffectExecutionError(error: unknown): void {
    const cause = error instanceof Error ? error : undefined;
    const wrapped = new Error(stringifyError(error), cause ? { cause } : undefined);
    if (this.connectWaiter) {
      this.failConnect(wrapped);
      for (const portId of [...this.ports.keys()]) {
        this.detachPort(portId, true);
      }
      return;
    }
    this.closeWithError(wrapped);
  }

  private rejectAllPublicPromises(error: Error): void {
    for (const promise of this.publicPromises.values()) {
      promise.reject(error);
    }
    this.publicPromises.clear();
  }

  private clearAllTimers(): void {
    for (const timer of this.timers.values()) {
      clearTimeout(timer);
    }
    this.timers.clear();
  }
}

let brokerClientWasmInitPromise: Promise<BrokerClientModule> | null = null;

async function loadBrokerClientWasmModule(
  runtime?: RuntimeSourcesConfig,
): Promise<BrokerClientModule> {
  if (brokerClientWasmInitPromise) {
    return brokerClientWasmInitPromise;
  }

  brokerClientWasmInitPromise = loadAndInitializeBrokerClientWasmModule(runtime).catch((error) => {
    brokerClientWasmInitPromise = null;
    throw error;
  });
  return brokerClientWasmInitPromise;
}

async function loadAndInitializeBrokerClientWasmModule(
  runtime?: RuntimeSourcesConfig,
): Promise<BrokerClientModule> {
  const wasmModule = (await import("jazz-wasm")) as unknown as BrokerClientModule;
  const syncInitInput = resolveRuntimeConfigSyncInitInput(runtime);

  if (syncInitInput && wasmModule.initSync) {
    wasmModule.initSync(syncInitInput);
    return wasmModule;
  }

  if (typeof process !== "undefined" && process.versions?.node && wasmModule.initSync) {
    try {
      const wasmBinary = tryLoadNodePackagedWasmBinarySync();
      if (wasmBinary) {
        wasmModule.initSync({ module: wasmBinary });
        return wasmModule;
      }
    } catch {
      // Browser-like runtimes may expose a partial process object.
    }
  }

  if (typeof wasmModule.default === "function") {
    const wasmUrl =
      typeof location !== "undefined"
        ? resolveRuntimeConfigWasmUrl(import.meta.url, location.href, runtime)
        : null;
    if (wasmUrl) {
      await wasmModule.default({ module_or_path: wasmUrl });
    } else {
      await wasmModule.default();
    }
  }

  return wasmModule;
}

function tryLoadNodePackagedWasmBinarySync(): Uint8Array | null {
  const moduleBuiltin = process.getBuiltinModule?.("module");
  const fsBuiltin = process.getBuiltinModule?.("fs");
  const pathBuiltin = process.getBuiltinModule?.("path");

  if (!moduleBuiltin || !fsBuiltin || !pathBuiltin) {
    return null;
  }

  const { createRequire } = moduleBuiltin;
  const { existsSync, readFileSync } = fsBuiltin;
  const { dirname, resolve } = pathBuiltin;

  const require = createRequire(import.meta.url);
  const packageJsonPath = require.resolve("jazz-wasm/package.json");
  const packageDir = dirname(packageJsonPath);
  const wasmPath = resolve(packageDir, "pkg/jazz_wasm_bg.wasm");

  if (!existsSync(wasmPath)) {
    return null;
  }

  return readFileSync(wasmPath);
}

function errorFromBroker(reason: string, code?: BrowserBrokerUnsupportedCode): Error {
  return code ? createBrowserBrokerUnsupportedError(reason, code) : new Error(reason);
}

function sanitizeOptionalTimeoutMs(value: number | undefined): number | undefined {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return undefined;
  }
  const normalized = Math.max(0, Math.floor(value));
  return normalized > 0 ? normalized : undefined;
}

function wrapUnknownError(error: unknown): Error {
  const cause = error instanceof Error ? error : undefined;
  return new Error(stringifyError(error), cause ? { cause } : undefined);
}
