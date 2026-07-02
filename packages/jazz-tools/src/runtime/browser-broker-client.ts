import {
  DEFAULT_BROKER_PING_INTERVAL_MS,
  DEFAULT_BROKER_PONG_TIMEOUT_MS,
  detectBrowserBrokerMissingCapabilities,
  formatUnsupportedBrowserBrokerError,
  normalizePositiveTimeout,
  stringifyError,
  type BrowserBrokerCapabilityGlobal,
  type BrowserBrokerControlMessage,
  type BrowserBrokerRole,
  type BrowserBrokerTabMessage,
  type BrowserBrokerTabMessageInput,
  type BrowserBrokerVisibility,
} from "./browser-broker-protocol.js";
import { createBrowserBrokerUnsupportedError } from "./browser-broker-errors.js";
import type { RuntimeSourcesConfig } from "./context.js";
import { resolveConfiguredUrl, resolveRuntimeConfigBrokerWorkerUrl } from "./runtime-config.js";

const DEFAULT_STORAGE_RESET_TIMEOUT_MS = 5_000;

/**
 * The broker-worker script URL the SharedWorker is constructed with. A
 * SharedWorker's identity is `(script URL, name)`, so a second client (e.g. the
 * inspector overlay, running in a different bundle) can only *join* this broker
 * by constructing with this exact URL. Resolved against this module's
 * `import.meta.url`, so callers in the same bundle agree; cross-bundle callers
 * must forward the result and pass it back via `runtimeSources.brokerWorkerUrl`.
 */
export function resolveBrokerWorkerUrl(runtimeSources?: RuntimeSourcesConfig): string {
  if (runtimeSources?.brokerWorkerUrl || runtimeSources?.baseUrl) {
    return resolveRuntimeConfigBrokerWorkerUrl(
      import.meta.url,
      typeof location !== "undefined" ? location.href : undefined,
      runtimeSources,
    );
  }
  // Literal `new URL("<path>", import.meta.url)` so bundlers (Turbopack,
  // webpack, Vite) emit the worker script as an asset and rewrite this URL.
  // Must stay statically analyzable — do not extract into a helper.
  const bundledUrl = new URL("../worker/jazz-broker-worker.js", import.meta.url).href;
  // Turbopack's rewrite can yield a root-relative path (e.g. "/_next/static/…"),
  // and the broker fingerprint compares this string across bundles: absolutize
  // against the page exactly like the explicit-URL branch does, so a cross-bundle
  // client (the inspector overlay) resolving the forwarded URL lands on the
  // identical string.
  return resolveConfiguredUrl(
    bundledUrl,
    typeof location !== "undefined" ? location.href : undefined,
  );
}

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

type RoleWaiter = {
  role: BrowserBrokerRole;
  resolve: () => void;
  reject: (error: Error) => void;
  timeout: ReturnType<typeof setTimeout>;
};

type ResetWaiter = {
  resolve: () => void;
  reject: (error: Error) => void;
};

type ResetStartWaiter = {
  resolve: () => void;
  reject: (error: Error) => void;
  timeout: ReturnType<typeof setTimeout>;
};

type SharedWorkerConstructor = new (
  scriptURL: string | URL,
  options?: string | BrowserBrokerSharedWorkerOptions,
) => SharedWorker;

interface BrowserBrokerSharedWorkerOptions {
  type?: WorkerType;
  name?: string;
  credentials?: RequestCredentials;
}

export class BrowserBrokerClient {
  private readonly options: BrowserBrokerClientOptions;
  private worker: SharedWorker | null = null;
  private port: MessagePort | null = null;
  private brokerInstanceId: string | null = null;
  private role: BrowserBrokerRole = "follower";
  private leaderTabId: string | null = null;
  private leadershipId = 0;
  private visibility: BrowserBrokerVisibility;
  private closed = false;
  private closedError: Error | null = null;
  private reconnecting = false;
  private reconnectDone: Promise<void> | null = null;
  private resolveReconnectDone: (() => void) | null = null;
  private readonly roleWaiters = new Set<RoleWaiter>();
  private readonly resetWaiters = new Map<string, ResetWaiter[]>();
  private readonly resetStartWaiters = new Map<string, ResetStartWaiter[]>();
  private readonly queuedMessages: BrowserBrokerTabMessage[] = [];
  private brokerLivenessTimer: ReturnType<typeof setTimeout> | null = null;

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
    return {
      brokerInstanceId: this.brokerInstanceId,
      role: this.role,
      tabId: this.options.tabId,
      leaderTabId: this.leaderTabId,
      leadershipId: this.leadershipId,
    };
  }

  async waitForRole(role: BrowserBrokerRole, timeoutMs = 5_000): Promise<void> {
    if (this.closed) {
      throw this.closedError ?? new Error("Browser broker client closed");
    }
    if (this.role === role && this.leaderTabId !== null) {
      return;
    }

    await new Promise<void>((resolve, reject) => {
      const waiter: RoleWaiter = {
        role,
        resolve,
        reject,
        timeout: setTimeout(() => {
          this.roleWaiters.delete(waiter);
          reject(new Error(`Timed out waiting for broker role ${role}`));
        }, timeoutMs),
      };
      this.roleWaiters.add(waiter);
    });
  }

  reportLeaderReady(input: BrowserBrokerLeaderReadyInput): void {
    this.send({
      type: "leader-ready",
      leadershipId: input.leadershipId,
      tabLockName: input.tabLockName,
      workerLockName: input.workerLockName,
      ...(input.bridgelessStorageReset ? { bridgelessStorageReset: true } : {}),
    });
  }

  reportLeaderFailed(leadershipId: number, reason: string): void {
    this.send({
      type: "leader-failed",
      leadershipId,
      reason,
    });
  }

  reportVisibility(visibility: BrowserBrokerVisibility): void {
    this.visibility = visibility;
    this.send({ type: "visibility", visibility });
  }

  reportFollowerPortAttached(followerTabId: string, leadershipId: number): void {
    this.send({
      type: "follower-port-attached",
      followerTabId,
      leadershipId,
    });
  }

  reportFollowerPortClosed(followerTabId: string, leadershipId: number): void {
    this.send({
      type: "follower-port-closed",
      followerTabId,
      leadershipId,
    });
  }

  reportSchemaReady(schemaFingerprint: string): void {
    this.send({
      type: "schema-ready",
      schemaFingerprint,
    });
  }

  async requestStorageReset(requestId: string): Promise<void> {
    if (this.closed) {
      throw this.closedError ?? new Error("Browser broker client closed");
    }
    // A reconnect drops in-flight sends; wait for it to settle so the
    // reset request reaches the new broker instance instead of vanishing.
    while (this.reconnecting && this.reconnectDone) {
      await this.reconnectDone;
      if (this.closed) {
        throw this.closedError ?? new Error("Browser broker client closed");
      }
    }
    let startWaiter!: ResetStartWaiter;
    let waiter!: ResetWaiter;
    const started = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.removeResetStartWaiter(requestId, startWaiter);
        reject(new Error(`Timed out waiting for browser storage reset ${requestId} to start`));
      }, this.storageResetTimeoutMs());
      startWaiter = { resolve, reject, timeout };
      const waiters = this.resetStartWaiters.get(requestId) ?? [];
      waiters.push(startWaiter);
      this.resetStartWaiters.set(requestId, waiters);
    });
    const completion = new Promise<void>((resolve, reject) => {
      waiter = { resolve, reject };
      const waiters = this.resetWaiters.get(requestId) ?? [];
      waiters.push(waiter);
      this.resetWaiters.set(requestId, waiters);
    });
    this.send({
      type: "storage-reset-request",
      requestId,
    });
    try {
      await started;
      await completion;
    } catch (error) {
      this.removeResetWaiter(requestId, waiter);
      throw error;
    }
  }

  async shutdown(): Promise<void> {
    if (this.closed) return;
    const shutdownMessage = this.stampTabMessage({ type: "shutdown" });
    this.closed = true;
    this.closedError = new Error("Browser broker client closed");
    this.stopBrokerLivenessTimer();
    this.queuedMessages.length = 0;
    if (shutdownMessage) {
      this.port?.postMessage(shutdownMessage);
    }
    if (this.port) {
      this.detachBrokerPort(this.port);
    }
    for (const waiter of this.roleWaiters) {
      clearTimeout(waiter.timeout);
      waiter.reject(new Error("Browser broker client closed"));
    }
    this.roleWaiters.clear();
    this.rejectResetStartWaiters(new Error("Browser broker client closed"));
    this.rejectResetWaiters(new Error("Browser broker client closed"));
  }

  private async connectToBroker(): Promise<void> {
    const worker = this.createSharedWorker();
    const port = worker.port;
    this.worker = worker;
    this.port = port;

    port.addEventListener("message", this.onMessage);
    port.addEventListener("messageerror", this.onPortMessageError);
    port.start();

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
      await hello;
      await this.waitForInitialLeadershipMessage(port);
      this.refreshBrokerLivenessTimer();
      this.flushQueuedMessages();
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

  private async waitForInitialLeadershipMessage(port: MessagePort): Promise<void> {
    if (this.leadershipId > 0 || this.closed || this.port !== port) return;

    await new Promise<void>((resolve) => {
      let timeout: ReturnType<typeof setTimeout>;
      const cleanup = () => {
        clearTimeout(timeout);
        port.removeEventListener("message", onMessage);
        resolve();
      };
      const onMessage = () => {
        if (this.leadershipId > 0 || this.closed || this.port !== port) {
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
    return new SharedWorkerCtor(resolveBrokerWorkerUrl(this.options.runtimeSources), {
      type: "module",
      name: `jazz-broker:${this.options.appId}:${this.options.dbName}`,
    });
  }

  private readonly onMessage = (event: MessageEvent): void => {
    this.handleControlMessage(event.data as BrowserBrokerControlMessage);
  };

  private readonly onPortMessageError = (): void => {
    void this.reconnectAfterBrokerPortFailure(new Error("Browser broker port message error"));
  };

  private handleControlMessage(message: BrowserBrokerControlMessage): void {
    if (!message || typeof message !== "object") return;
    if (this.brokerInstanceId && message.brokerInstanceId !== this.brokerInstanceId) {
      void this.reconnectAfterBrokerInstanceChange(message.brokerInstanceId);
      return;
    }

    switch (message.type) {
      case "broker-hello":
        this.brokerInstanceId = message.brokerInstanceId;
        return;
      case "broker-ping":
        this.refreshBrokerLivenessTimer();
        this.options.onBrokerPing?.();
        if (this.shouldRespondToBrokerPing()) {
          this.port?.postMessage({
            type: "broker-pong",
            brokerInstanceId: message.brokerInstanceId,
          });
        }
        return;
      case "become-leader":
        this.leadershipId = message.leadershipId;
        void Promise.resolve(
          this.options.onBecomeLeader?.(this, message.leadershipId, message.resetRequestId),
        ).catch((error) => {
          this.reportLeaderFailed(message.leadershipId, stringifyError(error));
        });
        return;
      case "demote":
        if (message.leadershipId === this.leadershipId) {
          this.role = "follower";
          this.leaderTabId = null;
          this.resolveRoleWaiters();
        }
        void this.options.onDemote?.(message.leadershipId);
        return;
      case "leader-ready":
        this.leadershipId = message.leadershipId;
        this.leaderTabId = message.leaderTabId;
        this.role = message.leaderTabId === this.options.tabId ? "leader" : "follower";
        this.resolveRoleWaiters();
        return;
      case "attach-follower-port":
        if (message.leadershipId !== this.leadershipId) return;
        this.options.onAttachFollowerPort?.(
          message.followerTabId,
          message.leadershipId,
          message.port,
        );
        return;
      case "use-follower-port":
        this.leadershipId = message.leadershipId;
        this.leaderTabId = message.leaderTabId;
        this.role = "follower";
        this.options.onUseFollowerPort?.(message.leadershipId, message.port);
        return;
      case "follower-ready":
        this.leadershipId = message.leadershipId;
        this.leaderTabId = message.leaderTabId;
        this.role = "follower";
        this.options.onFollowerReady?.(message.leadershipId);
        this.resolveRoleWaiters();
        return;
      case "close-follower-port":
        this.options.onCloseFollowerPort?.(message.leadershipId);
        return;
      case "detach-follower-port":
        this.options.onDetachFollowerPort?.(message.followerTabId, message.leadershipId);
        return;
      case "storage-reset-begin":
        this.resolveResetStartWaiters(message.requestId);
        void Promise.resolve(
          this.options.onStorageResetBegin?.(message.requestId, message.leadershipId),
        )
          .then(() => {
            this.reportStorageResetReady(message.requestId, true);
          })
          .catch((error) => {
            this.reportStorageResetReady(message.requestId, false, stringifyError(error));
          });
        return;
      case "storage-reset-started":
        this.resolveResetStartWaiters(message.requestId);
        return;
      case "storage-reset-finished":
        this.resolveResetStartWaiters(message.requestId);
        this.resolveResetWaiters(message.requestId, message.success, message.errorMessage);
        return;
      case "schema-blocked":
        this.options.onSchemaBlocked?.(message.reason);
        return;
      case "unsupported":
        this.closeWithError(createBrowserBrokerUnsupportedError(message.reason, message.code));
        return;
    }
  }

  private async reconnectAfterBrokerInstanceChange(nextBrokerInstanceId: string): Promise<void> {
    await this.reconnectAfterBrokerPortFailure(
      new Error(
        `Browser broker instance changed from ${this.brokerInstanceId} to ${nextBrokerInstanceId}`,
      ),
    );
  }

  private async reconnectAfterBrokerPortFailure(_error: Error): Promise<void> {
    if (this.closed || this.reconnecting) return;
    this.reconnecting = true;
    this.reconnectDone = new Promise((resolve) => {
      this.resolveReconnectDone = resolve;
    });

    const previousRole = this.role;
    const previousLeadershipId = this.leadershipId;
    const previousPort = this.port;

    this.stopBrokerLivenessTimer();
    this.brokerInstanceId = null;
    this.role = "follower";
    this.leaderTabId = null;
    this.leadershipId = 0;
    this.queuedMessages.length = 0;
    const resetError = new Error("Browser broker restarted during storage reset");
    this.rejectResetStartWaiters(resetError);
    this.rejectResetWaiters(resetError);

    if (previousPort) {
      this.detachBrokerPort(previousPort);
      if (this.port === previousPort) {
        this.port = null;
      }
    }

    let reconnectError: unknown;
    try {
      if (previousLeadershipId > 0) {
        await this.options.onDemote?.(previousLeadershipId);
      }
      if (previousRole === "follower" && previousLeadershipId > 0) {
        this.options.onCloseFollowerPort?.(previousLeadershipId);
      }

      if (!this.closed) {
        await this.connectToBroker();
      }
    } catch (error) {
      reconnectError = error;
    }

    this.reconnecting = false;
    this.resolveReconnectDone?.();
    this.resolveReconnectDone = null;
    this.reconnectDone = null;
    if (reconnectError) {
      this.closeWithError(new Error(stringifyError(reconnectError), { cause: reconnectError }));
      return;
    }
    if (!this.closed) {
      this.send({ type: "visibility", visibility: this.visibility });
      this.options.onReconnected?.(this);
      this.flushQueuedMessages();
    }
  }

  private resolveRoleWaiters(): void {
    for (const waiter of this.roleWaiters) {
      if (this.role !== waiter.role || this.leaderTabId === null) {
        continue;
      }
      clearTimeout(waiter.timeout);
      this.roleWaiters.delete(waiter);
      waiter.resolve();
    }
  }

  private rejectRoleWaiters(error: Error): void {
    for (const waiter of this.roleWaiters) {
      clearTimeout(waiter.timeout);
      waiter.reject(error);
    }
    this.roleWaiters.clear();
  }

  private reportStorageResetReady(
    requestId: string,
    success: boolean,
    errorMessage?: string,
  ): void {
    this.send({
      type: "storage-reset-ready",
      requestId,
      success,
      ...(errorMessage ? { errorMessage } : {}),
    });
  }

  private resolveResetWaiters(
    requestId: string,
    success: boolean,
    errorMessage: string | undefined,
  ): void {
    const waiters = this.resetWaiters.get(requestId);
    if (!waiters) return;
    this.resetWaiters.delete(requestId);
    const error = success ? null : new Error(errorMessage ?? "Browser storage reset failed");
    for (const waiter of waiters) {
      if (error) {
        waiter.reject(error);
      } else {
        waiter.resolve();
      }
    }
  }

  private resolveResetStartWaiters(requestId: string): void {
    const waiters = this.resetStartWaiters.get(requestId);
    if (!waiters) return;
    this.resetStartWaiters.delete(requestId);
    for (const waiter of waiters) {
      clearTimeout(waiter.timeout);
      waiter.resolve();
    }
  }

  private removeResetStartWaiter(requestId: string, waiter: ResetStartWaiter): void {
    const waiters = this.resetStartWaiters.get(requestId);
    if (!waiters) return;
    const next = waiters.filter((candidate) => candidate !== waiter);
    if (next.length === 0) {
      this.resetStartWaiters.delete(requestId);
    } else {
      this.resetStartWaiters.set(requestId, next);
    }
  }

  private removeResetWaiter(requestId: string, waiter: ResetWaiter): void {
    const waiters = this.resetWaiters.get(requestId);
    if (!waiters) return;
    const next = waiters.filter((candidate) => candidate !== waiter);
    if (next.length === 0) {
      this.resetWaiters.delete(requestId);
    } else {
      this.resetWaiters.set(requestId, next);
    }
  }

  private rejectResetStartWaiters(error: Error): void {
    for (const waiters of this.resetStartWaiters.values()) {
      for (const waiter of waiters) {
        clearTimeout(waiter.timeout);
        waiter.reject(error);
      }
    }
    this.resetStartWaiters.clear();
  }

  private rejectResetWaiters(error: Error): void {
    for (const waiters of this.resetWaiters.values()) {
      for (const waiter of waiters) {
        waiter.reject(error);
      }
    }
    this.resetWaiters.clear();
  }

  private shouldRespondToBrokerPing(): boolean {
    const respond = this.options.respondToBrokerPings;
    if (typeof respond === "function") {
      return respond();
    }
    return respond !== false;
  }

  private refreshBrokerLivenessTimer(): void {
    this.stopBrokerLivenessTimer();
    if (this.closed) return;
    this.brokerLivenessTimer = setTimeout(() => {
      this.brokerLivenessTimer = null;
      void this.reconnectAfterBrokerPortFailure(
        new Error("Browser broker liveness timed out waiting for broker ping"),
      );
    }, this.brokerLivenessTimeoutMs());
  }

  private stopBrokerLivenessTimer(): void {
    if (!this.brokerLivenessTimer) return;
    clearTimeout(this.brokerLivenessTimer);
    this.brokerLivenessTimer = null;
  }

  private brokerLivenessTimeoutMs(): number {
    return (
      normalizePositiveTimeout(this.options.brokerPingIntervalMs, DEFAULT_BROKER_PING_INTERVAL_MS) +
      normalizePositiveTimeout(this.options.brokerPongTimeoutMs, DEFAULT_BROKER_PONG_TIMEOUT_MS)
    );
  }

  private storageResetTimeoutMs(): number {
    return normalizePositiveTimeout(
      this.options.storageResetTimeoutMs,
      DEFAULT_STORAGE_RESET_TIMEOUT_MS,
    );
  }

  private send(message: BrowserBrokerTabMessageInput): void {
    if (this.closed) return;
    const stampedMessage = this.stampTabMessage(message);
    if (!stampedMessage) return;
    if (this.reconnecting) return;
    if (!this.port) {
      this.queuedMessages.push(stampedMessage);
      return;
    }
    this.port.postMessage(stampedMessage);
  }

  private stampTabMessage(message: BrowserBrokerTabMessageInput): BrowserBrokerTabMessage | null {
    if (message.type === "hello") return message;
    if (!this.brokerInstanceId) return null;
    return { ...message, brokerInstanceId: this.brokerInstanceId };
  }

  private flushQueuedMessages(): void {
    if (this.closed || this.reconnecting || !this.port) return;
    const messages = this.queuedMessages.splice(0);
    for (const message of messages) {
      this.port.postMessage(message);
    }
  }

  private detachBrokerPort(port: MessagePort): void {
    port.removeEventListener("message", this.onMessage);
    port.removeEventListener("messageerror", this.onPortMessageError);
    port.close();
  }

  private closeWithError(error: Error): void {
    if (this.closed && this.closedError === error) return;
    this.closed = true;
    this.closedError = error;
    this.stopBrokerLivenessTimer();
    this.queuedMessages.length = 0;
    if (this.port) {
      this.detachBrokerPort(this.port);
      this.port = null;
    }
    this.rejectRoleWaiters(error);
    this.rejectResetStartWaiters(error);
    this.rejectResetWaiters(error);
    this.options.onClosed?.(error);
  }
}
