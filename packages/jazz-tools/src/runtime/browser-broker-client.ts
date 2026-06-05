import {
  detectBrowserBrokerMissingCapabilities,
  formatUnsupportedBrowserBrokerError,
  type BrowserBrokerCapabilityGlobal,
  type BrowserBrokerControlMessage,
  type BrowserBrokerRole,
  type BrowserBrokerVisibility,
} from "./browser-broker-protocol.js";

export interface BrowserBrokerClientSnapshot {
  brokerEpoch: string | null;
  role: BrowserBrokerRole;
  tabId: string;
  leaderTabId: string | null;
  term: number;
}

export interface BrowserBrokerLeaderReadyInput {
  term: number;
  tabLockName: string;
  workerLockName: string;
  compatibilityLockName?: string;
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
  globalLike?: BrowserBrokerCapabilityGlobal;
  respondToBrokerPings?: boolean | (() => boolean);
  onBrokerPing?: () => void;
  onBecomeLeader?: (client: BrowserBrokerClient, term: number) => void | Promise<void>;
  onDemote?: (term: number) => void | Promise<void>;
  onAttachFollowerPort?: (followerTabId: string, term: number, port: MessagePort) => void;
  onUseFollowerPort?: (leaderTabId: string, term: number, port: MessagePort) => void;
  onFollowerReady?: (leaderTabId: string, term: number) => void;
  onCloseFollowerPort?: (term: number) => void;
}

type RoleWaiter = {
  role: BrowserBrokerRole;
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
  private readonly worker: SharedWorker;
  private readonly port: MessagePort;
  private readonly options: BrowserBrokerClientOptions;
  private brokerEpoch: string | null = null;
  private role: BrowserBrokerRole = "follower";
  private leaderTabId: string | null = null;
  private term = 0;
  private closed = false;
  private readonly roleWaiters = new Set<RoleWaiter>();

  private constructor(worker: SharedWorker, options: BrowserBrokerClientOptions) {
    this.worker = worker;
    this.port = worker.port;
    this.options = options;
  }

  static async connect(options: BrowserBrokerClientOptions): Promise<BrowserBrokerClient> {
    const globalLike = options.globalLike ?? (globalThis as BrowserBrokerCapabilityGlobal);
    const missing = detectBrowserBrokerMissingCapabilities(globalLike);
    if (missing.length > 0) {
      throw new Error(formatUnsupportedBrowserBrokerError(missing));
    }

    const SharedWorkerCtor = globalLike.SharedWorker as SharedWorkerConstructor;
    const worker = new SharedWorkerCtor(
      new URL("../worker/jazz-broker-worker.js", import.meta.url),
      {
        type: "module",
        name: `jazz-broker:${options.appId}:${options.dbName}`,
      },
    );

    const client = new BrowserBrokerClient(worker, options);
    await client.start();
    return client;
  }

  snapshot(): BrowserBrokerClientSnapshot {
    return {
      brokerEpoch: this.brokerEpoch,
      role: this.role,
      tabId: this.options.tabId,
      leaderTabId: this.leaderTabId,
      term: this.term,
    };
  }

  async waitForRole(role: BrowserBrokerRole, timeoutMs = 5_000): Promise<void> {
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
    if (this.closed) return;
    this.port.postMessage({
      type: "leader-ready",
      term: input.term,
      tabLockName: input.tabLockName,
      workerLockName: input.workerLockName,
      compatibilityLockName: input.compatibilityLockName,
    });
  }

  reportLeaderFailed(term: number, reason: string): void {
    if (this.closed) return;
    this.port.postMessage({
      type: "leader-failed",
      term,
      reason,
    });
  }

  reportVisibility(visibility: BrowserBrokerVisibility): void {
    if (this.closed) return;
    this.port.postMessage({ type: "visibility", visibility });
  }

  reportFollowerPortAttached(followerTabId: string, term: number): void {
    if (this.closed) return;
    this.port.postMessage({
      type: "follower-port-attached",
      followerTabId,
      term,
    });
  }

  requestStorageReset(requestId: string): void {
    if (this.closed) return;
    this.port.postMessage({
      type: "storage-reset-request",
      requestId,
    });
  }

  async shutdown(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.port.postMessage({ type: "shutdown" });
    this.worker.port.close();
    for (const waiter of this.roleWaiters) {
      clearTimeout(waiter.timeout);
      waiter.reject(new Error("Browser broker client closed"));
    }
    this.roleWaiters.clear();
  }

  private async start(): Promise<void> {
    this.port.addEventListener("message", this.onMessage);
    this.port.start();

    const hello = new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => {
        cleanup();
        reject(new Error("Timed out waiting for browser broker hello"));
      }, 5_000);

      const cleanup = () => {
        clearTimeout(timeout);
        this.port.removeEventListener("message", onHello);
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
          reject(new Error(message.reason));
        }
      };

      this.port.addEventListener("message", onHello);
    });

    this.port.postMessage({
      type: "hello",
      tabId: this.options.tabId,
      appId: this.options.appId,
      dbName: this.options.dbName,
      fingerprint: this.options.fingerprint,
      visibility: this.options.visibility,
      forceTakeoverTimeoutMs: this.options.forceTakeoverTimeoutMs,
      brokerPingIntervalMs: this.options.brokerPingIntervalMs,
      brokerPongTimeoutMs: this.options.brokerPongTimeoutMs,
    });

    await hello;
  }

  private readonly onMessage = (event: MessageEvent): void => {
    this.handleControlMessage(event.data as BrowserBrokerControlMessage);
  };

  private handleControlMessage(message: BrowserBrokerControlMessage): void {
    if (!message || typeof message !== "object") return;
    if (this.brokerEpoch && message.brokerEpoch !== this.brokerEpoch) {
      return;
    }

    switch (message.type) {
      case "broker-hello":
        this.brokerEpoch = message.brokerEpoch;
        return;
      case "broker-ping":
        this.options.onBrokerPing?.();
        if (this.shouldRespondToBrokerPing()) {
          this.port.postMessage({ type: "broker-pong", brokerEpoch: message.brokerEpoch });
        }
        return;
      case "become-leader":
        this.term = message.term;
        void Promise.resolve(this.options.onBecomeLeader?.(this, message.term)).catch((error) => {
          this.reportLeaderFailed(message.term, stringifyError(error));
        });
        return;
      case "demote":
        if (message.term !== this.term) return;
        this.role = "follower";
        this.leaderTabId = null;
        this.resolveRoleWaiters();
        void this.options.onDemote?.(message.term);
        return;
      case "leader-ready":
        this.term = message.term;
        this.leaderTabId = message.leaderTabId;
        this.role = message.leaderTabId === this.options.tabId ? "leader" : "follower";
        this.resolveRoleWaiters();
        return;
      case "attach-follower-port":
        if (message.term !== this.term) return;
        this.options.onAttachFollowerPort?.(message.followerTabId, message.term, message.port);
        return;
      case "use-follower-port":
        this.term = message.term;
        this.leaderTabId = message.leaderTabId;
        this.role = "follower";
        this.options.onUseFollowerPort?.(message.leaderTabId, message.term, message.port);
        return;
      case "follower-ready":
        this.term = message.term;
        this.leaderTabId = message.leaderTabId;
        this.role = "follower";
        this.options.onFollowerReady?.(message.leaderTabId, message.term);
        this.resolveRoleWaiters();
        return;
      case "close-follower-port":
        this.options.onCloseFollowerPort?.(message.term);
        return;
      case "unsupported":
        this.closed = true;
        this.worker.port.close();
        this.rejectRoleWaiters(new Error(message.reason));
        return;
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

  private shouldRespondToBrokerPing(): boolean {
    const respond = this.options.respondToBrokerPings;
    if (typeof respond === "function") {
      return respond();
    }
    return respond !== false;
  }
}

function stringifyError(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
