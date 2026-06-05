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
  globalLike?: BrowserBrokerCapabilityGlobal;
  onBecomeLeader?: (client: BrowserBrokerClient, term: number) => void | Promise<void>;
  onDemote?: (term: number) => void | Promise<void>;
}

type RoleWaiter = {
  role: BrowserBrokerRole;
  resolve: () => void;
  reject: (error: Error) => void;
  timeout: ReturnType<typeof setTimeout>;
};

type SharedWorkerConstructor = new (
  scriptURL: string | URL,
  options?: string | SharedWorkerOptions,
) => SharedWorker;

export class BrowserBrokerClient {
  private readonly port: MessagePort;
  private readonly options: BrowserBrokerClientOptions;
  private brokerEpoch: string | null = null;
  private role: BrowserBrokerRole = "follower";
  private leaderTabId: string | null = null;
  private term = 0;
  private closed = false;
  private readonly roleWaiters = new Set<RoleWaiter>();

  private constructor(port: MessagePort, options: BrowserBrokerClientOptions) {
    this.port = port;
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

    const client = new BrowserBrokerClient(worker.port, options);
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

  reportVisibility(visibility: BrowserBrokerVisibility): void {
    if (this.closed) return;
    this.port.postMessage({ type: "visibility", visibility });
  }

  async shutdown(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    this.port.postMessage({ type: "shutdown" });
    this.port.close();
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
        this.port.postMessage({ type: "broker-pong", brokerEpoch: message.brokerEpoch });
        return;
      case "become-leader":
        this.term = message.term;
        this.role = "leader";
        this.leaderTabId = this.options.tabId;
        this.resolveRoleWaiters();
        void this.options.onBecomeLeader?.(this, message.term);
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
      case "unsupported":
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
}
