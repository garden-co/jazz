import { afterEach, describe, expect, it, vi } from "vitest";

const { loadWasmModuleMock, tryAcquireWebLockMock, FakeBrowserBrokerClient } = vi.hoisted(() => {
  const loadWasmModuleMock = vi.fn().mockResolvedValue({}) as any;
  const tryAcquireWebLockMock = vi.fn(async (_lockName?: string) => ({ release: vi.fn() }));
  const connectMock = vi.fn(async (options: any) => {
    const client = new FakeBrowserBrokerClient(options);
    await options.onBecomeLeader?.(client, 1);
    return client;
  });

  class FakeBrowserBrokerClient {
    static connect = connectMock;
    static instances: FakeBrowserBrokerClient[] = [];
    static leaderFailures: Array<{ leadershipId: number; reason: string }> = [];
    private readonly options: any;
    private role: "leader" | "follower" = "leader";
    private leaderTabId: string | null;
    private leadershipId = 1;

    constructor(options: any) {
      this.options = options;
      this.leaderTabId = options.tabId;
      FakeBrowserBrokerClient.instances.push(this);
    }

    snapshot() {
      return {
        brokerInstanceId: "test-broker",
        role: this.role,
        tabId: this.options.tabId,
        leaderTabId: this.leaderTabId,
        leadershipId: this.leadershipId,
      };
    }

    reportLeaderReady(): void {}

    reportLeaderFailed(leadershipId: number, reason: string): void {
      FakeBrowserBrokerClient.leaderFailures.push({ leadershipId, reason });
    }

    reportVisibility(): void {}

    reportFollowerPortAttached(): void {}

    reportSchemaReady(): void {}

    async shutdown(): Promise<void> {}

    async beginStorageReset(leadershipId: number): Promise<void> {
      await this.options.onStorageResetBegin?.("reset-test", leadershipId);
    }

    async demote(leadershipId: number): Promise<void> {
      if (leadershipId === this.leadershipId) {
        this.role = "follower";
        this.leaderTabId = null;
      }
      await this.options.onDemote?.(leadershipId);
    }
  }

  return { loadWasmModuleMock, tryAcquireWebLockMock, FakeBrowserBrokerClient };
});

vi.mock("./client.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./client.js")>();
  return {
    ...actual,
    loadWasmModule: loadWasmModuleMock,
  };
});

vi.mock("./browser-broker-client.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./browser-broker-client.js")>();
  return {
    ...actual,
    BrowserBrokerClient: FakeBrowserBrokerClient,
  };
});

vi.mock("./leader-lock.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./leader-lock.js")>();
  return {
    ...actual,
    acquireWebLockWithRetry: tryAcquireWebLockMock,
  };
});

import { Db, type DbConfig } from "./db.js";
import { WasmRuntimeModule } from "./wasm-runtime-module.js";

const originalWindow = (globalThis as Record<string, unknown>).window;
const originalLocation = globalThis.location;
const originalWorker = (globalThis as Record<string, unknown>).Worker;
const originalBroadcastChannel = (globalThis as Record<string, unknown>).BroadcastChannel;

async function createWorkerDb(config: DbConfig): Promise<Db> {
  const runtimeModule = new WasmRuntimeModule();
  await runtimeModule.load(config);
  return await Db.createWithWorker(config, runtimeModule);
}

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs: number,
  message: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`Timed out: ${message}`);
}

afterEach(() => {
  loadWasmModuleMock.mockClear();
  tryAcquireWebLockMock.mockClear();
  FakeBrowserBrokerClient.connect.mockClear();
  FakeBrowserBrokerClient.instances.splice(0);
  FakeBrowserBrokerClient.leaderFailures.splice(0);

  if (originalWindow === undefined) {
    delete (globalThis as Record<string, unknown>).window;
  } else {
    (globalThis as Record<string, unknown>).window = originalWindow;
  }

  if (originalLocation === undefined) {
    delete (globalThis as Record<string, unknown>).location;
  } else {
    (globalThis as Record<string, unknown>).location = originalLocation;
  }

  if (originalWorker === undefined) {
    delete (globalThis as Record<string, unknown>).Worker;
  } else {
    (globalThis as Record<string, unknown>).Worker = originalWorker;
  }

  if (originalBroadcastChannel === undefined) {
    delete (globalThis as Record<string, unknown>).BroadcastChannel;
  } else {
    (globalThis as Record<string, unknown>).BroadcastChannel = originalBroadcastChannel;
  }
});

describe("Db worker runtime bootstrap", () => {
  it("prefers explicit workerUrl and wasmUrl over baseUrl and fallback resolution", async () => {
    const spawnedWorkerUrls: string[] = [];

    class FakeWorker extends EventTarget {
      constructor(url: string | URL, _options?: WorkerOptions) {
        super();
        spawnedWorkerUrls.push(String(url));
        queueMicrotask(() => {
          const event = new Event("message");
          Object.defineProperty(event, "data", {
            value: { type: "ready" },
            configurable: true,
          });
          this.dispatchEvent(event);
        });
      }

      postMessage(): void {}

      terminate(): void {}
    }

    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/app/",
    };
    (globalThis as Record<string, unknown>).Worker = FakeWorker;

    const db = await createWorkerDb({
      appId: "worker-bootstrap-explicit-urls",
      driver: { type: "persistent", dbName: "worker-bootstrap-explicit-urls" },
      runtimeSources: {
        baseUrl: "/ignored/",
        workerUrl: "/custom/jazz-worker.js",
        wasmUrl: "/custom/jazz_wasm_bg.wasm",
      },
    });

    await db.shutdown();

    expect(spawnedWorkerUrls).toEqual([
      "http://localhost:3000/custom/jazz-worker.js?jazz-wasm-url=http%3A%2F%2Flocalhost%3A3000%2Fcustom%2Fjazz_wasm_bg.wasm",
    ]);
  });

  it("derives worker and wasm URLs from runtimeSources.baseUrl when explicit URLs are omitted", async () => {
    const spawnedWorkerUrls: string[] = [];

    class FakeWorker extends EventTarget {
      constructor(url: string | URL, _options?: WorkerOptions) {
        super();
        spawnedWorkerUrls.push(String(url));
        queueMicrotask(() => {
          const event = new Event("message");
          Object.defineProperty(event, "data", {
            value: { type: "ready" },
            configurable: true,
          });
          this.dispatchEvent(event);
        });
      }

      postMessage(): void {}

      terminate(): void {}
    }

    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/app/",
    };
    (globalThis as Record<string, unknown>).Worker = FakeWorker;

    const db = await createWorkerDb({
      appId: "worker-bootstrap-base-url",
      driver: { type: "persistent", dbName: "worker-bootstrap-base-url" },
      runtimeSources: {
        baseUrl: "/assets/jazz/",
      },
    });

    await db.shutdown();

    expect(spawnedWorkerUrls).toEqual([
      "http://localhost:3000/assets/jazz/worker/jazz-worker.js?jazz-wasm-url=http%3A%2F%2Flocalhost%3A3000%2Fassets%2Fjazz%2Fjazz_wasm_bg.wasm",
    ]);
  });

  it("uses the static bundler-detected URL pattern when no runtimeSources are provided", async () => {
    const spawnedWorkerUrls: string[] = [];

    class FakeWorker extends EventTarget {
      constructor(url: string | URL, _options?: WorkerOptions) {
        super();
        spawnedWorkerUrls.push(String(url));
        queueMicrotask(() => {
          const event = new Event("message");
          Object.defineProperty(event, "data", {
            value: { type: "ready" },
            configurable: true,
          });
          this.dispatchEvent(event);
        });
      }

      postMessage(): void {}

      terminate(): void {}
    }

    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/",
    };
    (globalThis as Record<string, unknown>).Worker = FakeWorker;

    const db = await createWorkerDb({
      appId: "worker-bootstrap-browser-assets",
      driver: { type: "persistent", dbName: "worker-bootstrap-browser-assets" },
    });

    await db.shutdown();

    expect(loadWasmModuleMock).toHaveBeenCalledTimes(1);
    expect(spawnedWorkerUrls).toHaveLength(1);
    expect(spawnedWorkerUrls[0]).toMatch(/worker\/jazz-worker\.js$/);
  });

  it("does not open a BroadcastChannel in persistent browser broker mode", async () => {
    const openedChannels: string[] = [];

    class FakeBroadcastChannel extends EventTarget {
      name: string;

      constructor(name: string) {
        super();
        this.name = name;
        openedChannels.push(name);
      }

      postMessage(): void {}

      close(): void {}
    }

    class FakeWorker extends EventTarget {
      constructor(_url: string | URL, _options?: WorkerOptions) {
        super();
        queueMicrotask(() => {
          const event = new Event("message");
          Object.defineProperty(event, "data", {
            value: { type: "ready" },
            configurable: true,
          });
          this.dispatchEvent(event);
        });
      }

      postMessage(): void {}

      terminate(): void {}
    }

    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/",
    };
    (globalThis as Record<string, unknown>).Worker = FakeWorker;
    (globalThis as Record<string, unknown>).BroadcastChannel = FakeBroadcastChannel;

    const db = await createWorkerDb({
      appId: "worker-bootstrap-no-broadcast-channel",
      driver: { type: "persistent", dbName: "worker-bootstrap-no-broadcast-channel" },
    });

    await db.shutdown();

    expect(openedChannels).toEqual([]);
  });

  it("includes a fallbackWasmUrl in bridge options when no runtimeSources are provided", async () => {
    class FakeWorker extends EventTarget {
      constructor(_url: string | URL, _options?: WorkerOptions) {
        super();
        queueMicrotask(() => {
          const event = new Event("message");
          Object.defineProperty(event, "data", {
            value: { type: "ready" },
            configurable: true,
          });
          this.dispatchEvent(event);
        });
      }

      postMessage(): void {}

      terminate(): void {}
    }

    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/",
    };
    (globalThis as Record<string, unknown>).Worker = FakeWorker;

    const db = await createWorkerDb({
      appId: "worker-bootstrap-fallback-wasm",
      driver: { type: "persistent", dbName: "worker-bootstrap-fallback-wasm" },
    });

    const options = (db as any).buildWorkerBridgeOptions("{}");
    await db.shutdown();

    expect(options.fallbackWasmUrl).toMatch(/jazz_wasm_bg\.wasm$/);
  });

  it("passes telemetryCollectorUrl into worker bridge options", async () => {
    class FakeWorker extends EventTarget {
      constructor(_url: string | URL, _options?: WorkerOptions) {
        super();
        queueMicrotask(() => {
          const event = new Event("message");
          Object.defineProperty(event, "data", {
            value: { type: "ready" },
            configurable: true,
          });
          this.dispatchEvent(event);
        });
      }

      postMessage(): void {}

      terminate(): void {}
    }

    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/",
    };
    (globalThis as Record<string, unknown>).Worker = FakeWorker;

    const db = await createWorkerDb({
      appId: "worker-bootstrap-telemetry",
      driver: { type: "persistent", dbName: "worker-bootstrap-telemetry" },
      telemetryCollectorUrl: "http://127.0.0.1:54418",
    });

    const options = (db as any).buildWorkerBridgeOptions("{}");
    await db.shutdown();

    expect(options.telemetryCollectorUrl).toBe("http://127.0.0.1:54418");
  });

  it("does not append a bootstrap wasm URL when runtimeSources provides in-memory wasmSource", async () => {
    const spawnedWorkerUrls: string[] = [];

    class FakeWorker extends EventTarget {
      constructor(url: string | URL, _options?: WorkerOptions) {
        super();
        spawnedWorkerUrls.push(String(url));
        queueMicrotask(() => {
          const event = new Event("message");
          Object.defineProperty(event, "data", {
            value: { type: "ready" },
            configurable: true,
          });
          this.dispatchEvent(event);
        });
      }

      postMessage(): void {}

      terminate(): void {}
    }

    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/app/",
    };
    (globalThis as Record<string, unknown>).Worker = FakeWorker;

    const db = await createWorkerDb({
      appId: "worker-bootstrap-wasm-source",
      driver: { type: "persistent", dbName: "worker-bootstrap-wasm-source" },
      runtimeSources: {
        workerUrl: "/custom/jazz-worker.js",
        wasmSource: new Uint8Array([0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]),
      },
    });

    await db.shutdown();

    expect(spawnedWorkerUrls).toEqual(["http://localhost:3000/custom/jazz-worker.js"]);
  });

  it("releases broker leader resources when demoted during an in-flight promotion", async () => {
    const appId = "worker-bootstrap-demote-in-flight";
    const dbName = "worker-bootstrap-demote-in-flight";
    const releasedLocks: string[] = [];
    let workerReady: (() => void) | null = null;
    let terminatedWorkers = 0;

    tryAcquireWebLockMock.mockImplementation(async (lockName?: string) => ({
      release: vi.fn(() => {
        if (lockName) {
          releasedLocks.push(lockName);
        }
      }),
    }));

    class FakeWorker extends EventTarget {
      constructor(_url: string | URL, _options?: WorkerOptions) {
        super();
        workerReady = () => {
          const event = new Event("message");
          Object.defineProperty(event, "data", {
            value: { type: "ready" },
            configurable: true,
          });
          this.dispatchEvent(event);
        };
      }

      postMessage(): void {}

      terminate(): void {
        terminatedWorkers++;
      }
    }

    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/",
    };
    (globalThis as Record<string, unknown>).Worker = FakeWorker;

    const dbPromise = createWorkerDb({
      appId,
      driver: { type: "persistent", dbName },
    });

    let db: Db | null = null;
    try {
      await waitFor(
        () => tryAcquireWebLockMock.mock.calls.length >= 1 && workerReady !== null,
        200,
        "promotion should acquire the tab lock and start worker bootstrap",
      );

      await FakeBrowserBrokerClient.instances[0]!.demote(1);
      workerReady!();
      db = await dbPromise;

      expect(tryAcquireWebLockMock.mock.calls.map(([lockName]) => lockName)).toEqual([
        `jazz-leader-tab:${appId}:${dbName}`,
      ]);
      expect(releasedLocks).toEqual([`jazz-leader-tab:${appId}:${dbName}`]);
      expect(terminatedWorkers).toBe(1);
      expect((db as unknown as { tabRole?: unknown }).tabRole).toBe("follower");
    } finally {
      await db?.shutdown();
    }
  });

  it("waits for an in-flight promotion to settle before acking broker storage reset", async () => {
    const appId = "worker-bootstrap-reset-cancels-promotion";
    const dbName = "worker-bootstrap-reset-cancels-promotion";
    const releasedLocks: string[] = [];
    let workerReady: (() => void) | null = null;
    let resetPrepared = false;
    let terminatedWorkers = 0;

    tryAcquireWebLockMock.mockImplementation(async (lockName?: string) => ({
      release: vi.fn(() => {
        if (lockName) {
          releasedLocks.push(lockName);
        }
      }),
    }));

    class FakeWorker extends EventTarget {
      constructor(_url: string | URL, _options?: WorkerOptions) {
        super();
        workerReady = () => {
          const event = new Event("message");
          Object.defineProperty(event, "data", {
            value: { type: "ready" },
            configurable: true,
          });
          this.dispatchEvent(event);
        };
      }

      postMessage(): void {}

      terminate(): void {
        terminatedWorkers++;
      }
    }

    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/",
    };
    (globalThis as Record<string, unknown>).Worker = FakeWorker;

    const dbPromise = createWorkerDb({
      appId,
      driver: { type: "persistent", dbName },
    });

    let db: Db | null = null;
    const shutdownForReset = vi.spyOn(
      Db.prototype as unknown as {
        shutdownWorkerAndClientsForStorageReset: () => Promise<void>;
      },
      "shutdownWorkerAndClientsForStorageReset",
    );
    try {
      await waitFor(
        () => tryAcquireWebLockMock.mock.calls.length >= 1 && workerReady !== null,
        200,
        "promotion should acquire the tab lock and start worker bootstrap",
      );

      const resetPreparation = FakeBrowserBrokerClient.instances[0]!.beginStorageReset(1).then(
        () => {
          resetPrepared = true;
        },
      );
      await new Promise((resolve) => setTimeout(resolve, 0));

      expect(resetPrepared).toBe(false);

      workerReady!();
      db = await dbPromise;
      await resetPreparation;

      expect(resetPrepared).toBe(true);
      expect(releasedLocks).toEqual([`jazz-leader-tab:${appId}:${dbName}`]);
      expect(terminatedWorkers).toBe(1);
      expect(shutdownForReset).toHaveBeenCalledTimes(1);
    } finally {
      shutdownForReset.mockRestore();
      await db?.shutdown();
    }
  });

  it("adopts the broker snapshot when the broker reconnects", async () => {
    class FakeWorker extends EventTarget {
      constructor(_url: string | URL, _options?: WorkerOptions) {
        super();
        queueMicrotask(() => {
          const event = new Event("message");
          Object.defineProperty(event, "data", {
            value: { type: "ready" },
            configurable: true,
          });
          this.dispatchEvent(event);
        });
      }

      postMessage(): void {}

      terminate(): void {}
    }

    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/",
    };
    (globalThis as Record<string, unknown>).Worker = FakeWorker;

    const db = await createWorkerDb({
      appId: "worker-bootstrap-reconnect-snapshot",
      driver: { type: "persistent", dbName: "worker-bootstrap-reconnect-snapshot" },
    });

    try {
      const anyDb = db as unknown as {
        brokerSchemaFingerprint: string | null;
        currentLeaderTabId: string | null;
        currentLeadershipId: number;
        handleBrokerReconnected(client: unknown): void;
        tabId: string;
        tabRole: string;
      };
      const reportSchemaReady = vi.fn();

      anyDb.tabRole = "follower";
      anyDb.currentLeaderTabId = "old-leader";
      anyDb.currentLeadershipId = 42;
      anyDb.brokerSchemaFingerprint = "schema-a";

      anyDb.handleBrokerReconnected({
        snapshot: () => ({
          brokerInstanceId: "new-broker",
          role: "follower",
          tabId: anyDb.tabId,
          leaderTabId: null,
          leadershipId: 0,
        }),
        reportSchemaReady,
      });

      expect(anyDb.currentLeadershipId).toBe(0);
      expect(anyDb.currentLeaderTabId).toBeNull();
      expect(reportSchemaReady).toHaveBeenCalledWith("schema-a");
    } finally {
      await db.shutdown();
    }
  });

  it("shuts down broker leadership resources when the tab lock is lost", async () => {
    const appId = "worker-bootstrap-tab-lock-lost";
    const dbName = "worker-bootstrap-tab-lock-lost";
    const releasedLocks = new Set<string>();
    const lostCallbacks = new Map<string, (reason: unknown) => void>();
    let terminatedWorkers = 0;

    tryAcquireWebLockMock.mockImplementation(async (lockName?: string, options?: any) => {
      if (lockName && typeof options?.onLost === "function") {
        lostCallbacks.set(lockName, options.onLost);
      }
      return {
        release: vi.fn(() => {
          if (lockName) {
            releasedLocks.add(lockName);
          }
        }),
      };
    });

    class FakeWorker extends EventTarget {
      constructor(_url: string | URL, _options?: WorkerOptions) {
        super();
        queueMicrotask(() => {
          const event = new Event("message");
          Object.defineProperty(event, "data", {
            value: { type: "ready" },
            configurable: true,
          });
          this.dispatchEvent(event);
        });
      }

      postMessage(): void {}

      terminate(): void {
        terminatedWorkers++;
      }
    }

    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/",
    };
    (globalThis as Record<string, unknown>).Worker = FakeWorker;

    const db = await createWorkerDb({
      appId,
      driver: { type: "persistent", dbName },
    });

    try {
      const tabLockName = `jazz-leader-tab:${appId}:${dbName}`;
      expect(lostCallbacks.has(tabLockName)).toBe(true);

      lostCallbacks.get(tabLockName)!(new Error("tab lock stolen"));

      await waitFor(() => terminatedWorkers === 1, 200, "leader worker should terminate");
      expect(releasedLocks).toEqual(new Set([`jazz-leader-tab:${appId}:${dbName}`]));
      expect(FakeBrowserBrokerClient.leaderFailures).toEqual([
        { leadershipId: 1, reason: "tab lock stolen" },
      ]);
      expect((db as unknown as { tabRole?: unknown }).tabRole).toBe("follower");
    } finally {
      await db.shutdown();
    }
  });
});
