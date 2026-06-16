import { afterEach, describe, expect, it, vi } from "vitest";
import {
  INCOMPATIBLE_BROWSER_BROKER_CONFIGURATION_CODE,
  IncompatibleBrowserBrokerConfigurationError,
} from "./browser-broker-errors.js";
import { BrowserBrokerClient } from "./browser-broker-client.js";
import type { BrowserBrokerControlMessage } from "./browser-broker-protocol.js";

function dispatchPortMessage(port: EventTarget, data: unknown): void {
  const event = new Event("message");
  Object.defineProperty(event, "data", {
    value: data,
    configurable: true,
  });
  port.dispatchEvent(event);
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

interface FakeWorkerEnvOptions {
  /** broker-hello instance id per created worker; index N -> Nth worker. */
  brokerInstanceIds?: string[];
  /** When false, ports never answer hello (for timeout/error-path tests). */
  respondToHello?: boolean;
}

function createFakeWorkerEnv(options: FakeWorkerEnvOptions = {}) {
  const { brokerInstanceIds = ["instance-a"], respondToHello = true } = options;
  const workerUrls: string[] = [];

  class FakeBrokerPort extends EventTarget {
    readonly postedMessages: unknown[] = [];
    closed = false;

    constructor(private readonly brokerInstanceId: string) {
      super();
    }

    postMessage(message: unknown): void {
      this.postedMessages.push(message);
      if (respondToHello && (message as { type?: unknown }).type === "hello") {
        queueMicrotask(() => {
          dispatchPortMessage(this, {
            type: "broker-hello",
            brokerInstanceId: this.brokerInstanceId,
          });
        });
      }
    }

    start(): void {}

    close(): void {
      this.closed = true;
    }
  }

  class FakeSharedWorker extends EventTarget {
    readonly port: MessagePort & FakeBrokerPort;

    constructor(url: string | URL, _options?: string | { name?: string; type?: WorkerType }) {
      super();
      this.port = new FakeBrokerPort(
        brokerInstanceIds[workers.length] ?? "instance-next",
      ) as MessagePort & FakeBrokerPort;
      workerUrls.push(String(url));
      workers.push(this);
    }
  }

  const workers: FakeSharedWorker[] = [];

  return { workers, workerUrls, FakeSharedWorker };
}

describe("BrowserBrokerClient", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("reconnects and demotes stale local state when the broker instance changes", async () => {
    const brokerInstanceIds = ["instance-a", "instance-b"];
    const workers: FakeSharedWorker[] = [];
    const demotedLeadershipIds: number[] = [];

    class FakePort extends EventTarget {
      readonly postedMessages: unknown[] = [];
      closed = false;

      postMessage(message: unknown): void {
        this.postedMessages.push(message);
        if ((message as { type?: unknown }).type === "hello") {
          const brokerInstanceId = brokerInstanceIds[workers.length - 1];
          queueMicrotask(() => {
            dispatchPortMessage(this, { type: "broker-hello", brokerInstanceId });
          });
        }
      }

      start(): void {}

      close(): void {
        this.closed = true;
      }
    }

    class FakeSharedWorker {
      readonly port = new FakePort() as MessagePort & FakePort;

      constructor(_url: string | URL, _options?: string | { name?: string; type?: WorkerType }) {
        workers.push(this);
      }
    }

    const client = await BrowserBrokerClient.connect({
      appId: "app",
      dbName: "db",
      tabId: "tab-a",
      fingerprint: "fingerprint",
      visibility: "visible",
      globalLike: {
        SharedWorker: FakeSharedWorker,
        MessageChannel,
        navigator: {
          locks: { request() {} },
        },
      },
      onDemote: (leadershipId) => {
        demotedLeadershipIds.push(leadershipId);
      },
    });

    dispatchPortMessage(workers[0].port, {
      type: "leader-ready",
      brokerInstanceId: "instance-a",
      leaderTabId: "tab-a",
      leadershipId: 1,
    } satisfies BrowserBrokerControlMessage);
    await client.waitForRole("leader", 100);

    dispatchPortMessage(workers[0].port, {
      type: "broker-ping",
      brokerInstanceId: "instance-b",
    } satisfies BrowserBrokerControlMessage);

    await waitFor(
      () => workers.length === 2 && demotedLeadershipIds.includes(1),
      200,
      "client should reconnect after broker instance change",
    );

    expect(workers[0].port.closed).toBe(true);
    expect(client.snapshot()).toMatchObject({
      brokerInstanceId: "instance-b",
      role: "follower",
      leaderTabId: null,
      leadershipId: 0,
    });

    dispatchPortMessage(workers[1].port, {
      type: "leader-ready",
      brokerInstanceId: "instance-b",
      leaderTabId: "tab-a",
      leadershipId: 2,
    } satisfies BrowserBrokerControlMessage);

    await client.waitForRole("leader", 100);
    expect(client.snapshot()).toMatchObject({
      brokerInstanceId: "instance-b",
      role: "leader",
      leaderTabId: "tab-a",
      leadershipId: 2,
    });

    await client.shutdown();
  });

  it("stamps tab messages with the active broker instance", async () => {
    const env = createFakeWorkerEnv();

    const client = await BrowserBrokerClient.connect({
      appId: "app",
      dbName: "db",
      tabId: "tab-a",
      fingerprint: "fingerprint",
      visibility: "visible",
      globalLike: {
        SharedWorker: env.FakeSharedWorker,
        MessageChannel,
        navigator: {
          locks: { request() {} },
        },
      },
    });

    client.reportVisibility("hidden");
    client.reportSchemaReady("schema-a");
    client.reportLeaderFailed(1, "leader crashed");

    expect(env.workers[0]!.port.postedMessages).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          type: "visibility",
          brokerInstanceId: "instance-a",
          visibility: "hidden",
        }),
        expect.objectContaining({
          type: "schema-ready",
          brokerInstanceId: "instance-a",
          schemaFingerprint: "schema-a",
        }),
        expect.objectContaining({
          type: "leader-failed",
          brokerInstanceId: "instance-a",
          leadershipId: 1,
          reason: "leader crashed",
        }),
      ]),
    );

    await client.shutdown();
  });

  it("does not replay reconnect-time messages into a fresh broker instance", async () => {
    const env = createFakeWorkerEnv({ brokerInstanceIds: ["instance-a", "instance-b"] });
    let client: BrowserBrokerClient;

    client = await BrowserBrokerClient.connect({
      appId: "app",
      dbName: "db",
      tabId: "tab-a",
      fingerprint: "fingerprint",
      visibility: "visible",
      globalLike: {
        SharedWorker: env.FakeSharedWorker,
        MessageChannel,
        navigator: {
          locks: { request() {} },
        },
      },
      onDemote: (leadershipId) => {
        client.reportLeaderFailed(leadershipId, "late old-instance failure");
        client.reportVisibility("hidden");
      },
    });

    dispatchPortMessage(env.workers[0]!.port, {
      type: "leader-ready",
      brokerInstanceId: "instance-a",
      leaderTabId: "tab-a",
      leadershipId: 1,
    } satisfies BrowserBrokerControlMessage);
    await client.waitForRole("leader", 100);

    dispatchPortMessage(env.workers[0]!.port, {
      type: "broker-ping",
      brokerInstanceId: "instance-b",
    } satisfies BrowserBrokerControlMessage);

    await waitFor(
      () => client.snapshot().brokerInstanceId === "instance-b",
      300,
      "client should reconnect to the new broker",
    );
    await new Promise((resolve) => setTimeout(resolve, 150));

    expect(env.workers[1]!.port.postedMessages).not.toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          type: "leader-failed",
          reason: "late old-instance failure",
        }),
      ]),
    );
    expect(env.workers[1]!.port.postedMessages).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          type: "visibility",
          brokerInstanceId: "instance-b",
          visibility: "hidden",
        }),
      ]),
    );

    await client.shutdown();
  });

  it("replays the latest visibility when reconnecting to a fresh broker instance", async () => {
    const env = createFakeWorkerEnv({ brokerInstanceIds: ["instance-a", "instance-b"] });

    const client = await BrowserBrokerClient.connect({
      appId: "app",
      dbName: "db",
      tabId: "tab-a",
      fingerprint: "fingerprint",
      visibility: "visible",
      globalLike: {
        SharedWorker: env.FakeSharedWorker,
        MessageChannel,
        navigator: {
          locks: { request() {} },
        },
      },
    });

    client.reportVisibility("hidden");

    dispatchPortMessage(env.workers[0]!.port, {
      type: "broker-ping",
      brokerInstanceId: "instance-b",
    } satisfies BrowserBrokerControlMessage);

    await waitFor(
      () => client.snapshot().brokerInstanceId === "instance-b",
      300,
      "client should reconnect to the new broker",
    );

    expect(env.workers[1]!.port.postedMessages[0]).toMatchObject({
      type: "hello",
      visibility: "hidden",
    });

    await client.shutdown();
  });

  it("uses an explicit broker worker URL override", async () => {
    const env = createFakeWorkerEnv();

    const client = await BrowserBrokerClient.connect({
      appId: "app",
      dbName: "db",
      tabId: "tab-a",
      fingerprint: "fingerprint",
      visibility: "visible",
      runtimeSources: {
        brokerWorkerUrl: "https://cdn.example/jazz/worker/jazz-broker-worker.js",
      },
      globalLike: {
        SharedWorker: env.FakeSharedWorker,
        MessageChannel,
        navigator: {
          locks: { request() {} },
        },
      },
    });

    expect(env.workerUrls).toEqual(["https://cdn.example/jazz/worker/jazz-broker-worker.js"]);

    await client.shutdown();
  });

  it("rejects connect with a typed error when the broker rejects hello compatibility", async () => {
    const env = createFakeWorkerEnv({ respondToHello: false });

    const connecting = BrowserBrokerClient.connect({
      appId: "app",
      dbName: "db",
      tabId: "tab-a",
      fingerprint: "fingerprint",
      visibility: "visible",
      globalLike: {
        SharedWorker: env.FakeSharedWorker,
        MessageChannel,
        navigator: {
          locks: { request() {} },
        },
      },
    } as Parameters<typeof BrowserBrokerClient.connect>[0]);

    dispatchPortMessage(env.workers[0]!.port, {
      type: "unsupported",
      brokerInstanceId: "instance-a",
      code: INCOMPATIBLE_BROWSER_BROKER_CONFIGURATION_CODE,
      reason: "incompatible persistent browser configuration",
    } satisfies BrowserBrokerControlMessage);

    await expect(connecting).rejects.toBeInstanceOf(IncompatibleBrowserBrokerConfigurationError);
    expect(env.workers[0]!.port.closed).toBe(true);
  });

  it("rejects connect when the SharedWorker fires an error event", async () => {
    const env = createFakeWorkerEnv({ respondToHello: false });

    const connecting = BrowserBrokerClient.connect({
      appId: "app",
      dbName: "db",
      tabId: "tab-a",
      fingerprint: "fingerprint",
      visibility: "visible",
      globalLike: {
        SharedWorker: env.FakeSharedWorker,
        MessageChannel,
        navigator: {
          locks: { request() {} },
        },
      },
    });
    // Swallow the rejection until the explicit await below so vitest does not
    // flag an unhandled rejection in the interim.
    connecting.catch(() => {});

    await waitFor(() => env.workers.length === 1, 100, "worker should be created");
    const errorEvent = new Event("error");
    Object.defineProperty(errorEvent, "message", { value: "script URL mismatch" });
    env.workers[0]!.dispatchEvent(errorEvent);

    await expect(connecting).rejects.toThrow(
      "Browser broker SharedWorker failed to start: script URL mismatch",
    );
  }, 10_000);

  it("cleans up the port and handlers when broker hello times out", async () => {
    vi.useFakeTimers();
    const workers: FakeSharedWorker[] = [];
    const onBecomeLeader = vi.fn();

    class FakePort extends EventTarget {
      readonly postedMessages: unknown[] = [];
      closed = false;

      postMessage(message: unknown): void {
        this.postedMessages.push(message);
      }

      start(): void {}

      close(): void {
        this.closed = true;
      }
    }

    class FakeSharedWorker {
      readonly port = new FakePort() as MessagePort & FakePort;

      constructor(_url: string | URL, _options?: string | { name?: string; type?: WorkerType }) {
        workers.push(this);
      }
    }

    const connecting = BrowserBrokerClient.connect({
      appId: "app",
      dbName: "db",
      tabId: "tab-a",
      fingerprint: "fingerprint",
      visibility: "visible",
      globalLike: {
        SharedWorker: FakeSharedWorker,
        MessageChannel,
        navigator: {
          locks: { request() {} },
        },
      },
      onBecomeLeader,
    });
    connecting.catch(() => undefined);

    await vi.advanceTimersByTimeAsync(5_000);
    await expect(connecting).rejects.toThrow("Timed out waiting for browser broker hello");

    expect(workers[0]?.port.closed).toBe(true);

    dispatchPortMessage(workers[0]!.port, {
      type: "become-leader",
      brokerInstanceId: "late-instance",
      leadershipId: 1,
    } satisfies BrowserBrokerControlMessage);

    expect(onBecomeLeader).not.toHaveBeenCalled();
  });

  it("reconnects when broker pings stop", async () => {
    const brokerInstanceIds = ["instance-a", "instance-b"];
    const workers: FakeSharedWorker[] = [];
    const demotedLeadershipIds: number[] = [];

    class FakePort extends EventTarget {
      readonly postedMessages: unknown[] = [];
      closed = false;

      constructor(private readonly brokerInstanceId: string) {
        super();
      }

      postMessage(message: unknown): void {
        this.postedMessages.push(message);
        if ((message as { type?: unknown }).type === "hello") {
          queueMicrotask(() => {
            dispatchPortMessage(this, {
              type: "broker-hello",
              brokerInstanceId: this.brokerInstanceId,
            });
          });
        }
      }

      start(): void {}

      close(): void {
        this.closed = true;
      }
    }

    class FakeSharedWorker {
      readonly port: MessagePort & FakePort;

      constructor(_url: string | URL, _options?: string | { name?: string; type?: WorkerType }) {
        this.port = new FakePort(
          brokerInstanceIds[workers.length] ?? "instance-next",
        ) as MessagePort & FakePort;
        workers.push(this);
      }
    }

    const client = await BrowserBrokerClient.connect({
      appId: "app",
      dbName: "db",
      tabId: "tab-a",
      fingerprint: "fingerprint",
      visibility: "visible",
      brokerPingIntervalMs: 10,
      brokerPongTimeoutMs: 20,
      globalLike: {
        SharedWorker: FakeSharedWorker,
        MessageChannel,
        navigator: {
          locks: { request() {} },
        },
      },
      onDemote: (leadershipId) => {
        demotedLeadershipIds.push(leadershipId);
      },
    });

    dispatchPortMessage(workers[0]!.port, {
      type: "leader-ready",
      brokerInstanceId: "instance-a",
      leaderTabId: "tab-a",
      leadershipId: 1,
    } satisfies BrowserBrokerControlMessage);
    await client.waitForRole("leader", 100);

    await waitFor(
      () => workers.length >= 2 && demotedLeadershipIds.includes(1),
      500,
      "client should reconnect after broker pings stop",
    );

    expect(workers[0]!.port.closed).toBe(true);
    expect(client.snapshot()).toMatchObject({
      brokerInstanceId: "instance-b",
      role: "follower",
      leaderTabId: null,
      leadershipId: 0,
    });

    await client.shutdown();
  });

  it("closes with the reconnect failure as the error cause", async () => {
    const env = createFakeWorkerEnv({ brokerInstanceIds: ["instance-a"] });
    const constructionError = new Error("second construction failed");
    let constructed = 0;
    let closedError: Error | null = null;

    class FlakySharedWorker extends env.FakeSharedWorker {
      constructor(url: string | URL, options?: string | { name?: string; type?: WorkerType }) {
        constructed += 1;
        if (constructed > 1) {
          throw constructionError;
        }
        super(url, options);
      }
    }

    const client = await BrowserBrokerClient.connect({
      appId: "app",
      dbName: "db",
      tabId: "tab-a",
      fingerprint: "fingerprint",
      visibility: "visible",
      globalLike: {
        SharedWorker: FlakySharedWorker,
        MessageChannel,
        navigator: {
          locks: { request() {} },
        },
      },
      onClosed: (error) => {
        closedError = error;
      },
    });

    dispatchPortMessage(env.workers[0]!.port, {
      type: "broker-ping",
      brokerInstanceId: "instance-b",
    } satisfies BrowserBrokerControlMessage);

    await waitFor(() => closedError !== null, 300, "client should close on reconnect failure");
    expect(closedError!.message).toContain("second construction failed");
    expect(closedError!.cause).toBe(constructionError);
    void client;
  });

  it("notifies the owner when the broker rejects an established tab", async () => {
    const workers: FakeSharedWorker[] = [];
    const onClosed = vi.fn();

    class FakePort extends EventTarget {
      readonly postedMessages: unknown[] = [];
      closed = false;

      postMessage(message: unknown): void {
        this.postedMessages.push(message);
        if ((message as { type?: unknown }).type === "hello") {
          queueMicrotask(() => {
            dispatchPortMessage(this, {
              type: "broker-hello",
              brokerInstanceId: "instance-a",
            });
          });
        }
      }

      start(): void {}

      close(): void {
        this.closed = true;
      }
    }

    class FakeSharedWorker {
      readonly port = new FakePort() as MessagePort & FakePort;

      constructor(_url: string | URL, _options?: string | { name?: string; type?: WorkerType }) {
        workers.push(this);
      }
    }

    const client = await BrowserBrokerClient.connect({
      appId: "app",
      dbName: "db",
      tabId: "tab-a",
      fingerprint: "fingerprint",
      visibility: "visible",
      globalLike: {
        SharedWorker: FakeSharedWorker,
        MessageChannel,
        navigator: {
          locks: { request() {} },
        },
      },
      onClosed,
    } as Parameters<typeof BrowserBrokerClient.connect>[0]);

    dispatchPortMessage(workers[0]!.port, {
      type: "unsupported",
      brokerInstanceId: "instance-a",
      code: INCOMPATIBLE_BROWSER_BROKER_CONFIGURATION_CODE,
      reason: "incompatible persistent browser configuration",
    } satisfies BrowserBrokerControlMessage);

    expect(onClosed).toHaveBeenCalledTimes(1);
    expect(onClosed.mock.calls[0]![0]).toBeInstanceOf(IncompatibleBrowserBrokerConfigurationError);
    expect(onClosed.mock.calls[0]![0].message).toBe(
      "incompatible persistent browser configuration",
    );
    expect(workers[0]!.port.closed).toBe(true);

    await expect(client.waitForRole("leader", 10)).rejects.toThrow(
      "incompatible persistent browser configuration",
    );
  });

  it("forwards future demote messages so in-flight promotions can be cancelled", async () => {
    const workers: FakeSharedWorker[] = [];
    const demotedLeadershipIds: number[] = [];

    class FakePort extends EventTarget {
      readonly postedMessages: unknown[] = [];
      closed = false;

      postMessage(message: unknown): void {
        this.postedMessages.push(message);
        if ((message as { type?: unknown }).type === "hello") {
          queueMicrotask(() => {
            dispatchPortMessage(this, {
              type: "broker-hello",
              brokerInstanceId: "instance-a",
            });
          });
        }
      }

      start(): void {}

      close(): void {
        this.closed = true;
      }
    }

    class FakeSharedWorker {
      readonly port = new FakePort() as MessagePort & FakePort;

      constructor(_url: string | URL, _options?: string | { name?: string; type?: WorkerType }) {
        workers.push(this);
      }
    }

    const client = await BrowserBrokerClient.connect({
      appId: "app",
      dbName: "db",
      tabId: "tab-a",
      fingerprint: "fingerprint",
      visibility: "visible",
      globalLike: {
        SharedWorker: FakeSharedWorker,
        MessageChannel,
        navigator: {
          locks: { request() {} },
        },
      },
      onDemote: (leadershipId) => {
        demotedLeadershipIds.push(leadershipId);
      },
    });

    dispatchPortMessage(workers[0]!.port, {
      type: "leader-ready",
      brokerInstanceId: "instance-a",
      leaderTabId: "tab-a",
      leadershipId: 1,
    } satisfies BrowserBrokerControlMessage);
    await client.waitForRole("leader", 100);

    dispatchPortMessage(workers[0]!.port, {
      type: "demote",
      brokerInstanceId: "instance-a",
      leadershipId: 2,
    } satisfies BrowserBrokerControlMessage);

    expect(demotedLeadershipIds).toEqual([2]);

    await client.shutdown();
  });

  it("times out only the storage reset start acknowledgment", async () => {
    const workers: FakeSharedWorker[] = [];
    let resetOutcome: "pending" | "resolved" | string = "pending";

    class FakePort extends EventTarget {
      readonly postedMessages: unknown[] = [];
      closed = false;

      postMessage(message: unknown): void {
        this.postedMessages.push(message);
        if ((message as { type?: unknown }).type === "hello") {
          queueMicrotask(() => {
            dispatchPortMessage(this, {
              type: "broker-hello",
              brokerInstanceId: "instance-a",
            });
          });
        }
      }

      start(): void {}

      close(): void {
        this.closed = true;
      }
    }

    class FakeSharedWorker {
      readonly port = new FakePort() as MessagePort & FakePort;

      constructor(_url: string | URL, _options?: string | { name?: string; type?: WorkerType }) {
        workers.push(this);
      }
    }

    const client = await BrowserBrokerClient.connect({
      appId: "app",
      dbName: "db",
      tabId: "tab-a",
      fingerprint: "fingerprint",
      visibility: "visible",
      storageResetTimeoutMs: 25,
      globalLike: {
        SharedWorker: FakeSharedWorker,
        MessageChannel,
        navigator: {
          locks: { request() {} },
        },
      },
    });

    const reset = client.requestStorageReset("reset-a").then(
      () => {
        resetOutcome = "resolved";
      },
      (error) => {
        resetOutcome = error instanceof Error ? error.message : String(error);
      },
    );

    dispatchPortMessage(workers[0]!.port, {
      type: "storage-reset-begin",
      brokerInstanceId: "instance-a",
      requestId: "reset-a",
      leadershipId: 1,
    } satisfies BrowserBrokerControlMessage);

    await new Promise((resolve) => setTimeout(resolve, 75));
    expect(resetOutcome).toBe("pending");

    dispatchPortMessage(workers[0]!.port, {
      type: "storage-reset-finished",
      brokerInstanceId: "instance-a",
      requestId: "reset-a",
      success: true,
    } satisfies BrowserBrokerControlMessage);

    await reset;
    expect(resetOutcome).toBe("resolved");

    await client.shutdown();
  });

  it("delays storage resets issued during a reconnect until the new broker attaches", async () => {
    const env = createFakeWorkerEnv({ brokerInstanceIds: ["instance-a", "instance-b"] });
    let client: BrowserBrokerClient;
    let resetResult: Promise<void> | null = null;

    client = await BrowserBrokerClient.connect({
      appId: "app",
      dbName: "db",
      tabId: "tab-a",
      fingerprint: "fingerprint",
      visibility: "visible",
      globalLike: {
        SharedWorker: env.FakeSharedWorker,
        MessageChannel,
        navigator: {
          locks: { request() {} },
        },
      },
      onDemote: () => {
        // Fires mid-reconnect, while sends are suppressed.
        resetResult = client.requestStorageReset("reset-1");
        resetResult.catch(() => {});
      },
    });

    dispatchPortMessage(env.workers[0]!.port, {
      type: "leader-ready",
      brokerInstanceId: "instance-a",
      leaderTabId: "tab-a",
      leadershipId: 1,
    } satisfies BrowserBrokerControlMessage);
    await client.waitForRole("leader", 100);

    dispatchPortMessage(env.workers[0]!.port, {
      type: "broker-ping",
      brokerInstanceId: "instance-b",
    } satisfies BrowserBrokerControlMessage);

    await waitFor(() => resetResult !== null, 300, "reset should be requested during demote");
    await waitFor(
      () =>
        env.workers[1]?.port.postedMessages.some(
          (m) => (m as { type?: string }).type === "storage-reset-request",
        ) ?? false,
      300,
      "reset request should reach the new broker",
    );

    expect(env.workers[1]!.port.postedMessages).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          type: "storage-reset-request",
          requestId: "reset-1",
          brokerInstanceId: "instance-b",
        }),
      ]),
    );

    dispatchPortMessage(env.workers[1]!.port, {
      type: "storage-reset-started",
      brokerInstanceId: "instance-b",
      requestId: "reset-1",
    } satisfies BrowserBrokerControlMessage);
    dispatchPortMessage(env.workers[1]!.port, {
      type: "storage-reset-finished",
      brokerInstanceId: "instance-b",
      requestId: "reset-1",
      success: true,
    } satisfies BrowserBrokerControlMessage);

    await resetResult!;
    await client.shutdown();
  });

  it("rejects storage reset completion waiters when the broker reconnects mid-reset", async () => {
    const brokerInstanceIds = ["instance-a", "instance-b"];
    const workers: FakeSharedWorker[] = [];
    let resetOutcome = "pending";

    class FakePort extends EventTarget {
      readonly postedMessages: unknown[] = [];
      closed = false;

      constructor(private readonly brokerInstanceId: string) {
        super();
      }

      postMessage(message: unknown): void {
        this.postedMessages.push(message);
        if ((message as { type?: unknown }).type === "hello") {
          queueMicrotask(() => {
            dispatchPortMessage(this, {
              type: "broker-hello",
              brokerInstanceId: this.brokerInstanceId,
            });
          });
        }
      }

      start(): void {}

      close(): void {
        this.closed = true;
      }
    }

    class FakeSharedWorker {
      readonly port: MessagePort & FakePort;

      constructor(_url: string | URL, _options?: string | { name?: string; type?: WorkerType }) {
        this.port = new FakePort(
          brokerInstanceIds[workers.length] ?? "instance-next",
        ) as MessagePort & FakePort;
        workers.push(this);
      }
    }

    const client = await BrowserBrokerClient.connect({
      appId: "app",
      dbName: "db",
      tabId: "tab-a",
      fingerprint: "fingerprint",
      visibility: "visible",
      globalLike: {
        SharedWorker: FakeSharedWorker,
        MessageChannel,
        navigator: {
          locks: { request() {} },
        },
      },
    });

    const reset = client.requestStorageReset("reset-a").then(
      () => {
        resetOutcome = "resolved";
      },
      (error) => {
        resetOutcome = error instanceof Error ? error.message : String(error);
      },
    );

    dispatchPortMessage(workers[0]!.port, {
      type: "storage-reset-started",
      brokerInstanceId: "instance-a",
      requestId: "reset-a",
    } satisfies BrowserBrokerControlMessage);

    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(resetOutcome).toBe("pending");

    dispatchPortMessage(workers[0]!.port, {
      type: "broker-ping",
      brokerInstanceId: "instance-b",
    } satisfies BrowserBrokerControlMessage);

    await waitFor(() => workers.length === 2, 200, "client should reconnect to the new broker");
    await waitFor(
      () => resetOutcome !== "pending",
      200,
      "storage reset waiter should reject when the broker reconnects",
    );

    expect(resetOutcome).toBe("Browser broker restarted during storage reset");

    await reset;
    await client.shutdown();
  });
});
