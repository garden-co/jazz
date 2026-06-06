import { afterEach, describe, expect, it, vi } from "vitest";
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

describe("BrowserBrokerClient", () => {
  afterEach(() => {
    vi.useRealTimers();
  });

  it("reconnects and demotes stale local state when the broker epoch changes", async () => {
    const brokerEpochs = ["epoch-a", "epoch-b"];
    const workers: FakeSharedWorker[] = [];
    const demotedTerms: number[] = [];

    class FakePort extends EventTarget {
      readonly postedMessages: unknown[] = [];
      closed = false;

      postMessage(message: unknown): void {
        this.postedMessages.push(message);
        if ((message as { type?: unknown }).type === "hello") {
          const brokerEpoch = brokerEpochs[workers.length - 1];
          queueMicrotask(() => {
            dispatchPortMessage(this, { type: "broker-hello", brokerEpoch });
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
      onDemote: (term) => {
        demotedTerms.push(term);
      },
    });

    dispatchPortMessage(workers[0].port, {
      type: "leader-ready",
      brokerEpoch: "epoch-a",
      leaderTabId: "tab-a",
      term: 1,
    } satisfies BrowserBrokerControlMessage);
    await client.waitForRole("leader", 100);

    dispatchPortMessage(workers[0].port, {
      type: "broker-ping",
      brokerEpoch: "epoch-b",
    } satisfies BrowserBrokerControlMessage);

    await waitFor(
      () => workers.length === 2 && demotedTerms.includes(1),
      200,
      "client should reconnect after broker epoch change",
    );

    expect(workers[0].port.closed).toBe(true);
    expect(client.snapshot()).toMatchObject({
      brokerEpoch: "epoch-b",
      role: "follower",
      leaderTabId: null,
      term: 0,
    });

    dispatchPortMessage(workers[1].port, {
      type: "leader-ready",
      brokerEpoch: "epoch-b",
      leaderTabId: "tab-a",
      term: 2,
    } satisfies BrowserBrokerControlMessage);

    await client.waitForRole("leader", 100);
    expect(client.snapshot()).toMatchObject({
      brokerEpoch: "epoch-b",
      role: "leader",
      leaderTabId: "tab-a",
      term: 2,
    });

    await client.shutdown();
  });

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
      brokerEpoch: "late-epoch",
      term: 1,
    } satisfies BrowserBrokerControlMessage);

    expect(onBecomeLeader).not.toHaveBeenCalled();
  });

  it("reconnects when broker pings stop", async () => {
    const brokerEpochs = ["epoch-a", "epoch-b"];
    const workers: FakeSharedWorker[] = [];
    const demotedTerms: number[] = [];

    class FakePort extends EventTarget {
      readonly postedMessages: unknown[] = [];
      closed = false;

      constructor(private readonly brokerEpoch: string) {
        super();
      }

      postMessage(message: unknown): void {
        this.postedMessages.push(message);
        if ((message as { type?: unknown }).type === "hello") {
          queueMicrotask(() => {
            dispatchPortMessage(this, { type: "broker-hello", brokerEpoch: this.brokerEpoch });
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
        this.port = new FakePort(brokerEpochs[workers.length] ?? "epoch-next") as MessagePort &
          FakePort;
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
      onDemote: (term) => {
        demotedTerms.push(term);
      },
    });

    dispatchPortMessage(workers[0]!.port, {
      type: "leader-ready",
      brokerEpoch: "epoch-a",
      leaderTabId: "tab-a",
      term: 1,
    } satisfies BrowserBrokerControlMessage);
    await client.waitForRole("leader", 100);

    await waitFor(
      () => workers.length >= 2 && demotedTerms.includes(1),
      500,
      "client should reconnect after broker pings stop",
    );

    expect(workers[0]!.port.closed).toBe(true);
    expect(client.snapshot()).toMatchObject({
      brokerEpoch: "epoch-b",
      role: "follower",
      leaderTabId: null,
      term: 0,
    });

    await client.shutdown();
  });
});
