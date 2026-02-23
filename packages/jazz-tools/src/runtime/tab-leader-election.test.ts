import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { TabLeaderElection } from "./tab-leader-election.js";

class MockBroadcastChannel {
  static channels = new Map<string, Set<MockBroadcastChannel>>();

  private listeners = new Set<(event: MessageEvent) => void>();
  private readonly name: string;

  constructor(name: string) {
    this.name = name;
    if (!MockBroadcastChannel.channels.has(name)) {
      MockBroadcastChannel.channels.set(name, new Set());
    }
    MockBroadcastChannel.channels.get(name)!.add(this);
  }

  addEventListener(type: "message", listener: (event: MessageEvent) => void): void {
    if (type !== "message") return;
    this.listeners.add(listener);
  }

  removeEventListener(type: "message", listener: (event: MessageEvent) => void): void {
    if (type !== "message") return;
    this.listeners.delete(listener);
  }

  postMessage(data: unknown): void {
    const peers = MockBroadcastChannel.channels.get(this.name);
    if (!peers) return;
    for (const peer of peers) {
      if (peer === this) continue;
      setTimeout(() => {
        for (const listener of peer.listeners) {
          listener({ data } as MessageEvent);
        }
      }, 0);
    }
  }

  close(): void {
    const peers = MockBroadcastChannel.channels.get(this.name);
    peers?.delete(this);
    if (peers && peers.size === 0) {
      MockBroadcastChannel.channels.delete(this.name);
    }
    this.listeners.clear();
  }

  static reset(): void {
    MockBroadcastChannel.channels.clear();
  }
}

describe("TabLeaderElection", () => {
  const originalBroadcastChannel = (globalThis as { BroadcastChannel?: unknown }).BroadcastChannel;

  beforeEach(() => {
    (globalThis as unknown as { BroadcastChannel?: typeof MockBroadcastChannel }).BroadcastChannel =
      MockBroadcastChannel;
  });

  afterEach(() => {
    MockBroadcastChannel.reset();
    (globalThis as { BroadcastChannel?: unknown }).BroadcastChannel = originalBroadcastChannel;
  });

  async function sleep(ms: number): Promise<void> {
    await new Promise<void>((resolve) => setTimeout(resolve, ms));
  }

  it("elects itself as leader when no other tabs exist", async () => {
    const election = new TabLeaderElection({
      appId: "test-app",
      dbName: "test-db",
      heartbeatMs: 50,
      leaseMs: 150,
      initialElectionDelayMs: 60,
      tabId: "tab-a",
    });

    election.start();
    await sleep(100);

    const state = election.snapshot();
    expect(state.role).toBe("leader");
    expect(state.leaderTabId).toBe("tab-a");
    expect(state.term).toBeGreaterThan(0);

    election.stop();
  });

  it("follows an existing leader", async () => {
    const leader = new TabLeaderElection({
      appId: "test-app",
      dbName: "test-db",
      heartbeatMs: 50,
      leaseMs: 200,
      initialElectionDelayMs: 60,
      tabId: "tab-a",
    });
    leader.start();
    await sleep(140);
    expect(leader.snapshot().role).toBe("leader");

    const follower = new TabLeaderElection({
      appId: "test-app",
      dbName: "test-db",
      heartbeatMs: 50,
      leaseMs: 200,
      initialElectionDelayMs: 60,
      tabId: "tab-b",
    });
    follower.start();
    await sleep(140);

    const followerState = follower.snapshot();
    expect(followerState.role).toBe("follower");
    expect(followerState.leaderTabId).toBe("tab-a");

    leader.stop();
    follower.stop();
  });

  it("fails over to another tab when leader stops heartbeating", async () => {
    const first = new TabLeaderElection({
      appId: "test-app",
      dbName: "test-db",
      heartbeatMs: 50,
      leaseMs: 200,
      initialElectionDelayMs: 60,
      tabId: "tab-a",
    });
    const second = new TabLeaderElection({
      appId: "test-app",
      dbName: "test-db",
      heartbeatMs: 50,
      leaseMs: 200,
      initialElectionDelayMs: 60,
      tabId: "tab-b",
    });

    first.start();
    second.start();
    await sleep(200);
    expect(
      [first.snapshot().role, second.snapshot().role].sort((a, b) => (a < b ? -1 : 1)),
    ).toEqual(["follower", "leader"]);

    first.stop();
    await sleep(280);

    const nextState = second.snapshot();
    expect(nextState.role).toBe("leader");
    expect(nextState.leaderTabId).toBe("tab-b");
    expect(nextState.term).toBeGreaterThan(0);

    second.stop();
  });

  it("falls back to single-tab leader mode when BroadcastChannel is unavailable", async () => {
    delete (globalThis as { BroadcastChannel?: unknown }).BroadcastChannel;

    const election = new TabLeaderElection({
      appId: "test-app",
      dbName: "test-db",
      heartbeatMs: 50,
      leaseMs: 200,
      tabId: "tab-a",
    });

    election.start();
    await sleep(10);

    const state = election.snapshot();
    expect(state.role).toBe("leader");
    expect(state.leaderTabId).toBe("tab-a");
    expect(state.term).toBeGreaterThan(0);

    election.stop();
  });
});
