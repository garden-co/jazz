import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { TabLeaderElection } from "./tab-leader-election.js";
import type { LeaderLockStrategy } from "./leader-lock.js";

class MockBroadcastChannel {
  static channels = new Map<string, Set<MockBroadcastChannel>>();
  static dropPredicate:
    | ((args: { channelName: string; data: unknown; sender: MockBroadcastChannel }) => boolean)
    | null = null;

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
      if (
        MockBroadcastChannel.dropPredicate?.({
          channelName: this.name,
          data,
          sender: this,
        })
      ) {
        continue;
      }
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
    MockBroadcastChannel.dropPredicate = null;
  }
}

describe("TabLeaderElection", () => {
  const originalBroadcastChannel = (globalThis as { BroadcastChannel?: unknown }).BroadcastChannel;
  const elections: TabLeaderElection[] = [];
  const lockOwners = new Map<string, string>();

  function createLockStrategyForTab(
    tabId: string,
    canAcquire: () => boolean = () => true,
  ): LeaderLockStrategy {
    return {
      async tryAcquire(lockName: string) {
        if (!canAcquire()) return null;
        const owner = lockOwners.get(lockName);
        if (owner && owner !== tabId) return null;
        lockOwners.set(lockName, tabId);
        let released = false;
        return {
          release: () => {
            if (released) return;
            released = true;
            if (lockOwners.get(lockName) === tabId) {
              lockOwners.delete(lockName);
            }
          },
        };
      },
    };
  }

  function createElection(
    tabId: string,
    options: Partial<ConstructorParameters<typeof TabLeaderElection>[0]> = {},
  ) {
    const election = new TabLeaderElection({
      appId: "test-app",
      dbName: "test-db",
      heartbeatMs: 100,
      leaseMs: 280,
      tabId,
      lockStrategy: createLockStrategyForTab(tabId),
      ...options,
    });
    elections.push(election);
    return election;
  }

  function getRoles(
    a: TabLeaderElection,
    b: TabLeaderElection,
  ): ["leader", "follower"] | ["follower", "leader"] {
    const roleA = a.snapshot().role;
    const roleB = b.snapshot().role;
    if (roleA === "leader" && roleB === "follower") return ["leader", "follower"];
    if (roleA === "follower" && roleB === "leader") return ["follower", "leader"];
    throw new Error(`Expected one leader and one follower, got ${roleA}/${roleB}`);
  }

  beforeEach(() => {
    vi.useFakeTimers();
    (globalThis as unknown as { BroadcastChannel?: typeof MockBroadcastChannel }).BroadcastChannel =
      MockBroadcastChannel;
  });

  afterEach(() => {
    for (const election of elections.splice(0)) {
      election.stop();
    }
    MockBroadcastChannel.reset();
    lockOwners.clear();
    (globalThis as { BroadcastChannel?: unknown }).BroadcastChannel = originalBroadcastChannel;
    vi.useRealTimers();
  });

  async function advance(ms: number): Promise<void> {
    await vi.advanceTimersByTimeAsync(ms);
    await Promise.resolve();
  }

  it("elects itself as leader when no other tabs exist", async () => {
    const election = createElection("tab-a");

    election.start();
    await advance(140);

    const state = election.snapshot();
    expect(state.role).toBe("leader");
    expect(state.leaderTabId).toBe("tab-a");
    expect(state.term).toBeGreaterThan(0);

    election.stop();
  });

  it("follows an existing leader", async () => {
    const leader = createElection("tab-a");
    leader.start();
    await advance(180);
    expect(leader.snapshot().role).toBe("leader");

    const follower = createElection("tab-b");
    follower.start();
    await advance(140);

    const followerState = follower.snapshot();
    expect(followerState.role).toBe("follower");
    expect(followerState.leaderTabId).toBe("tab-a");
    expect(followerState.term).toBe(leader.snapshot().term);

    await advance(400);
    expect(follower.snapshot().role).toBe("follower");
    expect(follower.snapshot().leaderTabId).toBe("tab-a");
  });

  it("fails over to another tab when leader stops heartbeating", async () => {
    const first = createElection("tab-a");
    const second = createElection("tab-b");

    first.start();
    second.start();
    await advance(220);

    const [firstRole] = getRoles(first, second);
    const leader = firstRole === "leader" ? first : second;
    const follower = firstRole === "leader" ? second : first;

    leader.stop();
    await advance(320);

    const nextState = follower.snapshot();
    expect(nextState.role).toBe("leader");
    expect(nextState.leaderTabId).toBe(nextState.tabId);
    expect(nextState.term).toBeGreaterThan(0);
  });

  it("converges to a single leader when two tabs start together", async () => {
    const a = createElection("tab-a");
    const z = createElection("tab-z");

    a.start();
    z.start();
    await advance(240);

    const stateA = a.snapshot();
    const stateZ = z.snapshot();
    expect(stateA.leaderTabId).toBe(stateZ.leaderTabId);
    expect(stateA.term).toBe(stateZ.term);
    expect([stateA.role, stateZ.role].sort()).toEqual(["follower", "leader"]);
  });

  it("ignores stale-term heartbeats", async () => {
    const election = createElection("tab-a");
    election.start();
    await advance(160);

    const before = election.snapshot();
    (election as any).handleIncomingMessage({
      type: "leader-heartbeat",
      leaderTabId: "tab-stale",
      term: Math.max(0, before.term - 1),
      sentAtMs: Date.now(),
    });

    const after = election.snapshot();
    expect(after).toEqual(before);
  });

  it("adopts a higher-term heartbeat and steps down", async () => {
    const election = createElection("tab-a");
    election.start();
    await advance(160);

    const before = election.snapshot();
    expect(before.role).toBe("leader");

    (election as any).handleIncomingMessage({
      type: "leader-heartbeat",
      leaderTabId: "tab-new",
      term: before.term + 3,
      sentAtMs: Date.now(),
    });

    const after = election.snapshot();
    expect(after.role).toBe("follower");
    expect(after.leaderTabId).toBe("tab-new");
    expect(after.term).toBe(before.term + 3);
  });

  it("still self-elects when startup request/heartbeats are dropped", async () => {
    MockBroadcastChannel.dropPredicate = () => true;
    const election = createElection("tab-a");

    election.start();
    await advance(140);

    const state = election.snapshot();
    expect(state.role).toBe("leader");
    expect(state.leaderTabId).toBe("tab-a");
    expect(state.term).toBeGreaterThan(0);
  });

  it("waitForInitialLeader rejects if stopped before a leader is chosen", async () => {
    const election = createElection("tab-a", {
      lockStrategy: createLockStrategyForTab("tab-a", () => false),
    });
    election.start();
    const leaderPromise = election.waitForInitialLeader(2000);
    election.stop();

    await expect(leaderPromise).rejects.toThrow(
      "Leader election stopped before initial leader was chosen",
    );
  });

  it("falls back to single-tab leader mode when BroadcastChannel is unavailable", async () => {
    delete (globalThis as { BroadcastChannel?: unknown }).BroadcastChannel;

    const election = createElection("tab-a");
    election.start();
    await advance(1);

    const state = election.snapshot();
    expect(state.role).toBe("leader");
    expect(state.leaderTabId).toBe("tab-a");
    expect(state.term).toBeGreaterThan(0);
  });

  it("uses take-then-ask-forgiveness startup: probe first, discover on failure", async () => {
    const leader = createElection("tab-a", {
      lockStrategy: createLockStrategyForTab("tab-a", () => true),
    });
    leader.start();
    await advance(140);
    expect(leader.snapshot().role).toBe("leader");

    const follower = createElection("tab-b", {
      lockStrategy: createLockStrategyForTab("tab-b", () => false),
    });
    follower.start();
    await advance(220);

    const state = follower.snapshot();
    expect(state.role).toBe("follower");
    expect(state.leaderTabId).toBe("tab-a");
    expect(state.term).toBe(leader.snapshot().term);
  });

  it("re-probes on leader loss and promotes when lock probe succeeds", async () => {
    const first = createElection("tab-a", {
      lockStrategy: createLockStrategyForTab("tab-a", () => true),
    });
    let secondCanLead = false;
    const second = createElection("tab-b", {
      lockStrategy: createLockStrategyForTab("tab-b", () => secondCanLead),
    });

    first.start();
    second.start();
    await advance(260);
    expect(first.snapshot().role).toBe("leader");
    expect(second.snapshot().role).toBe("follower");

    first.stop();
    secondCanLead = true;
    await advance(360);

    const nextState = second.snapshot();
    expect(nextState.role).toBe("leader");
    expect(nextState.leaderTabId).toBe("tab-b");
  });
});
