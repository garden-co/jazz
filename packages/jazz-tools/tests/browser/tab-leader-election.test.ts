import { afterEach, describe, expect, it } from "vitest";
import { TabLeaderElection } from "../../src/runtime/tab-leader-election.js";
import type { LeaderLockStrategy } from "../../src/runtime/leader-lock.js";

function uniqueName(prefix: string): string {
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

async function waitForCondition(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs: number,
  message: string,
): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await predicate()) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error(message);
}

describe("tab-leader-election browser integration", () => {
  const elections: TabLeaderElection[] = [];

  function createElection(
    appId: string,
    dbName: string,
    tabId: string,
    options: Partial<ConstructorParameters<typeof TabLeaderElection>[0]> = {},
  ): TabLeaderElection {
    const election = new TabLeaderElection({
      appId,
      dbName,
      tabId,
      heartbeatMs: 100,
      leaseMs: 280,
      ...options,
    });
    elections.push(election);
    return election;
  }

  afterEach(() => {
    for (const election of elections.splice(0)) {
      election.stop();
    }
  });

  it("elects itself as leader when no competing tab holds the lock", async () => {
    const appId = uniqueName("app");
    const dbName = uniqueName("db");
    const election = createElection(appId, dbName, "tab-a");
    election.start();

    await waitForCondition(
      () => election.snapshot().role === "leader",
      2000,
      "expected single tab to become leader",
    );

    const state = election.snapshot();
    expect(state.role).toBe("leader");
    expect(state.leaderTabId).toBe("tab-a");
  });

  it("follows an existing leader when lock is already held", async () => {
    const appId = uniqueName("app");
    const dbName = uniqueName("db");
    const leader = createElection(appId, dbName, "tab-a");
    leader.start();
    await waitForCondition(() => leader.snapshot().role === "leader", 2000, "leader not elected");

    const follower = createElection(appId, dbName, "tab-b");
    follower.start();

    await waitForCondition(
      () => {
        const state = follower.snapshot();
        return state.role === "follower" && state.leaderTabId === "tab-a";
      },
      2500,
      "follower did not attach to current leader",
    );
  });

  it("fails over to the remaining tab after leader stops", async () => {
    const appId = uniqueName("app");
    const dbName = uniqueName("db");
    const a = createElection(appId, dbName, "tab-a");
    const b = createElection(appId, dbName, "tab-b");
    a.start();
    b.start();

    await waitForCondition(
      () => {
        const roleA = a.snapshot().role;
        const roleB = b.snapshot().role;
        return (
          (roleA === "leader" && roleB === "follower") ||
          (roleA === "follower" && roleB === "leader")
        );
      },
      3000,
      "pair did not converge to leader/follower",
    );

    const leader = a.snapshot().role === "leader" ? a : b;
    const follower = leader === a ? b : a;
    leader.stop();

    await waitForCondition(
      () => {
        const state = follower.snapshot();
        return state.role === "leader" && state.leaderTabId === state.tabId;
      },
      3000,
      "follower did not take over after leader stop",
    );
  });

  it("converges to exactly one leader when two tabs start together", async () => {
    const appId = uniqueName("app");
    const dbName = uniqueName("db");
    const a = createElection(appId, dbName, "tab-a");
    const b = createElection(appId, dbName, "tab-b");
    a.start();
    b.start();

    await waitForCondition(
      () => {
        const roleA = a.snapshot().role;
        const roleB = b.snapshot().role;
        return (
          (roleA === "leader" && roleB === "follower") ||
          (roleA === "follower" && roleB === "leader")
        );
      },
      3000,
      "did not converge to single leader",
    );

    const stateA = a.snapshot();
    const stateB = b.snapshot();
    expect(stateA.leaderTabId).toBe(stateB.leaderTabId);
    expect([stateA.role, stateB.role].sort()).toEqual(["follower", "leader"]);
  });

  it("ignores stale-term heartbeats", async () => {
    const appId = uniqueName("app");
    const dbName = uniqueName("db");
    const election = createElection(appId, dbName, "tab-a");
    election.start();
    await waitForCondition(() => election.snapshot().role === "leader", 2000, "leader not elected");

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
    const appId = uniqueName("app");
    const dbName = uniqueName("db");
    const election = createElection(appId, dbName, "tab-a");
    election.start();
    await waitForCondition(() => election.snapshot().role === "leader", 2000, "leader not elected");

    const before = election.snapshot();
    (election as any).handleIncomingMessage({
      type: "leader-heartbeat",
      leaderTabId: "tab-new",
      term: before.term + 5,
      sentAtMs: Date.now(),
    });

    const after = election.snapshot();
    expect(after.role).toBe("follower");
    expect(after.leaderTabId).toBe("tab-new");
    expect(after.term).toBe(before.term + 5);
  });

  it("waitForInitialLeader rejects if stopped before any leader is chosen", async () => {
    const appId = uniqueName("app");
    const dbName = uniqueName("db");
    const neverAcquire: LeaderLockStrategy = {
      async tryAcquire() {
        return null;
      },
    };

    const originalBroadcastChannel = (globalThis as { BroadcastChannel?: unknown })
      .BroadcastChannel;
    delete (globalThis as { BroadcastChannel?: unknown }).BroadcastChannel;
    try {
      const election = createElection(appId, dbName, "tab-a", { lockStrategy: neverAcquire });
      election.start();
      const promise = election.waitForInitialLeader(1000);
      election.stop();
      await expect(promise).rejects.toThrow(
        "Leader election stopped before initial leader was chosen",
      );
    } finally {
      (globalThis as { BroadcastChannel?: unknown }).BroadcastChannel = originalBroadcastChannel;
    }
  });

  it("does not fail over while leader lock is still held even without heartbeat/discovery messages", async () => {
    const appId = uniqueName("app");
    const dbName = uniqueName("db");
    const originalBroadcastChannel = (globalThis as { BroadcastChannel?: unknown })
      .BroadcastChannel;
    delete (globalThis as { BroadcastChannel?: unknown }).BroadcastChannel;
    try {
      const a = createElection(appId, dbName, "tab-a");
      const b = createElection(appId, dbName, "tab-b");
      a.start();
      b.start();

      await waitForCondition(
        () => {
          const roleA = a.snapshot().role;
          const roleB = b.snapshot().role;
          return (
            (roleA === "leader" && roleB === "follower") ||
            (roleA === "follower" && roleB === "leader")
          );
        },
        3000,
        "pair did not converge to leader/follower with lock-only coordination",
      );

      const leader = a.snapshot().role === "leader" ? a : b;
      const follower = leader === a ? b : a;

      await new Promise((resolve) => setTimeout(resolve, 900));
      expect(follower.snapshot().role).toBe("follower");

      leader.stop();
      await waitForCondition(
        () => follower.snapshot().role === "leader",
        3000,
        "follower did not take over after leader released lock",
      );
    } finally {
      (globalThis as { BroadcastChannel?: unknown }).BroadcastChannel = originalBroadcastChannel;
    }
  });
});
