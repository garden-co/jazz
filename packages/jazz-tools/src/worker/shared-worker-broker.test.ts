import { describe, expect, it, vi } from "vitest";

import {
  installSharedWorkerBroker,
  type MessageChannelLike,
  type MessagePortLike,
  type SharedWorkerBrokerGlobal,
} from "./shared-worker-broker.js";

type Listener = (event: { data?: unknown }) => void;

class FakeMessagePort implements MessagePortLike {
  readonly posted: Array<{ message: unknown; transfer?: Transferable[] }> = [];
  readonly messageListeners = new Set<Listener>();
  readonly messageErrorListeners = new Set<Listener>();
  startCount = 0;
  closed = false;
  /**
   * When true, the port auto-replies to broker `leader-ping` posts with a
   * matching `leader-pong`. Tests that simulate a *live* leader (the
   * common case) set this so the broker's probe resolves before the
   * follower-port handoff. Leaving it false simulates a *stale* leader
   * (e.g. a SharedWorker that survived a dev-server restart whose tab is
   * gone) — those tests want the probe to time out and the broker to
   * evict.
   */
  autoPongOnLeaderPing = false;

  postMessage(message: unknown, transfer?: Transferable[]): void {
    this.posted.push({ message, transfer });
    if (
      this.autoPongOnLeaderPing &&
      typeof message === "object" &&
      message !== null &&
      (message as { type?: unknown }).type === "leader-ping"
    ) {
      const seq = (message as { seq?: unknown }).seq;
      if (typeof seq === "number") {
        // Synchronous emit lets the broker observe the pong before the
        // probe-timeout fires. The probe's `.then` continuation still
        // settles via a microtask, so callers need a `await
        // Promise.resolve()` before asserting the resulting channel.
        this.emit({ type: "leader-pong", seq });
      }
    }
  }

  addEventListener(type: "message" | "messageerror", listener: Listener): void {
    if (type === "message") this.messageListeners.add(listener);
    else this.messageErrorListeners.add(listener);
  }

  removeEventListener(type: "message" | "messageerror", listener: Listener): void {
    if (type === "message") this.messageListeners.delete(listener);
    else this.messageErrorListeners.delete(listener);
  }

  start(): void {
    this.startCount++;
  }

  close(): void {
    this.closed = true;
  }

  emit(message: unknown): void {
    for (const listener of this.messageListeners) listener({ data: message });
  }

  messageTypes(): unknown[] {
    return this.posted.map((entry) =>
      typeof entry.message === "object" && entry.message !== null
        ? (entry.message as { type?: unknown }).type
        : entry.message,
    );
  }
}

let channelCounter = 0;
class FakeMessageChannel implements MessageChannelLike {
  readonly port1: FakeMessagePort;
  readonly port2: FakeMessagePort;
  readonly id: number;

  constructor() {
    this.id = ++channelCounter;
    this.port1 = new FakeMessagePort();
    this.port2 = new FakeMessagePort();
  }
}

class FakeSharedWorkerGlobal implements SharedWorkerBrokerGlobal {
  onconnect: ((event: { ports: MessagePortLike[] }) => void) | null = null;

  connect(port: FakeMessagePort): void {
    this.onconnect?.({ ports: [port] });
  }
}

function setup(): {
  globalScope: FakeSharedWorkerGlobal;
  channels: FakeMessageChannel[];
} {
  const globalScope = new FakeSharedWorkerGlobal();
  const channels: FakeMessageChannel[] = [];
  installSharedWorkerBroker(globalScope, {
    MessageChannelCtor: class extends FakeMessageChannel {
      constructor() {
        super();
        channels.push(this);
      }
    },
  });
  return { globalScope, channels };
}

describe("SharedWorker broker", () => {
  it("starts each connected port and listens for messages", () => {
    const { globalScope } = setup();
    const port = new FakeMessagePort();
    globalScope.connect(port);
    expect(port.startCount).toBe(1);
    expect(port.messageListeners.size).toBe(1);
  });

  it("responds with no-leader when a follower requests before any claim", () => {
    const { globalScope } = setup();
    const follower = new FakeMessagePort();
    globalScope.connect(follower);

    follower.emit({ type: "request-leader" });

    expect(follower.messageTypes()).toEqual(["no-leader"]);
    expect(follower.posted[0]!.transfer).toBeUndefined();
  });

  it("hands a fresh MessageChannel out on follower request once a leader has claimed", async () => {
    const { globalScope, channels } = setup();
    const leader = new FakeMessagePort();
    leader.autoPongOnLeaderPing = true;
    const follower = new FakeMessagePort();
    globalScope.connect(leader);
    globalScope.connect(follower);

    leader.emit({ type: "claim-leader" });
    follower.emit({ type: "request-leader" });
    await Promise.resolve();

    expect(channels).toHaveLength(1);
    const channel = channels[0]!;

    // Follower receives port1 with `leader-port`.
    const followerHandoff = follower.posted.find(
      (entry) => (entry.message as { type?: unknown }).type === "leader-port",
    );
    expect(followerHandoff).toBeDefined();
    expect(followerHandoff!.transfer).toEqual([channel.port1]);

    // Leader receives port2 with `follower-port`.
    const leaderHandoff = leader.posted.find(
      (entry) => (entry.message as { type?: unknown }).type === "follower-port",
    );
    expect(leaderHandoff).toBeDefined();
    expect(leaderHandoff!.transfer).toEqual([channel.port2]);
  });

  it("broadcasts leader-changed to every port except the new leader on claim", () => {
    const { globalScope } = setup();
    const a = new FakeMessagePort();
    const b = new FakeMessagePort();
    const c = new FakeMessagePort();
    globalScope.connect(a);
    globalScope.connect(b);
    globalScope.connect(c);

    b.emit({ type: "claim-leader" });

    expect(a.messageTypes()).toEqual(["leader-changed"]);
    expect(c.messageTypes()).toEqual(["leader-changed"]);
    expect(b.posted).toHaveLength(0);
  });

  it("does not re-broadcast leader-changed when the same port re-claims", () => {
    const { globalScope } = setup();
    const leader = new FakeMessagePort();
    const follower = new FakeMessagePort();
    globalScope.connect(leader);
    globalScope.connect(follower);

    leader.emit({ type: "claim-leader" });
    expect(follower.messageTypes()).toEqual(["leader-changed"]);
    leader.emit({ type: "claim-leader" });
    // Still one — idempotent claim is silent.
    expect(follower.messageTypes()).toEqual(["leader-changed"]);
  });

  it("rebroadcasts leader-changed when a new tab takes leadership over an existing leader", () => {
    const { globalScope } = setup();
    const a = new FakeMessagePort();
    const b = new FakeMessagePort();
    const c = new FakeMessagePort();
    globalScope.connect(a);
    globalScope.connect(b);
    globalScope.connect(c);

    a.emit({ type: "claim-leader" });
    expect(b.messageTypes()).toEqual(["leader-changed"]);
    expect(c.messageTypes()).toEqual(["leader-changed"]);

    const bPostedBeforeOwnClaim = b.posted.length;
    b.emit({ type: "claim-leader" });
    // a (old leader) and c are notified; b receives nothing new from its own
    // claim (it already had the prior leader-changed from a's claim).
    expect(a.messageTypes()).toEqual(["leader-changed"]);
    expect(c.messageTypes()).toEqual(["leader-changed", "leader-changed"]);
    expect(b.posted.length).toBe(bPostedBeforeOwnClaim);
  });

  it("routes follower-request to whichever port last claimed", async () => {
    const { globalScope, channels } = setup();
    const a = new FakeMessagePort();
    const b = new FakeMessagePort();
    b.autoPongOnLeaderPing = true;
    const follower = new FakeMessagePort();
    globalScope.connect(a);
    globalScope.connect(b);
    globalScope.connect(follower);

    a.emit({ type: "claim-leader" });
    b.emit({ type: "claim-leader" });

    follower.emit({ type: "request-leader" });
    await Promise.resolve();

    expect(channels).toHaveLength(1);
    const followerToLeader = follower.posted.find(
      (entry) => (entry.message as { type?: unknown }).type === "leader-port",
    );
    expect(followerToLeader).toBeDefined();
    const newLeaderToFollower = b.posted.find(
      (entry) => (entry.message as { type?: unknown }).type === "follower-port",
    );
    expect(newLeaderToFollower).toBeDefined();
    expect(
      a.posted.some((entry) => (entry.message as { type?: unknown }).type === "follower-port"),
    ).toBe(false);
  });

  it("clears the leader on release and broadcasts leader-changed to everyone", async () => {
    const { globalScope } = setup();
    const leader = new FakeMessagePort();
    leader.autoPongOnLeaderPing = true;
    const follower = new FakeMessagePort();
    globalScope.connect(leader);
    globalScope.connect(follower);

    leader.emit({ type: "claim-leader" });
    follower.emit({ type: "request-leader" });
    await Promise.resolve();
    expect(follower.messageTypes()).toContain("leader-port");

    leader.emit({ type: "release-leader" });

    // Everyone gets leader-changed, including the releasing tab — it may want
    // to know its own release was applied (and the broker is the source of
    // truth, not the supervisor).
    expect(leader.messageTypes()).toContain("leader-changed");
    expect(follower.messageTypes()).toContain("leader-changed");

    const followerMessagesAfter = follower.posted.length;
    follower.emit({ type: "request-leader" });
    expect(follower.messageTypes().slice(followerMessagesAfter)).toEqual(["no-leader"]);
  });

  it("ignores release-leader from a port that is not the current leader", () => {
    const { globalScope } = setup();
    const leader = new FakeMessagePort();
    const other = new FakeMessagePort();
    globalScope.connect(leader);
    globalScope.connect(other);

    leader.emit({ type: "claim-leader" });
    const followerMessagesBefore = leader.posted.length;
    other.emit({ type: "release-leader" });

    // The real leader was not displaced, so no new leader-changed fan-out.
    expect(leader.posted.length).toBe(followerMessagesBefore);
  });

  it("does not wire a self-loop when the leader requests its own port", () => {
    const { globalScope, channels } = setup();
    const leader = new FakeMessagePort();
    globalScope.connect(leader);

    leader.emit({ type: "claim-leader" });
    leader.emit({ type: "request-leader" });

    expect(channels).toHaveLength(0);
    expect(leader.messageTypes()).toEqual(["no-leader"]);
  });

  it("probes the leader before minting and posts leader-port only after a pong", async () => {
    const { globalScope, channels } = setup();
    const leader = new FakeMessagePort();
    leader.autoPongOnLeaderPing = true;
    const follower = new FakeMessagePort();
    globalScope.connect(leader);
    globalScope.connect(follower);

    leader.emit({ type: "claim-leader" });
    follower.emit({ type: "request-leader" });

    // Synchronously the broker has posted the ping but not yet the channel.
    expect(leader.messageTypes()).toContain("leader-ping");
    expect(channels).toHaveLength(0);
    expect(follower.messageTypes().filter((t) => t === "leader-port")).toEqual([]);

    // After the pong settles via microtask, the channel is minted.
    await Promise.resolve();
    expect(channels).toHaveLength(1);
    expect(follower.messageTypes()).toContain("leader-port");
  });

  it("evicts a stale leader whose port never pongs and replies no-leader", async () => {
    vi.useFakeTimers();
    try {
      const { globalScope, channels } = setup();
      // Stale leader: claimed before but is no longer alive (e.g. a
      // SharedWorker that survived a dev-server restart while the
      // previous leader tab is gone). autoPongOnLeaderPing stays false.
      const stale = new FakeMessagePort();
      const observer = new FakeMessagePort();
      const follower = new FakeMessagePort();
      globalScope.connect(stale);
      globalScope.connect(observer);
      globalScope.connect(follower);

      stale.emit({ type: "claim-leader" });
      // observer already saw the original leader-changed; clear so we can
      // assert the eviction broadcast cleanly.
      observer.posted.length = 0;

      follower.emit({ type: "request-leader" });
      // Probe fires synchronously but the timeout drives the eviction.
      expect(stale.messageTypes()).toContain("leader-ping");
      await vi.advanceTimersByTimeAsync(300);

      expect(channels).toHaveLength(0);
      expect(follower.messageTypes()).toContain("no-leader");
      expect(observer.messageTypes()).toContain("leader-changed");

      // A fresh claim can now succeed; the next follower request mints.
      const fresh = new FakeMessagePort();
      fresh.autoPongOnLeaderPing = true;
      globalScope.connect(fresh);
      fresh.emit({ type: "claim-leader" });
      follower.posted.length = 0;
      follower.emit({ type: "request-leader" });
      await Promise.resolve();
      expect(follower.messageTypes()).toContain("leader-port");
    } finally {
      vi.useRealTimers();
    }
  });

  it("ignores a leader-pong from a port that is no longer the current leader", async () => {
    vi.useFakeTimers();
    try {
      const { globalScope, channels } = setup();
      const stale = new FakeMessagePort(); // does NOT auto-pong
      const follower = new FakeMessagePort();
      globalScope.connect(stale);
      globalScope.connect(follower);

      stale.emit({ type: "claim-leader" });
      follower.emit({ type: "request-leader" });
      const ping = stale.posted.find(
        (e) => (e.message as { type?: unknown }).type === "leader-ping",
      )!;
      const seq = (ping.message as { seq: number }).seq;

      // Simulate the stale tab being evicted (release-leader fired by
      // some teardown path) before its tardy pong shows up.
      stale.emit({ type: "release-leader" });
      stale.emit({ type: "leader-pong", seq });

      // The stale pong must not revive the evicted port; probe still
      // resolves false at the timeout boundary and follower gets
      // no-leader.
      await vi.advanceTimersByTimeAsync(300);
      expect(channels).toHaveLength(0);
      expect(follower.messageTypes()).toContain("no-leader");
    } finally {
      vi.useRealTimers();
    }
  });

  it("ignores non-protocol messages", () => {
    const { globalScope } = setup();
    const port = new FakeMessagePort();
    globalScope.connect(port);

    port.emit({ type: "something-else" });
    port.emit("not an object");
    port.emit(null);
    port.emit(undefined);

    expect(port.posted).toHaveLength(0);
  });

  it("throws if installed without MessageChannel support", () => {
    const globalScope = new FakeSharedWorkerGlobal();
    const original = (globalThis as { MessageChannel?: unknown }).MessageChannel;
    delete (globalThis as { MessageChannel?: unknown }).MessageChannel;
    try {
      expect(() => installSharedWorkerBroker(globalScope)).toThrow(/MessageChannel/);
    } finally {
      (globalThis as { MessageChannel?: unknown }).MessageChannel = original;
    }
  });
});
