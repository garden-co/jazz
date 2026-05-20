import { afterEach, describe, expect, it } from "vitest";

import {
  createTabSupervisor,
  type LocksBackend,
  type SupervisorBrokerPort,
  type SupervisorWorkerLike,
} from "./shared-worker-supervisor.js";

type BrokerListener = (event: { data?: unknown; ports?: ReadonlyArray<unknown> }) => void;

class FakeBrokerPort implements SupervisorBrokerPort {
  readonly posted: Array<{ message: unknown; transfer?: Transferable[] }> = [];
  readonly messageListeners = new Set<BrokerListener>();
  startCount = 0;

  postMessage(message: unknown, transfer?: Transferable[]): void {
    this.posted.push({ message, transfer });
  }

  addEventListener(type: "message", listener: BrokerListener): void {
    this.messageListeners.add(listener);
  }

  removeEventListener(type: "message", listener: BrokerListener): void {
    this.messageListeners.delete(listener);
  }

  start(): void {
    this.startCount++;
  }

  emit(message: unknown, ports?: ReadonlyArray<unknown>): void {
    for (const listener of this.messageListeners) {
      listener({ data: message, ports });
    }
  }

  postedTypes(): unknown[] {
    return this.posted.map((entry) =>
      typeof entry.message === "object" && entry.message !== null
        ? (entry.message as { type?: unknown }).type
        : entry.message,
    );
  }
}

class FakeWorker implements SupervisorWorkerLike {
  static readonly instances: FakeWorker[] = [];

  readonly posted: Array<{ message: unknown; transfer?: Transferable[] }> = [];
  terminated = false;

  constructor(
    readonly url: string | URL,
    readonly options?: WorkerOptions,
  ) {
    FakeWorker.instances.push(this);
  }

  postMessage(message: unknown, transfer?: Transferable[]): void {
    this.posted.push({ message, transfer });
  }

  terminate(): void {
    this.terminated = true;
  }
}

/**
 * Fake `LocksBackend` that lets the test choose when the lock is granted and
 * when it gets released, so leader transitions are deterministic.
 */
class FakeLocks implements LocksBackend {
  readonly requests: Array<{
    name: string;
    signal?: AbortSignal;
    holdWhile: () => Promise<void>;
  }> = [];
  private holdResolves: Array<() => void> = [];

  request(
    name: string,
    options: { signal?: AbortSignal },
    holdWhile: () => Promise<void>,
  ): Promise<void> {
    this.requests.push({ name, signal: options.signal, holdWhile });
    return new Promise<void>((resolve, reject) => {
      const idx = this.holdResolves.length;
      this.holdResolves.push(resolve);
      // Mirror real navigator.locks behaviour: aborting the signal rejects
      // the outer request promise so awaiters of shutdown() can proceed.
      options.signal?.addEventListener("abort", () => {
        this.holdResolves[idx] = () => {};
        reject(new DOMException("AbortError", "AbortError"));
      });
    });
  }

  /** Pretend the OS just granted the lock for the most recent request. */
  async grant(): Promise<void> {
    const req = this.requests[this.requests.length - 1];
    if (!req) throw new Error("no lock request to grant");
    // Start the holdWhile — supervisor will resolve it on release/shutdown,
    // at which point we resolve the outer request promise.
    const requestIndex = this.requests.length - 1;
    void req.holdWhile().then(() => {
      const resolveOuter = this.holdResolves[requestIndex];
      resolveOuter?.();
    });
    await flush();
  }
}

function flush(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

afterEach(() => {
  FakeWorker.instances.length = 0;
});

function brokerHandshake() {
  const brokerPort = new FakeBrokerPort();
  const locks = new FakeLocks();
  return {
    brokerPort,
    locks,
    boot(opts: { workerUrl?: string } = {}) {
      return createTabSupervisor({
        brokerPort,
        lockName: "jazz:leader:app:db:v1",
        locks,
        WorkerCtor: FakeWorker,
        workerUrl: opts.workerUrl ?? "jazz-worker.js",
        workerOptions: { type: "module", name: "jazz-runtime" },
      });
    },
  };
}

describe("createTabSupervisor", () => {
  it("attaches a broker listener, starts the port, and posts the initial request-leader", () => {
    const { brokerPort, boot } = brokerHandshake();
    boot();

    expect(brokerPort.startCount).toBe(1);
    expect(brokerPort.messageListeners.size).toBe(1);
    expect(brokerPort.postedTypes()).toEqual(["request-leader"]);
  });

  it("attempts to acquire the named lock", () => {
    const { locks, boot } = brokerHandshake();
    boot();
    expect(locks.requests).toHaveLength(1);
    expect(locks.requests[0]!.name).toBe("jazz:leader:app:db:v1");
  });

  it("stays role=none when the broker reports no-leader and no lock has been granted", () => {
    const { brokerPort, boot } = brokerHandshake();
    const sup = boot();
    brokerPort.emit({ type: "no-leader" });
    expect(sup.state.role).toBe("none");
    expect(sup.state.endpoint).toBeNull();
  });

  it("becomes follower when the broker delivers a leader-port", () => {
    const { brokerPort, boot } = brokerHandshake();
    const sup = boot();
    const port = { postMessage: () => {} };
    brokerPort.emit({ type: "leader-port" }, [port]);
    expect(sup.state.role).toBe("follower");
    expect(sup.state.endpoint).toBe(port);
  });

  it("becomes leader on lock grant: spawns a worker, exposes it as endpoint, and claims leadership eagerly", async () => {
    const { brokerPort, locks, boot } = brokerHandshake();
    const sup = boot();

    await locks.grant();

    expect(FakeWorker.instances).toHaveLength(1);
    const worker = FakeWorker.instances[0]!;
    expect(String(worker.url)).toBe("jazz-worker.js");
    expect(worker.options).toEqual({ type: "module", name: "jazz-runtime" });

    // Claim happens eagerly: without it, a second tab whose own
    // `createDb()` is awaiting an endpoint can never become follower
    // because the broker has no claimed leader to attach to. The race the
    // old withheld-claim guarded against (a follower port arriving before
    // Rust owns `onmessage`) is now handled by the JS shim buffering
    // `event.ports` and Rust draining them after Ready.
    expect(brokerPort.postedTypes()).toContain("claim-leader");
    expect(sup.state.role).toBe("leader");
    expect(sup.state.endpoint).toBe(worker);
  });

  it("notifyLeaderReady is a retained-for-compat no-op", async () => {
    const { brokerPort, locks, boot } = brokerHandshake();
    const sup = boot();

    // Before lock grant: harmless no-op, no extra claims.
    sup.notifyLeaderReady();
    expect(brokerPort.postedTypes()).not.toContain("claim-leader");

    await locks.grant();
    const claimsAfterGrant = brokerPort.postedTypes().filter((t) => t === "claim-leader").length;
    expect(claimsAfterGrant).toBe(1);

    // After grant: still a no-op; does not duplicate the eager claim.
    sup.notifyLeaderReady();
    sup.notifyLeaderReady();
    const claimsAfterCalls = brokerPort.postedTypes().filter((t) => t === "claim-leader").length;
    expect(claimsAfterCalls).toBe(1);
  });

  it("forwards follower-port deliveries to the dedicated worker with the port in the transfer list", async () => {
    const { brokerPort, locks, boot } = brokerHandshake();
    boot();
    await locks.grant();
    const worker = FakeWorker.instances[0]!;

    const incomingPort = { postMessage: () => {} };
    brokerPort.emit({ type: "follower-port" }, [incomingPort]);

    expect(worker.posted).toHaveLength(1);
    expect(worker.posted[0]!.message).toEqual({ type: "attach-tab-port" });
    expect(worker.posted[0]!.transfer).toEqual([incomingPort]);
  });

  it("ignores leader-changed while leader (the broker doesn't echo our own claim to us, but be defensive)", async () => {
    const { brokerPort, locks, boot } = brokerHandshake();
    const sup = boot();
    await locks.grant();
    const leaderEndpointBefore = sup.state.endpoint;

    brokerPort.emit({ type: "leader-changed" });

    expect(sup.state.role).toBe("leader");
    expect(sup.state.endpoint).toBe(leaderEndpointBefore);
  });

  it("on leader-changed as a follower, drops the stale endpoint and re-requests", () => {
    const { brokerPort, boot } = brokerHandshake();
    const sup = boot();
    const stalePort = { postMessage: () => {} };
    brokerPort.emit({ type: "leader-port" }, [stalePort]);
    expect(sup.state.role).toBe("follower");

    const postedBefore = brokerPort.posted.length;
    brokerPort.emit({ type: "leader-changed" });

    expect(sup.state.role).toBe("none");
    expect(sup.state.endpoint).toBeNull();
    // A new request-leader was posted.
    expect(brokerPort.postedTypes().slice(postedBefore)).toEqual(["request-leader"]);
  });

  it("re-attaches to a new leader-port after a leader-changed", () => {
    const { brokerPort, boot } = brokerHandshake();
    const sup = boot();
    const oldPort = { postMessage: () => {} };
    brokerPort.emit({ type: "leader-port" }, [oldPort]);
    brokerPort.emit({ type: "leader-changed" });
    const newPort = { postMessage: () => {} };
    brokerPort.emit({ type: "leader-port" }, [newPort]);

    expect(sup.state.role).toBe("follower");
    expect(sup.state.endpoint).toBe(newPort);
  });

  it("notifies subscribers on every state transition", async () => {
    const { brokerPort, locks, boot } = brokerHandshake();
    const sup = boot();
    const states: string[] = [];
    sup.subscribe((s) => states.push(s.role));

    brokerPort.emit({ type: "no-leader" });
    await locks.grant();

    expect(states).toEqual(["none", "leader"]);
  });

  it("shutdown while leader: posts release-leader, terminates the worker, and clears state", async () => {
    const { brokerPort, locks, boot } = brokerHandshake();
    const sup = boot();
    await locks.grant();
    const worker = FakeWorker.instances[0]!;

    await sup.shutdown();

    expect(brokerPort.postedTypes()).toContain("release-leader");
    expect(worker.terminated).toBe(true);
    expect(sup.state.role).toBe("none");
    expect(sup.state.endpoint).toBeNull();
  });

  it("shutdown while follower: does not post release-leader, just clears state", () => {
    const { brokerPort, boot } = brokerHandshake();
    const sup = boot();
    const port = { postMessage: () => {} };
    brokerPort.emit({ type: "leader-port" }, [port]);
    expect(sup.state.role).toBe("follower");

    const postedBefore = brokerPort.posted.length;
    sup.shutdown();

    expect(brokerPort.postedTypes().slice(postedBefore)).not.toContain("release-leader");
    expect(sup.state.role).toBe("none");
  });

  it("shutdown while still a queued lock waiter (role=none): aborts the lock request, posts no release-leader, and completes", async () => {
    const { brokerPort, locks, boot } = brokerHandshake();
    const sup = boot();

    // Lock requested but never granted: this tab is queued behind the
    // current leader and stays role=none. Models the user navigating away
    // before winning the election.
    expect(sup.state.role).toBe("none");
    const signal = locks.requests[0]!.signal;
    expect(signal?.aborted).toBe(false);

    const postedBefore = brokerPort.posted.length;

    // Resolves only because shutdown aborts the lock signal, which rejects
    // the still-pending request promise so `await lockPromise` can settle.
    // Without the abort this await would hang and the test would time out.
    await sup.shutdown();

    expect(signal?.aborted).toBe(true);
    // Never became leader, so the broker has nothing to release.
    expect(brokerPort.postedTypes().slice(postedBefore)).not.toContain("release-leader");
    expect(FakeWorker.instances).toHaveLength(0);
    expect(sup.state.role).toBe("none");
    expect(sup.state.endpoint).toBeNull();
  });

  it("ignores broker messages after shutdown", async () => {
    const { brokerPort, boot } = brokerHandshake();
    const sup = boot();
    await sup.shutdown();

    const port = { postMessage: () => {} };
    brokerPort.emit({ type: "leader-port" }, [port]);

    expect(sup.state.role).toBe("none");
    expect(sup.state.endpoint).toBeNull();
  });

  it("responds to broker leader-ping with a matching leader-pong while it holds the lock", async () => {
    const { brokerPort, locks, boot } = brokerHandshake();
    boot();
    await locks.grant();

    const postedBefore = brokerPort.posted.length;
    brokerPort.emit({ type: "leader-ping", seq: 42 });

    const pong = brokerPort.posted
      .slice(postedBefore)
      .find((e) => (e.message as { type?: unknown }).type === "leader-pong");
    expect(pong).toBeDefined();
    expect((pong!.message as { seq?: unknown }).seq).toBe(42);
  });

  it("re-claims leadership on leader-changed if it still holds the lock", async () => {
    const { brokerPort, locks, boot } = brokerHandshake();
    boot();
    await locks.grant();
    // Confirm we claimed once on grant.
    const claimsAfterGrant = brokerPort.postedTypes().filter((t) => t === "claim-leader").length;
    expect(claimsAfterGrant).toBe(1);

    // Broker evicted us as stale (e.g. we missed a leader-ping window
    // because the main thread was busy) and broadcast leader-changed.
    // We still hold the Web Lock, so we must re-claim to put the broker
    // back in sync — otherwise followers would keep getting `no-leader`
    // while we sit on the runtime they want to attach to.
    brokerPort.emit({ type: "leader-changed" });

    const claimsAfterEvent = brokerPort.postedTypes().filter((t) => t === "claim-leader").length;
    expect(claimsAfterEvent).toBe(2);
  });

  it("does not respond to leader-ping before the lock has been granted", () => {
    const { brokerPort, boot } = brokerHandshake();
    boot();
    // Lock not yet granted — `leaderClaimed` is false and the supervisor
    // must not impersonate an unclaimed leadership. If it pongs here, a
    // stale broker would incorrectly believe a fresh leader exists.
    const postedBefore = brokerPort.posted.length;
    brokerPort.emit({ type: "leader-ping", seq: 1 });
    const newPosts = brokerPort.posted.slice(postedBefore);
    expect(newPosts.some((e) => (e.message as { type?: unknown }).type === "leader-pong")).toBe(
      false,
    );
  });
});
