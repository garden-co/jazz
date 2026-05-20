/**
 * Per-tab supervisor for the leader-tab runtime topology.
 *
 * Runs in the *tab* main thread (not in the SharedWorker, not in any
 * dedicated worker). Its responsibilities:
 *
 *   1. Hold a port to the SharedWorker broker (one per `(appId, dbName)`).
 *   2. Acquire the `navigator.locks` leader lease. The lock holder becomes
 *      the leader: it spawns a dedicated `Worker` that hosts the durable
 *      runtime (OPFS, upstream socket), and tells the broker to route
 *      follower requests to it.
 *   3. When this tab is not the leader, request a `MessagePort` to the
 *      current leader's runtime worker from the broker and surface it as the
 *      `endpoint`. Re-request on every `leader-changed` event.
 *   4. While leader, accept `follower-port` deliveries from the broker and
 *      forward each to the dedicated worker so the worker can register that
 *      tab as a peer session.
 *   5. On `shutdown()`, release the leader claim and lock, and terminate
 *      the dedicated worker.
 *
 * The supervisor itself does not perform sync, query, or storage work — it
 * only manages *which endpoint* `Db` should attach its `WorkerBridge` to,
 * and signals transitions through {@link TabSupervisor.subscribe}.
 *
 * See `specs/todo/b_launch/leader_tab_runtime.md`.
 */

import type { WorkerBridgeEndpoint } from "./worker-bridge.js";

export type TabSupervisorRole = "none" | "leader" | "follower";

export interface TabSupervisorState {
  role: TabSupervisorRole;
  /**
   * The endpoint to attach a `WorkerBridge` to. While `role === "leader"`,
   * this is the dedicated `Worker` this tab spawned. While
   * `role === "follower"`, it is a `MessagePort` minted by the broker. While
   * `role === "none"`, no leader exists yet and the consumer must wait for
   * a state change.
   */
  endpoint: WorkerBridgeEndpoint | null;
}

export interface TabSupervisor {
  readonly state: TabSupervisorState;
  subscribe(listener: (state: TabSupervisorState) => void): () => void;
  /**
   * Historical hook used by `Db` after `WorkerBridge.init()` completed. Kept
   * as a no-op so existing callers compile, but no longer needed for
   * correctness: the supervisor now claims leadership eagerly as soon as
   * the dedicated worker is constructed.
   *
   * The race the old withhold prevented (a follower-port delivered before
   * Rust owns `onmessage` would lose its transferred `MessagePort`) is now
   * handled in two places downstream: the JS worker shim buffers
   * `event.ports` alongside `event.data`, and Rust `runAsWorker` extracts
   * those ports into `host.pending_ports` and drains them after the runtime
   * transitions to `Ready`. With those in place, eager claim is safe and
   * lets a second tab become follower without waiting for the leader's
   * main thread to call `bridge.init` (which only happens when user code
   * invokes `getClient` — i.e., possibly never).
   */
  notifyLeaderReady(): void;
  shutdown(): Promise<void>;
}

/** Subset of `MessagePort` the supervisor needs (broker side and forwarded). */
export interface SupervisorBrokerPort {
  postMessage(message: unknown, transfer?: Transferable[]): void;
  addEventListener(
    type: "message",
    listener: (event: { data?: unknown; ports?: ReadonlyArray<unknown> }) => void,
  ): void;
  removeEventListener(
    type: "message",
    listener: (event: { data?: unknown; ports?: ReadonlyArray<unknown> }) => void,
  ): void;
  start?(): void;
  close?(): void;
}

/** Subset of `Worker` the supervisor needs. */
export interface SupervisorWorkerLike extends WorkerBridgeEndpoint {
  postMessage(message: unknown, transfer?: Transferable[]): void;
  terminate?(): void;
}

export interface SupervisorWorkerCtor {
  new (url: string | URL, options?: WorkerOptions): SupervisorWorkerLike;
}

/**
 * Pluggable `navigator.locks` backend so tests can drive election
 * deterministically. The browser backend wraps `navigator.locks.request`.
 */
export interface LocksBackend {
  /**
   * Acquires an exclusive lock by `name` and invokes `holdWhile` while held.
   * The lock is released when the promise returned by `holdWhile` resolves
   * (or rejects, or the AbortSignal aborts). The outer promise resolves once
   * the lock has been fully released.
   */
  request(
    name: string,
    options: { signal?: AbortSignal },
    holdWhile: () => Promise<void>,
  ): Promise<void>;
}

export interface TabSupervisorOptions {
  brokerPort: SupervisorBrokerPort;
  lockName: string;
  locks: LocksBackend;
  WorkerCtor: SupervisorWorkerCtor;
  workerUrl: string | URL;
  workerOptions?: WorkerOptions;
}

type Listener = (state: TabSupervisorState) => void;

const CLAIM_LEADER = "claim-leader" as const;
const RELEASE_LEADER = "release-leader" as const;
const REQUEST_LEADER = "request-leader" as const;
const LEADER_PONG = "leader-pong" as const;
const LEADER_PING = "leader-ping" as const;
const LEADER_PORT = "leader-port" as const;
const FOLLOWER_PORT = "follower-port" as const;
const NO_LEADER = "no-leader" as const;
const LEADER_CHANGED = "leader-changed" as const;
const ATTACH_TAB_PORT = "attach-tab-port" as const;

/** Forwarded to the dedicated worker so it can register the port as a peer. */
export interface AttachTabPortMessage {
  type: typeof ATTACH_TAB_PORT;
}

/**
 * Boots a per-tab supervisor against a broker port and a {@link LocksBackend}.
 *
 * Returns once the supervisor has installed its broker-port listener and
 * issued its initial `request-leader`. Actual leader/follower transitions
 * happen asynchronously thereafter.
 */
export function createTabSupervisor(options: TabSupervisorOptions): TabSupervisor {
  const subscribers = new Set<Listener>();
  let state: TabSupervisorState = { role: "none", endpoint: null };
  let ownWorker: SupervisorWorkerLike | null = null;
  let releaseLockHold: (() => void) | null = null;
  let shutdownSignal: AbortController | null = new AbortController();
  let shutdownInvoked = false;
  let leaderClaimed = false;

  const setState = (next: TabSupervisorState): void => {
    state = next;
    for (const listener of subscribers) listener(state);
  };

  const requestLeaderFromBroker = (): void => {
    if (shutdownInvoked) return;
    options.brokerPort.postMessage({ type: REQUEST_LEADER });
  };

  const onBrokerMessage = (event: { data?: unknown; ports?: ReadonlyArray<unknown> }): void => {
    const data = event.data;
    if (typeof data !== "object" || data === null) return;
    const type = (data as { type?: unknown }).type;
    switch (type) {
      case LEADER_PORT: {
        // We're a follower; the broker just delivered a port to the leader.
        if (state.role === "leader") return;
        const port = event.ports?.[0];
        if (!port) return;
        setState({ role: "follower", endpoint: port as WorkerBridgeEndpoint });
        return;
      }
      case NO_LEADER: {
        if (state.role === "leader") return;
        setState({ role: "none", endpoint: null });
        return;
      }
      case LEADER_CHANGED: {
        // Leader migrated. Two cases for a tab in `role: "leader"`:
        //
        //  1. The broker just accepted *our* claim and broadcast to peers.
        //     The broker uses `broadcastLeaderChanged(port)` with `port`
        //     as the except-set, so we won't receive that broadcast — no
        //     work needed.
        //
        //  2. The broker evicted us as a stale leader because we missed a
        //     liveness probe (e.g. our main thread was momentarily busy
        //     and didn't reply to `leader-ping` within the broker's
        //     timeout). In that case we still hold the Web Lock and the
        //     dedicated worker, but the broker has cleared its
        //     `leaderPort`. Re-claim to put it back in sync — the claim
        //     handler is idempotent and the broker just stores the new
        //     port. Without this, followers would keep getting `no-leader`
        //     even though we're alive and own the runtime.
        if (state.role === "leader") {
          if (leaderClaimed) {
            options.brokerPort.postMessage({ type: CLAIM_LEADER });
          }
          return;
        }
        setState({ role: "none", endpoint: null });
        requestLeaderFromBroker();
        return;
      }
      case FOLLOWER_PORT: {
        // Only meaningful when we are the leader: a new follower needs to be
        // wired into our dedicated worker.
        if (state.role !== "leader" || !ownWorker) return;
        const port = event.ports?.[0];
        if (!port) return;
        ownWorker.postMessage({ type: ATTACH_TAB_PORT } satisfies AttachTabPortMessage, [
          port as Transferable,
        ]);
        return;
      }
      case LEADER_PING: {
        // The broker is probing leader liveness before minting a follower
        // channel. Ack synchronously while we still hold the Web Lock —
        // staying silent here would let the broker evict us, even though
        // we are healthy and own the dedicated worker. Pong before the
        // probe window (broker default: 250ms) elapses.
        const seq = (data as { seq?: unknown }).seq;
        if (typeof seq !== "number") return;
        if (!leaderClaimed) return;
        options.brokerPort.postMessage({ type: LEADER_PONG, seq });
        return;
      }
    }
  };

  options.brokerPort.addEventListener("message", onBrokerMessage);
  options.brokerPort.start?.();

  // Try to become leader. While held, we are leader; when released (or on
  // shutdown / abort), we step down. Other tabs queued on the lock take over.
  const lockPromise = options.locks
    .request(options.lockName, { signal: shutdownSignal.signal }, async () => {
      if (shutdownInvoked) return;
      leaderClaimed = false;
      const worker = new options.WorkerCtor(options.workerUrl, options.workerOptions);
      ownWorker = worker;
      setState({ role: "leader", endpoint: worker });
      // Claim leadership at the broker eagerly, before the dedicated worker
      // has finished its WASM bootstrap. Follower tabs that connect now will
      // get a port mapped to this worker, and their handshake messages will
      // buffer on the channel until the worker installs `onmessage`. The
      // worker's JS shim preserves `event.ports` in its pending buffer (see
      // jazz-worker.ts) and Rust's `runAsWorker` drains those ports into
      // `handle_attach_tab_port` after the runtime is Ready — so no port is
      // lost across the race.
      //
      // Eager claim is what unblocks the multi-tab pattern where the second
      // tab's `createDb()` returns before the first tab has called
      // `bridge.init()`. Without it, the first tab holds the Web Lock but
      // hasn't told the broker, and the second tab is stranded: not leader
      // (the lock is held) and not follower (no claimed leader to attach to).
      leaderClaimed = true;
      options.brokerPort.postMessage({ type: CLAIM_LEADER });
      await new Promise<void>((resolve) => {
        releaseLockHold = resolve;
      });
    })
    .catch(() => {
      // AbortSignal aborts surface here on some backends; nothing to do.
    });

  // Initial follower request so we get a port (or `no-leader`) as soon as
  // possible. If we end up winning the lock, the broker will overwrite this
  // state with `claim-leader` (no port handoff to self).
  requestLeaderFromBroker();

  const stepDown = (): void => {
    if (state.role === "leader") {
      // Only the broker needs to know about leadership transitions; if we
      // never claimed (worker never reached `init-ok`), there is nothing for
      // it to release.
      if (leaderClaimed) {
        options.brokerPort.postMessage({ type: RELEASE_LEADER });
      }
      ownWorker?.terminate?.();
      ownWorker = null;
    }
    leaderClaimed = false;
    releaseLockHold?.();
    releaseLockHold = null;
  };

  return {
    get state() {
      return state;
    },
    subscribe(listener) {
      subscribers.add(listener);
      return () => {
        subscribers.delete(listener);
      };
    },
    notifyLeaderReady() {
      // No-op. Claim happens eagerly in the lock-grant callback above; the
      // method is retained so callers (and tests) that still invoke it
      // compile, but it does no work. See the comment on
      // {@link TabSupervisor.notifyLeaderReady}.
    },
    async shutdown() {
      if (shutdownInvoked) return;
      shutdownInvoked = true;
      options.brokerPort.removeEventListener("message", onBrokerMessage);
      stepDown();
      shutdownSignal?.abort();
      shutdownSignal = null;
      setState({ role: "none", endpoint: null });
      await lockPromise;
    },
  };
}

interface NavigatorLocksLike {
  request(
    name: string,
    options: { mode?: "exclusive" | "shared"; signal?: AbortSignal },
    callback: () => Promise<unknown>,
  ): Promise<unknown>;
}

/**
 * Wraps `navigator.locks` as a {@link LocksBackend}. Returns `null` when the
 * platform does not expose the Web Locks API (server-side, or unsupported
 * browsers). Callers should fall back to memory mode in that case.
 */
export function createBrowserLocksBackend(): LocksBackend | null {
  const nav = (globalThis as { navigator?: { locks?: NavigatorLocksLike } }).navigator;
  const locks = nav?.locks;
  if (!locks || typeof locks.request !== "function") return null;
  return {
    async request(name, options, holdWhile) {
      await locks.request(name, { mode: "exclusive", signal: options.signal }, async () => {
        await holdWhile();
      });
    },
  };
}
