/**
 * SharedWorker broker.
 *
 * The broker is a thin `MessagePort` relay. It does **not** host the durable
 * runtime, OPFS handles, or the upstream socket — those live in a dedicated
 * `Worker` owned by whichever tab currently holds the
 * `navigator.locks` leader lease. The broker only:
 *
 *   - tracks which connected tab port is the current leader,
 *   - on a follower's `request-leader`, mints a `MessageChannel`, transfers
 *     one end to the follower and the other end to the leader, so the two
 *     tabs end up with a direct port to the leader's runtime worker,
 *   - broadcasts `leader-changed` whenever the leader claim transitions, so
 *     followers know to drop stale ports and request fresh ones.
 *
 * The runtime never flows through the broker — only port handoff control
 * messages and the transferred `MessagePort`s themselves do.
 *
 * See `specs/todo/b_launch/leader_tab_runtime.md` for the full design.
 */

export type TabToBrokerMessage =
  | { type: "claim-leader" }
  | { type: "release-leader" }
  | { type: "request-leader" }
  | { type: "leader-pong"; seq: number };

export type BrokerToTabMessage =
  | { type: "leader-port" }
  | { type: "follower-port" }
  | { type: "no-leader" }
  | { type: "leader-changed" }
  | { type: "leader-ping"; seq: number };

const CLAIM_LEADER = "claim-leader" as const;
const RELEASE_LEADER = "release-leader" as const;
const REQUEST_LEADER = "request-leader" as const;
const LEADER_PONG = "leader-pong" as const;
const LEADER_PORT = "leader-port" as const;
const FOLLOWER_PORT = "follower-port" as const;
const NO_LEADER = "no-leader" as const;
const LEADER_CHANGED = "leader-changed" as const;
const LEADER_PING = "leader-ping" as const;

/**
 * Round-trip budget for the broker's leader-liveness probe.
 *
 * Chrome reuses a `SharedWorker` across page reloads of the same origin,
 * which means a dev-server restart (or any reload that doesn't drain the
 * worker) leaves the broker holding a `leaderPort` whose tab is gone. The
 * usual `postMessage` does not throw synchronously on a port whose owning
 * context has been torn down, so `safePost` is not sufficient to detect
 * the stale leader. Before minting a follower channel we therefore ping
 * the leader and require a pong within this window — otherwise we evict
 * and let the next `claim-leader` (the new page that just won the Web
 * Lock) take over.
 *
 * 750 ms is generous enough to absorb a leader main-thread that's
 * processing a tight insert loop or a brief GC pause without false
 * eviction, and short enough that the dev-server-restart scenario
 * surfaces well before the bridge-init timeout. A false eviction is
 * self-correcting — the still-alive leader re-claims on receiving
 * `leader-changed` (see {@link createTabSupervisor}) — so this timeout
 * controls a worst-case stall, not data correctness.
 */
const LEADER_PROBE_TIMEOUT_MS = 750;

export interface SharedWorkerBrokerGlobal {
  onconnect: ((event: { ports: MessagePortLike[] }) => void) | null;
}

export interface MessagePortLike {
  postMessage(message: unknown, transfer?: Transferable[]): void;
  addEventListener(
    type: "message" | "messageerror",
    listener: (event: { data?: unknown }) => void,
  ): void;
  removeEventListener(
    type: "message" | "messageerror",
    listener: (event: { data?: unknown }) => void,
  ): void;
  start?(): void;
  close?(): void;
}

export interface MessageChannelLike {
  port1: MessagePortLike;
  port2: MessagePortLike;
}

export interface MessageChannelCtor {
  new (): MessageChannelLike;
}

export interface SharedWorkerBrokerOptions {
  /** Defaults to `globalThis.MessageChannel`; injected for tests. */
  MessageChannelCtor?: MessageChannelCtor;
}

function isTabToBrokerMessage(value: unknown): value is TabToBrokerMessage {
  if (typeof value !== "object" || value === null) return false;
  const type = (value as { type?: unknown }).type;
  if (type === CLAIM_LEADER || type === RELEASE_LEADER || type === REQUEST_LEADER) {
    return true;
  }
  if (type === LEADER_PONG && typeof (value as { seq?: unknown }).seq === "number") {
    return true;
  }
  return false;
}

export function installSharedWorkerBroker(
  target: SharedWorkerBrokerGlobal,
  options: SharedWorkerBrokerOptions = {},
): void {
  const MessageChannelCtor =
    options.MessageChannelCtor ??
    (globalThis as { MessageChannel?: MessageChannelCtor }).MessageChannel;
  if (!MessageChannelCtor) {
    throw new Error("SharedWorker broker requires MessageChannel support");
  }

  const allPorts = new Set<MessagePortLike>();
  let leaderPort: MessagePortLike | null = null;
  let probeSeq = 0;
  const pendingProbes = new Map<
    number,
    { resolve: (alive: boolean) => void; timer: ReturnType<typeof setTimeout> }
  >();

  /**
   * Posting to a `MessagePort` whose owning context is gone throws (Chrome:
   * `InvalidStateError`; some browsers silently drop). Wrap every broker
   * post so we can detect this and treat the port as dead — critically, if
   * the dying port was the current leader, we clear `leaderPort` so the
   * next claimer can take over and so followers can be re-broadcast.
   */
  const safePost = (
    port: MessagePortLike,
    message: BrokerToTabMessage,
    transfer?: Transferable[],
  ): boolean => {
    try {
      if (transfer) port.postMessage(message, transfer);
      else port.postMessage(message);
      return true;
    } catch {
      return false;
    }
  };

  const dropPort = (port: MessagePortLike): void => {
    allPorts.delete(port);
    if (leaderPort === port) leaderPort = null;
  };

  const broadcastLeaderChanged = (except: MessagePortLike | null): void => {
    const stale: MessagePortLike[] = [];
    for (const port of allPorts) {
      if (port === except) continue;
      if (!safePost(port, { type: LEADER_CHANGED })) stale.push(port);
    }
    for (const port of stale) dropPort(port);
  };

  const handleClaim = (port: MessagePortLike): void => {
    const previousLeader = leaderPort;
    leaderPort = port;
    if (previousLeader === port) return;
    broadcastLeaderChanged(port);
  };

  const handleRelease = (port: MessagePortLike): void => {
    if (leaderPort !== port) return;
    leaderPort = null;
    broadcastLeaderChanged(null);
  };

  const mintAndDeliver = (port: MessagePortLike): void => {
    if (!leaderPort) {
      safePost(port, { type: NO_LEADER });
      return;
    }
    const channel = new MessageChannelCtor();
    const deliveredToFollower = safePost(port, { type: LEADER_PORT }, [
      channel.port1 as unknown as Transferable,
    ]);
    const deliveredToLeader = safePost(leaderPort, { type: FOLLOWER_PORT }, [
      channel.port2 as unknown as Transferable,
    ]);
    if (!deliveredToLeader) {
      // Leader port is dead. Clear and notify so the next claim can take
      // over. The follower already has its half-channel to nowhere; the
      // leader-changed broadcast tells it to discard and re-request.
      const dead = leaderPort;
      leaderPort = null;
      dropPort(dead);
      broadcastLeaderChanged(null);
    }
    if (!deliveredToFollower) {
      dropPort(port);
    }
  };

  const probeLeader = (target: MessagePortLike): Promise<boolean> =>
    new Promise<boolean>((resolve) => {
      const seq = ++probeSeq;
      const timer = setTimeout(() => {
        if (pendingProbes.delete(seq)) resolve(false);
      }, LEADER_PROBE_TIMEOUT_MS);
      pendingProbes.set(seq, { resolve, timer });
      const posted = safePost(target, { type: LEADER_PING, seq });
      if (!posted) {
        clearTimeout(timer);
        pendingProbes.delete(seq);
        resolve(false);
      }
    });

  const handlePong = (port: MessagePortLike, seq: number): void => {
    // Only pongs from the current leader count; stragglers from a port
    // we've already evicted must not revive it.
    if (leaderPort !== port) return;
    const pending = pendingProbes.get(seq);
    if (!pending) return;
    pendingProbes.delete(seq);
    clearTimeout(pending.timer);
    pending.resolve(true);
  };

  const handleRequest = (port: MessagePortLike): void => {
    const probeTarget = leaderPort;
    if (!probeTarget) {
      safePost(port, { type: NO_LEADER });
      return;
    }
    if (probeTarget === port) {
      // Leader's own tab asking for a port to itself is a programming error
      // upstream — leaders talk to their worker directly, not via the broker.
      // Respond with `no-leader` rather than wiring a self-loop.
      safePost(port, { type: NO_LEADER });
      return;
    }
    // Probe leader liveness before minting. A SharedWorker that survived
    // a dev-server restart can hold a `leaderPort` whose owning tab is
    // gone; `safePost` would still return true (no synchronous throw),
    // and the new follower would attach to a `MessageChannel` whose other
    // end nobody reads. Evict instead.
    void probeLeader(probeTarget).then((alive) => {
      if (leaderPort !== probeTarget) {
        // Leadership transitioned during the probe (e.g. release-leader,
        // or a fresh claim). Re-run against the current state — either we
        // mint against the new leader or fall through to no-leader.
        handleRequest(port);
        return;
      }
      if (!alive) {
        leaderPort = null;
        dropPort(probeTarget);
        broadcastLeaderChanged(null);
        safePost(port, { type: NO_LEADER });
        return;
      }
      mintAndDeliver(port);
    });
  };

  const adoptPort = (port: MessagePortLike): void => {
    allPorts.add(port);
    port.addEventListener("message", (event) => {
      const data = event.data;
      if (!isTabToBrokerMessage(data)) return;
      switch (data.type) {
        case CLAIM_LEADER:
          handleClaim(port);
          return;
        case RELEASE_LEADER:
          handleRelease(port);
          return;
        case REQUEST_LEADER:
          handleRequest(port);
          return;
        case LEADER_PONG:
          handlePong(port, data.seq);
          return;
      }
    });
    port.start?.();
  };

  target.onconnect = (event): void => {
    const port = event.ports[0];
    if (!port) return;
    adoptPort(port);
  };
}
