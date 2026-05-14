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
  | { type: "request-leader" };

export type BrokerToTabMessage =
  | { type: "leader-port" }
  | { type: "follower-port" }
  | { type: "no-leader" }
  | { type: "leader-changed" };

const CLAIM_LEADER = "claim-leader" as const;
const RELEASE_LEADER = "release-leader" as const;
const REQUEST_LEADER = "request-leader" as const;
const LEADER_PORT = "leader-port" as const;
const FOLLOWER_PORT = "follower-port" as const;
const NO_LEADER = "no-leader" as const;
const LEADER_CHANGED = "leader-changed" as const;

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
  return type === CLAIM_LEADER || type === RELEASE_LEADER || type === REQUEST_LEADER;
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

  const handleRequest = (port: MessagePortLike): void => {
    if (!leaderPort) {
      safePost(port, { type: NO_LEADER });
      return;
    }
    if (leaderPort === port) {
      // Leader's own tab asking for a port to itself is a programming error
      // upstream — leaders talk to their worker directly, not via the broker.
      // Respond with `no-leader` rather than wiring a self-loop.
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
