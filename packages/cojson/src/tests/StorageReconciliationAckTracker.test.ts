import { describe, expect, test, vi } from "vitest";
import { PeerState } from "../PeerState.js";
import { StorageReconciliationAckTracker } from "../StorageReconciliationAckTracker.js";
import { ConnectedPeerChannel } from "../streamUtils.js";
import { Peer } from "../sync.js";

function createPeerState(id = "peer-1"): PeerState {
  return createPeerStateWithChannels(id).peerState;
}

function createPeerStateWithChannels(id = "peer-1"): {
  peerState: PeerState;
  incoming: ConnectedPeerChannel;
  outgoing: ConnectedPeerChannel;
} {
  const incoming = new ConnectedPeerChannel();
  const outgoing = new ConnectedPeerChannel();

  const peer: Peer = {
    id,
    role: "server",
    persistent: true,
    incoming,
    outgoing,
  };

  return {
    peerState: new PeerState(peer, undefined),
    incoming,
    outgoing,
  };
}

describe("StorageReconciliationAckTracker", () => {
  test("tracks pending acks and returns next offset on ack", () => {
    const tracker = new StorageReconciliationAckTracker();

    tracker.trackBatch("batch-1", "peer-1", 100);

    expect(tracker.pendingReconciliationAck.get("batch-1#peer-1")).toBe(100);

    const nextOffset = tracker.handleAck("batch-1", "peer-1");

    expect(nextOffset).toBe(100);
    expect(tracker.pendingReconciliationAck.has("batch-1#peer-1")).toBe(false);
  });

  test("invokes registered callback when ack is received", () => {
    const tracker = new StorageReconciliationAckTracker();
    const peer = createPeerState("peer-1");
    const onAck = vi.fn();

    tracker.trackBatch("batch-1", peer.id, 50);
    tracker.waitForAck("batch-1", peer, onAck);

    expect(onAck).not.toHaveBeenCalled();

    tracker.handleAck("batch-1", peer.id);

    expect(onAck).toHaveBeenCalledTimes(1);
  });

  test("waits for peer outgoing queue to drain after ack before invoking callback", () => {
    vi.useFakeTimers();

    try {
      const tracker = new StorageReconciliationAckTracker();
      const { peerState, outgoing } = createPeerStateWithChannels("peer-1");
      const onAck = vi.fn();

      tracker.trackBatch("batch-1", peerState.id, 50);

      // Queue an unsent message before waiting for ack.
      outgoing.push("Disconnected");
      expect(peerState.hasUnsentMessages).toBe(true);

      tracker.waitForAck("batch-1", peerState, onAck);
      tracker.handleAck("batch-1", peerState.id);

      // Ack arrived, but callback should wait until unsent queue is empty.
      vi.advanceTimersByTime(30);
      expect(onAck).not.toHaveBeenCalled();

      // Draining the outgoing channel clears buffered unsent messages.
      outgoing.onMessage(() => {});
      vi.advanceTimersByTime(10);
      expect(onAck).toHaveBeenCalledTimes(1);
    } finally {
      vi.useRealTimers();
    }
  });

  test("invokes callback only once even if peer closes after ack", () => {
    const tracker = new StorageReconciliationAckTracker();
    const peer = createPeerState("peer-1");
    const onAck = vi.fn();

    tracker.trackBatch("batch-1", peer.id, 50);
    tracker.waitForAck("batch-1", peer, onAck);
    tracker.handleAck("batch-1", peer.id);
    peer.gracefulShutdown();

    expect(onAck).toHaveBeenCalledTimes(1);
  });

  test("invokes all listeners registered for a batch", () => {
    const tracker = new StorageReconciliationAckTracker();
    const peer = createPeerState("peer-1");
    const first = vi.fn();
    const second = vi.fn();

    tracker.trackBatch("batch-1", peer.id, 50);
    tracker.waitForAck("batch-1", peer, first);
    tracker.waitForAck("batch-1", peer, second);
    tracker.handleAck("batch-1", peer.id);

    expect(first).toHaveBeenCalledTimes(1);
    expect(second).toHaveBeenCalledTimes(1);
  });

  test("calls callback immediately when batch is not pending", () => {
    const tracker = new StorageReconciliationAckTracker();
    const peer = createPeerState("peer-1");
    const onAck = vi.fn();

    tracker.waitForAck("missing-batch", peer, onAck);

    expect(onAck).toHaveBeenCalledTimes(1);
  });

  test("aborts wait on peer close and clears pending ack", () => {
    const tracker = new StorageReconciliationAckTracker();
    const peer = createPeerState("peer-1");
    const onAck = vi.fn();

    tracker.trackBatch("batch-1", peer.id, 50);
    tracker.waitForAck("batch-1", peer, onAck);

    peer.gracefulShutdown();

    expect(onAck).not.toHaveBeenCalled();
    expect(tracker.pendingReconciliationAck.has("batch-1#peer-1")).toBe(false);
  });
});
