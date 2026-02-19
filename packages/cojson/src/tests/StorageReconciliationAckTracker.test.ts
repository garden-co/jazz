import { describe, expect, test, vi } from "vitest";
import { PeerState } from "../PeerState.js";
import { StorageReconciliationAckTracker } from "../StorageReconciliationAckTracker.js";
import { ConnectedPeerChannel } from "../streamUtils.js";
import { Peer } from "../sync.js";

function createPeerState(id = "peer-1"): PeerState {
  const peer: Peer = {
    id,
    role: "server",
    persistent: true,
    incoming: new ConnectedPeerChannel(),
    outgoing: new ConnectedPeerChannel(),
  };

  return new PeerState(peer, undefined);
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
