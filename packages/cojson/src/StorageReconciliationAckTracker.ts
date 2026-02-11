import { PeerState } from "./PeerState.js";

const OUTGOING_QUEUE_POLL_INTERVAL_MS = 10;

export class StorageReconciliationAckTracker {
  /**
   * Tracks pending reconcile acks: "batchId#peerId->offset".
   * Cleared in handleAck.
   */
  pendingReconciliationAck: Map<string, number> = new Map();
  private batchAckListeners: Map<string, Set<() => void>> = new Map();

  trackBatch(batchId: string, peerId: string, nextOffset: number): void {
    const key = this.key(batchId, peerId);
    this.pendingReconciliationAck.set(key, nextOffset);
    if (!this.batchAckListeners.has(key)) {
      this.batchAckListeners.set(key, new Set());
    }
  }

  handleAck(batchId: string, peerId: string): number | undefined {
    const key = this.key(batchId, peerId);
    const nextOffset = this.pendingReconciliationAck.get(key);

    this.pendingReconciliationAck.delete(key);

    const listeners = this.batchAckListeners.get(key);
    if (listeners) {
      for (const listener of listeners) {
        listener();
      }
      this.batchAckListeners.delete(key);
    }

    return nextOffset;
  }

  waitForAck(batchId: string, peer: PeerState, callback: () => void): void {
    const key = this.key(batchId, peer.id);
    const listeners = this.batchAckListeners.get(key);
    if (!this.pendingReconciliationAck.has(key) || !listeners) {
      callback();
      return;
    }

    let finished = false;
    let unsubscribeCloseListener: () => void = () => {};
    let drainTimer: ReturnType<typeof setTimeout> | undefined;

    const finish = () => {
      if (finished) {
        return false;
      }
      finished = true;
      if (drainTimer !== undefined) {
        clearTimeout(drainTimer);
        drainTimer = undefined;
      }
      unsubscribeCloseListener();
      return true;
    };

    const waitForOutgoingQueueToDrain = () => {
      if (finished) {
        return;
      }

      if (peer.closed) {
        onPeerClose();
        return;
      }

      if (!peer.hasUnsentMessages) {
        if (!finish()) {
          return;
        }
        callback();
        return;
      }

      drainTimer = setTimeout(
        waitForOutgoingQueueToDrain,
        OUTGOING_QUEUE_POLL_INTERVAL_MS,
      );
    };

    const onAck = () => {
      waitForOutgoingQueueToDrain();
    };

    const onPeerClose = () => {
      if (!finish()) {
        return;
      }
      listeners.delete(onAck);
      this.pendingReconciliationAck.delete(key);
      if (listeners.size === 0) {
        this.batchAckListeners.delete(key);
      }
    };

    listeners.add(onAck);
    unsubscribeCloseListener = peer.addCloseListener(onPeerClose);
  }

  private key(batchId: string, peerId: string): string {
    return `${batchId}#${peerId}`;
  }
}
