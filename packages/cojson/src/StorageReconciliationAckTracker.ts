import { PeerState } from "./PeerState.js";

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

    const finish = () => {
      if (finished) {
        return false;
      }
      finished = true;
      unsubscribeCloseListener();
      return true;
    };

    const onAck = () => {
      if (!finish()) {
        return;
      }
      callback();
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
