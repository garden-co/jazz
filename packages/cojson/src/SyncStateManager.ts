import { RawCoID } from "./ids.js";
import {
  CoValueKnownState,
  emptyKnownState,
  areLocalSessionsUploaded,
} from "./knownState.js";
import { PeerID, SyncManager } from "./sync.js";

export type SyncState = {
  uploaded: boolean;
};

export type GlobalSyncStateListenerCallback = (
  peerId: PeerID,
  knownState: CoValueKnownState,
  sync: SyncState,
) => void;

export type PeerSyncStateListenerCallback = (
  knownState: CoValueKnownState,
  sync: SyncState,
) => void;

export class SyncStateManager {
  constructor(private syncManager: SyncManager) {}

  private listeners = new Set<GlobalSyncStateListenerCallback>();
  private listenersByPeers = new Map<
    PeerID,
    Set<PeerSyncStateListenerCallback>
  >();

  subscribeToUpdates(listener: GlobalSyncStateListenerCallback) {
    this.listeners.add(listener);

    return () => {
      this.listeners.delete(listener);
    };
  }

  subscribeToPeerUpdates(
    peerId: PeerID,
    listener: PeerSyncStateListenerCallback,
  ) {
    const listeners = this.listenersByPeers.get(peerId) ?? new Set();

    if (listeners.size === 0) {
      this.listenersByPeers.set(peerId, listeners);
    }

    listeners.add(listener);

    return () => {
      listeners.delete(listener);
    };
  }

  getCurrentSyncState(peerId: PeerID, id: RawCoID) {
    return {
      uploaded: this.getIsCoValueFullyUploadedIntoPeer(peerId, id),
    };
  }

  triggerUpdate(peerId: PeerID, id: RawCoID) {
    const peer = this.syncManager.peers[peerId];

    if (!peer) {
      return;
    }

    const peerListeners = this.listenersByPeers.get(peer.id);

    // If we don't have any active listeners do nothing
    if (!peerListeners?.size && !this.listeners.size) {
      return;
    }

    const knownState = peer.getKnownState(id) ?? emptyKnownState(id);
    const syncState = this.getCurrentSyncState(peerId, id);

    for (const listener of this.listeners) {
      listener(peerId, knownState, syncState);
    }

    if (!peerListeners) return;

    for (const listener of peerListeners) {
      listener(knownState, syncState);
    }
  }

  private getIsCoValueFullyUploadedIntoPeer(peerId: PeerID, id: RawCoID) {
    const peer = this.syncManager.peers[peerId];

    if (!peer) {
      return false;
    }

    const peerSessions = peer.getKnownState(id)?.sessions;

    if (!peerSessions) {
      return false;
    }

    const entry = this.syncManager.local.getCoValue(id);

    if (!entry.isAvailable()) {
      return false;
    }

    const coValueSessions = entry.knownState().sessions;

    return areLocalSessionsUploaded(coValueSessions, peerSessions);
  }
}
