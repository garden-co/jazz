import { RawCoID } from "./ids.js";
import { CoValueKnownState, areLocalSessionsUploaded } from "./knownState.js";
import { PeerState } from "./PeerState.js";
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

  triggerUpdate(peerId: PeerID, id: RawCoID, knownState: CoValueKnownState) {
    const peerListeners = this.listenersByPeers.get(peerId);

    // If we don't have any active listeners do nothing
    if (!peerListeners?.size && !this.listeners.size) {
      return;
    }

    const syncState = {
      uploaded: this.getIsCoValueFullyUploadedIntoPeer(knownState, id),
    };

    for (const listener of this.listeners) {
      listener(peerId, knownState, syncState);
    }

    if (!peerListeners) return;

    for (const listener of peerListeners) {
      listener(knownState, syncState);
    }
  }

  isSynced(peer: PeerState, id: RawCoID) {
    const peerKnownState = peer.getKnownState(id);

    if (!peerKnownState) return false;

    return this.getIsCoValueFullyUploadedIntoPeer(peerKnownState, id);
  }

  private getIsCoValueFullyUploadedIntoPeer(
    peerKnownState: CoValueKnownState,
    id: RawCoID,
  ) {
    const entry = this.syncManager.local.getCoValue(id);

    if (!entry.hasVerifiedContent()) {
      return false;
    }

    // Accessing verified knownState to skip the immutability
    // applied on CoValueCore
    const knownState = entry.verified.knownState();

    return areLocalSessionsUploaded(
      knownState.sessions,
      peerKnownState.sessions,
    );
  }
}
