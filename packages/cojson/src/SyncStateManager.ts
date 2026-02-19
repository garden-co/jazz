import { RawCoID } from "./ids.js";
import {
  CoValueKnownState,
  areCurrentSessionsInSyncWith,
} from "./knownState.js";
import { PeerState } from "./PeerState.js";
import { Peer, PeerID, SyncManager } from "./sync.js";

export type SyncState = {
  uploaded: boolean;
};

export type GlobalSyncStateListenerCallback = (
  peer: Peer,
  knownState: CoValueKnownState,
  sync: SyncState,
) => void;

export type CoValueSyncStateListenerCallback = GlobalSyncStateListenerCallback;

export type PeerSyncStateListenerCallback = (
  knownState: CoValueKnownState,
  sync: SyncState,
) => void;

export class SyncStateManager {
  constructor(private syncManager: SyncManager) {}

  private listeners = new Set<GlobalSyncStateListenerCallback>();
  private listenersByCoValues = new Map<
    RawCoID,
    Set<CoValueSyncStateListenerCallback>
  >();
  private listenersByPeersAndCoValues = new Map<
    PeerID,
    Map<RawCoID, Set<PeerSyncStateListenerCallback>>
  >();

  subscribeToUpdates(listener: GlobalSyncStateListenerCallback) {
    this.listeners.add(listener);

    return () => {
      this.listeners.delete(listener);
    };
  }

  subscribeToCoValueUpdates(
    coValueId: RawCoID,
    listener: CoValueSyncStateListenerCallback,
  ) {
    let listeners = this.listenersByCoValues.get(coValueId);
    if (!listeners) {
      listeners = new Set();
      this.listenersByCoValues.set(coValueId, listeners);
    }
    listeners.add(listener);

    return () => {
      listeners.delete(listener);
      if (listeners.size === 0) {
        this.listenersByCoValues.delete(coValueId);
      }
    };
  }

  subscribeToPeerUpdates(
    peerId: PeerID,
    coValueId: RawCoID,
    listener: PeerSyncStateListenerCallback,
  ) {
    let peerMap = this.listenersByPeersAndCoValues.get(peerId);
    if (!peerMap) {
      peerMap = new Map();
      this.listenersByPeersAndCoValues.set(peerId, peerMap);
    }

    let listeners = peerMap.get(coValueId);
    if (!listeners) {
      listeners = new Set();
      peerMap.set(coValueId, listeners);
    }

    listeners.add(listener);

    return () => {
      listeners.delete(listener);
      if (listeners.size === 0) {
        peerMap.delete(coValueId);
        if (peerMap.size === 0) {
          this.listenersByPeersAndCoValues.delete(peerId);
        }
      }
    };
  }

  triggerUpdate(peer: Peer, id: RawCoID, knownState: CoValueKnownState) {
    const globalListeners = this.listeners;
    const coValueListeners = this.listenersByCoValues.get(id);
    const peerMap = this.listenersByPeersAndCoValues.get(peer.id);
    const coValueAndPeerListeners = peerMap?.get(id);

    if (
      !globalListeners.size &&
      !coValueListeners?.size &&
      !coValueAndPeerListeners?.size
    ) {
      // If we don't have any active listeners do nothing
      return;
    }

    const syncState = {
      uploaded: this.getIsCoValueFullyUploadedIntoPeer(knownState, id),
    };

    for (const listener of this.listeners) {
      listener(peer, knownState, syncState);
    }

    if (coValueListeners) {
      for (const listener of coValueListeners) {
        listener(peer, knownState, syncState);
      }
    }

    if (coValueAndPeerListeners) {
      for (const listener of coValueAndPeerListeners) {
        listener(knownState, syncState);
      }
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

    const knownState = entry.knownState();

    if (!knownState.header) {
      return false;
    }

    return areCurrentSessionsInSyncWith(
      knownState.sessions,
      peerKnownState.sessions,
    );
  }
}
