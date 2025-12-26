import type { RawCoID } from "./ids.js";
import { logger } from "./logger.js";
import type { PeerID } from "./sync.js";
import type { StorageAPI } from "./storage/types.js";

/**
 * Tracks CoValues that have unsynced changes to specific peers.
 * Maintains an in-memory map and periodically persists to storage.
 */
export class UnsyncedCoValuesTracker {
  private unsynced: Map<RawCoID, Set<PeerID>> = new Map();
  private coValueListeners: Map<RawCoID, Set<(synced: boolean) => void>> =
    new Map();
  // Listeners for global "all synced" status changes
  private globalListeners: Set<(synced: boolean) => void> = new Set();

  constructor(private storage?: StorageAPI) {}

  /**
   * Add a CoValue as unsynced to a specific peer.
   * Triggers persistence if storage is available.
   */
  add(id: RawCoID, peerId: PeerID): void {
    if (!this.unsynced.has(id)) {
      this.unsynced.set(id, new Set());
    }
    const peerSet = this.unsynced.get(id)!;

    // Only update if this is a new peer
    if (!peerSet.has(peerId)) {
      peerSet.add(peerId);

      this.persist(id, peerId, false);

      this.notifyCoValueListeners(id, false);
      this.notifyGlobalListeners(false);
    }
  }

  /**
   * Remove a CoValue from being unsynced to a specific peer.
   * Triggers persistence if storage is available.
   */
  remove(id: RawCoID, peerId: PeerID): void {
    const peerSet = this.unsynced.get(id);
    if (!peerSet || !peerSet.has(peerId)) {
      return;
    }

    peerSet.delete(peerId);

    // If no more unsynced peers for this CoValue, remove the entry
    if (peerSet.size === 0) {
      this.unsynced.delete(id);
    }

    this.persist(id, peerId, true);

    const isSynced = !this.unsynced.has(id);
    this.notifyCoValueListeners(id, isSynced);
    this.notifyGlobalListeners(this.isAllSynced());
  }

  private persist(id: RawCoID, peerId: PeerID, synced: boolean): void {
    if (this.storage) {
      try {
        this.storage.trackCoValueSyncStatus(id, peerId, synced);
      } catch (error) {
        logger.warn("Failed to persist unsynced CoValue tracking", {
          err: error,
        });
      }
    }
  }

  /**
   * Get all CoValue IDs that have at least one unsynced peer.
   */
  getAll(): RawCoID[] {
    return Array.from(this.unsynced.keys());
  }

  /**
   * Check if all CoValues are synced (O(1)).
   */
  isAllSynced(): boolean {
    return this.unsynced.size === 0;
  }

  /**
   * Subscribe to changes in whether a specific CoValue is synced.
   * The listener is called immediately with the current state.
   * @returns Unsubscribe function
   */
  subscribe(id: RawCoID, listener: (synced: boolean) => void): () => void;
  /**
   * Subscribe to changes in whether all CoValues are synced.
   * The listener is called immediately with the current state.
   * @returns Unsubscribe function
   */
  subscribe(listener: (synced: boolean) => void): () => void;
  subscribe(
    idOrListener: RawCoID | ((synced: boolean) => void),
    listener?: (synced: boolean) => void,
  ): () => void {
    if (typeof idOrListener === "string" && listener) {
      const id = idOrListener;
      if (!this.coValueListeners.has(id)) {
        this.coValueListeners.set(id, new Set());
      }
      this.coValueListeners.get(id)!.add(listener);

      // Call immediately with current state
      const isSynced = !this.unsynced.has(id);
      listener(isSynced);

      return () => {
        const listeners = this.coValueListeners.get(id);
        if (listeners) {
          listeners.delete(listener);
          if (listeners.size === 0) {
            this.coValueListeners.delete(id);
          }
        }
      };
    }

    const globalListener = idOrListener as (synced: boolean) => void;
    this.globalListeners.add(globalListener);

    // Call immediately with current state
    globalListener(this.isAllSynced());

    return () => {
      this.globalListeners.delete(globalListener);
    };
  }

  /**
   * Notify all listeners for a specific CoValue about sync status change.
   */
  private notifyCoValueListeners(id: RawCoID, synced: boolean): void {
    const listeners = this.coValueListeners.get(id);
    if (listeners) {
      for (const listener of listeners) {
        listener(synced);
      }
    }
  }

  /**
   * Notify all global listeners about "all synced" status change.
   */
  private notifyGlobalListeners(allSynced: boolean): void {
    for (const listener of this.globalListeners) {
      listener(allSynced);
    }
  }
}
