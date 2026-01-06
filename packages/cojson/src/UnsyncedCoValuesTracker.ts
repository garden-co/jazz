import type { RawCoID } from "./ids.js";
import { logger } from "./logger.js";
import type { PeerID } from "./sync.js";
import type { StorageAPI } from "./storage/types.js";

/**
 * Used to track a CoValue that hasn't been synced to any peer,
 * because none is currently connected.
 */
const ANY_PEER_ID: PeerID = "any";

// Flush pending updates to storage after 200ms
let BATCH_DELAY_MS = 200;

/**
 * Set the delay for flushing pending sync state updates to storage.
 * @internal
 */
export function setSyncStateTrackingBatchDelay(delay: number): void {
  BATCH_DELAY_MS = delay;
}

type PendingUpdate = {
  id: RawCoID;
  peerId: PeerID;
  synced: boolean;
};

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

  // Pending updates to be persisted
  private pendingUpdates: PendingUpdate[] = [];
  private flushTimer: ReturnType<typeof setTimeout> | undefined;

  private storage?: StorageAPI;

  /**
   * Add a CoValue as unsynced to a specific peer.
   * Triggers persistence if storage is available.
   * @returns true if the CoValue was already tracked, false otherwise.
   */
  add(id: RawCoID, peerId: PeerID = ANY_PEER_ID): boolean {
    if (!this.unsynced.has(id)) {
      this.unsynced.set(id, new Set());
    }
    const peerSet = this.unsynced.get(id)!;

    const alreadyTracked = peerSet.has(peerId);
    if (!alreadyTracked) {
      // Only update if this is a new peer
      peerSet.add(peerId);

      this.schedulePersist(id, peerId, false);

      this.notifyCoValueListeners(id, false);
      this.notifyGlobalListeners(false);
    }

    return alreadyTracked;
  }

  /**
   * Remove a CoValue from being unsynced to a specific peer.
   * Triggers persistence if storage is available.
   */
  remove(id: RawCoID, peerId: PeerID = ANY_PEER_ID): void {
    const peerSet = this.unsynced.get(id);
    if (!peerSet || !peerSet.has(peerId)) {
      return;
    }

    peerSet.delete(peerId);

    // If no more unsynced peers for this CoValue, remove the entry
    if (peerSet.size === 0) {
      this.unsynced.delete(id);
    }

    this.schedulePersist(id, peerId, true);

    const isSynced = !this.unsynced.has(id);
    this.notifyCoValueListeners(id, isSynced);
    this.notifyGlobalListeners(this.isAllSynced());
  }

  private schedulePersist(id: RawCoID, peerId: PeerID, synced: boolean): void {
    const storage = this.storage;
    if (!storage) {
      return;
    }

    this.pendingUpdates.push({ id, peerId, synced });
    if (!this.flushTimer) {
      this.flushTimer = setTimeout(() => {
        this.flush();
      }, BATCH_DELAY_MS);
    }
  }

  /**
   * Flush all pending persistence updates in a batch
   */
  private flush(): void {
    if (this.flushTimer) {
      clearTimeout(this.flushTimer);
      this.flushTimer = undefined;
    }

    if (this.pendingUpdates.length === 0) {
      return;
    }

    const storage = this.storage;
    if (!storage) {
      return;
    }

    const filteredUpdates = this.simplifyPendingUpdates(this.pendingUpdates);
    this.pendingUpdates = [];

    if (filteredUpdates.length === 0) {
      return;
    }

    try {
      storage.trackCoValuesSyncState(filteredUpdates);
    } catch (error) {
      logger.warn("Failed to persist batched unsynced CoValue tracking", {
        err: error,
      });
    }
  }

  /**
   * Get all CoValue IDs that have at least one unsynced peer.
   */
  getAll(): RawCoID[] {
    return Array.from(this.unsynced.keys());
  }

  /**
   * Check if all CoValues are synced
   */
  isAllSynced(): boolean {
    return this.unsynced.size === 0;
  }

  /**
   * Check if a specific CoValue is tracked as unsynced.
   */
  has(id: RawCoID): boolean {
    return this.unsynced.has(id);
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

  setStorage(storage: StorageAPI) {
    this.storage = storage;
  }

  removeStorage() {
    this.storage = undefined;
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

  /**
   * Keep only the last update for each (id, peerId) combination
   */
  private simplifyPendingUpdates(updates: PendingUpdate[]): PendingUpdate[] {
    const latestUpdates = new Map<string, PendingUpdate>();
    for (const update of updates) {
      latestUpdates.set(`${update.id}|${update.peerId}`, update);
    }
    return Array.from(latestUpdates.values());
  }
}
