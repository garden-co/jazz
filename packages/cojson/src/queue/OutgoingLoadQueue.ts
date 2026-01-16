import { CO_VALUE_LOADING_CONFIG } from "../config.js";
import { CoValueCore } from "../exports.js";
import { logger } from "../logger.js";
import type { PeerID } from "../sync.js";
import { LinkedList } from "./LinkedList.js";

interface PendingLoad {
  value: CoValueCore;
  sendCallback: () => void;
}

/**
 * A queue that manages outgoing load requests with throttling.
 *
 * Features:
 * - Limits concurrent in-flight load requests per peer
 * - FIFO order for pending requests
 * - O(1) enqueue and dequeue operations using LinkedList
 * - Manages timeouts for in-flight loads with a single timer
 */
export class OutgoingLoadQueue {
  private inFlightLoads: Map<CoValueCore, number> = new Map();
  private pending: LinkedList<PendingLoad> = new LinkedList();
  private requestedSet: Set<CoValueCore["id"]> = new Set();
  private timeoutHandle: ReturnType<typeof setTimeout> | null = null;

  constructor(private peerId: PeerID) {}

  /**
   * Check if we can send another load request.
   */
  private canSend(): boolean {
    return (
      this.inFlightLoads.size <
      CO_VALUE_LOADING_CONFIG.MAX_IN_FLIGHT_LOADS_PER_PEER
    );
  }

  /**
   * Track that a load request has been sent.
   */
  private trackSent(coValue: CoValueCore): void {
    const now = performance.now();
    this.inFlightLoads.set(coValue, now);
    this.scheduleTimeoutCheck(CO_VALUE_LOADING_CONFIG.TIMEOUT);
  }

  /**
   * Schedule a timeout check if not already scheduled.
   * Uses a single timer to check all in-flight loads.
   */
  private scheduleTimeoutCheck(nextTimeout: number): void {
    if (this.timeoutHandle !== null) {
      return;
    }

    this.timeoutHandle = setTimeout(() => {
      this.timeoutHandle = null;
      this.checkTimeouts();
    }, nextTimeout);
  }

  /**
   * Check all in-flight loads for timeouts and handle them.
   */
  private checkTimeouts(): void {
    const now = performance.now();

    let nextTimeout: number | undefined;
    for (const [coValue, sentAt] of this.inFlightLoads.entries()) {
      const timeout = sentAt + CO_VALUE_LOADING_CONFIG.TIMEOUT;

      if (now >= timeout) {
        if (!coValue.isAvailable()) {
          logger.warn("Load request timed out", {
            id: coValue.id,
            peerId: this.peerId,
          });
          coValue.markNotFoundInPeer(this.peerId);
        } else if (coValue.isStreaming()) {
          logger.warn(
            "Content streaming is taking more than " +
              CO_VALUE_LOADING_CONFIG.TIMEOUT / 1000 +
              "s",
            {
              id: coValue.id,
              peerId: this.peerId,
              knownState: coValue.knownState().sessions,
              streamingTarget: coValue.knownStateWithStreaming().sessions,
            },
          );
        }

        this.inFlightLoads.delete(coValue);
        this.requestedSet.delete(coValue.id);
        this.processQueue();
      } else {
        nextTimeout = Math.min(nextTimeout ?? Infinity, timeout - now);
      }
    }

    // Reschedule if there are still in-flight loads
    if (nextTimeout) {
      this.scheduleTimeoutCheck(nextTimeout);
    }
  }

  trackUpdate(coValue: CoValueCore): void {
    if (!this.inFlightLoads.has(coValue)) {
      return;
    }

    // Refresh the timeout for the in-flight load
    this.inFlightLoads.set(coValue, performance.now());
  }

  /**
   * Track that a load request has completed.
   * Triggers processing of pending requests.
   */
  trackComplete(coValue: CoValueCore): void {
    if (!this.inFlightLoads.has(coValue)) {
      return;
    }

    if (coValue.isStreaming()) {
      // wait for the next chunk
      return;
    }

    this.inFlightLoads.delete(coValue);
    this.requestedSet.delete(coValue.id);
    this.processQueue();
  }

  /**
   * Enqueue a load request.
   * Immediately processes the queue to send requests if capacity is available.
   * Skips CoValues that are already in-flight or pending.
   *
   * @param coValue - The CoValue to load
   * @param sendCallback - Callback to send the request when ready
   * @param allowOverflow - If true, send immediately bypassing the capacity limit (used for dependencies)
   */
  enqueue(
    coValue: CoValueCore,
    sendCallback: () => void,
    allowOverflow?: boolean,
  ): void {
    // Skip if already in-flight or pending
    if (this.inFlightLoads.has(coValue) || this.requestedSet.has(coValue.id)) {
      return;
    }

    this.requestedSet.add(coValue.id);

    // Dependencies bypass the queue and send immediately
    if (allowOverflow) {
      this.trackSent(coValue);
      sendCallback();
      return;
    }

    this.pending.push({ value: coValue, sendCallback });
    this.processQueue();
  }

  private processing = false;
  /**
   * Process all pending load requests while capacity is available.
   */
  private processQueue(): void {
    if (this.processing || !this.canSend()) {
      return;
    }
    this.processing = true;

    while (this.canSend()) {
      const next = this.pending.shift();

      if (!next) {
        break;
      }

      this.trackSent(next.value);
      next.sendCallback();
    }

    this.processing = false;
  }

  /**
   * Clear all state. Called on disconnect.
   * Clears the timeout and all pending/in-flight loads.
   */
  clear(): void {
    if (this.timeoutHandle !== null) {
      clearTimeout(this.timeoutHandle);
      this.timeoutHandle = null;
    }
    this.inFlightLoads.clear();
    this.requestedSet.clear();
    this.pending = new LinkedList();
  }

  /**
   * Get the number of in-flight loads (for testing/debugging).
   */
  get inFlightCount(): number {
    return this.inFlightLoads.size;
  }

  /**
   * Get the number of pending loads (for testing/debugging).
   */
  get pendingCount(): number {
    return this.pending.length;
  }
}
