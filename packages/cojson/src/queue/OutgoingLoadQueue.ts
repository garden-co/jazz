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
 * A queue that manages outgoing load requests with throttling and prioritization.
 *
 * Features:
 * - Limits concurrent in-flight load requests per peer
 * - Prioritizes unavailable CoValues over already-available ones (sync requests)
 * - FIFO order within each priority tier
 * - O(1) enqueue and dequeue operations using LinkedList
 * - Manages timeouts for in-flight loads with a single timer
 */
export class OutgoingLoadQueue {
  private inFlightLoads: Map<CoValueCore, number> = new Map();
  private pendingHigh: LinkedList<PendingLoad> = new LinkedList(); // Unavailable
  private pendingLow: LinkedList<PendingLoad> = new LinkedList(); // Available
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
        logger.warn("Load request timed out", {
          id: coValue.id,
          peerId: this.peerId,
        });
        this.inFlightLoads.delete(coValue);
        coValue.markNotFoundInPeer(this.peerId);
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

  /**
   * Track that a load request has completed.
   * Triggers processing of pending requests.
   */
  trackComplete(coValue: CoValueCore): void {
    if (this.inFlightLoads.has(coValue)) {
      this.inFlightLoads.delete(coValue);
      this.processQueue();
    }
  }

  /**
   * Enqueue a load request.
   * Immediately processes the queue to send requests if capacity is available.
   *
   * @param coValue - The CoValue to load
   * @param sendCallback - Callback to send the request when ready
   */
  enqueue(coValue: CoValueCore, sendCallback: () => void): void {
    const pending: PendingLoad = { value: coValue, sendCallback };

    if (coValue.isAvailable()) {
      this.pendingLow.push(pending);
    } else {
      this.pendingHigh.push(pending);
    }
    this.processQueue();
  }

  private processing = false;
  /**
   * Process all pending load requests while capacity is available.
   * Prioritizes unavailable CoValues (high) over available ones (low).
   */
  private processQueue(): void {
    if (this.processing || !this.canSend()) {
      return;
    }
    this.processing = true;

    while (this.canSend()) {
      // Try high priority first (unavailable CoValues)
      let next = this.pendingHigh.shift();

      // Fall back to low priority (available CoValues)
      if (!next) {
        next = this.pendingLow.shift();
      }

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
    this.pendingHigh = new LinkedList();
    this.pendingLow = new LinkedList();
  }

  /**
   * Get the number of in-flight loads (for testing/debugging).
   */
  get inFlightCount(): number {
    return this.inFlightLoads.size;
  }

  /**
   * Get the number of pending high priority loads (for testing/debugging).
   */
  get pendingHighCount(): number {
    return this.pendingHigh.length;
  }

  /**
   * Get the number of pending low priority loads (for testing/debugging).
   */
  get pendingLowCount(): number {
    return this.pendingLow.length;
  }
}
