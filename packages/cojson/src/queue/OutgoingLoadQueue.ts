import { CO_VALUE_LOADING_CONFIG } from "../config.js";
import { CoValueCore } from "../exports.js";
import type { RawCoID } from "../ids.js";
import { logger } from "../logger.js";
import type { PeerID } from "../sync.js";
import { LinkedList, type LinkedListNode, meteredList } from "./LinkedList.js";

interface PendingLoad {
  value: CoValueCore;
  sendCallback: () => void;
}

/**
 * Mode for enqueuing load requests:
 * - undefined (default): normal priority, processed in order
 * - "low-priority": processed after all normal priority requests
 * - "immediate": bypasses the queue entirely, executes immediately
 */
export type LoadMode = "low-priority" | "immediate";

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
  private highPriorityPending: LinkedList<PendingLoad> = meteredList(
    "load-requests-queue",
    { priority: "high" },
  );
  private lowPriorityPending: LinkedList<PendingLoad> = meteredList(
    "load-requests-queue",
    { priority: "low" },
  );
  /**
   * Tracks nodes in the low-priority queue by CoValue ID for O(1) upgrade lookup.
   */
  private lowPriorityNodes: Map<RawCoID, LinkedListNode<PendingLoad>> =
    new Map();
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
   * @param mode - Optional mode: "low-priority" for background loads, "immediate" to bypass queue
   */
  enqueue(
    coValue: CoValueCore,
    sendCallback: () => void,
    mode?: LoadMode,
  ): void {
    // Skip if already in-flight or pending
    if (this.inFlightLoads.has(coValue) || this.requestedSet.has(coValue.id)) {
      // Check if upgrade is needed: normal/immediate priority request for a low-priority pending item
      if (mode !== "low-priority") {
        const lowPriorityNode = this.lowPriorityNodes.get(coValue.id);
        if (lowPriorityNode) {
          // Upgrade: remove from low-priority queue
          this.lowPriorityPending.remove(lowPriorityNode);
          this.lowPriorityNodes.delete(coValue.id);

          if (mode === "immediate") {
            // Execute immediately
            this.trackSent(lowPriorityNode.value.value);
            lowPriorityNode.value.sendCallback();
          } else {
            // Move to high-priority queue
            this.highPriorityPending.push(lowPriorityNode.value);
            this.processQueue();
          }
        }
      }
      return;
    }

    this.requestedSet.add(coValue.id);

    // Immediate mode bypasses the queue and sends immediately
    if (mode === "immediate") {
      this.trackSent(coValue);
      sendCallback();
      return;
    }

    const pendingLoad = { value: coValue, sendCallback };

    if (mode === "low-priority") {
      const node = this.lowPriorityPending.push(pendingLoad);
      this.lowPriorityNodes.set(coValue.id, node);
    } else {
      this.highPriorityPending.push(pendingLoad);
    }

    this.processQueue();
  }

  private processing = false;
  /**
   * Process all pending load requests while capacity is available.
   * High-priority requests are processed first, then low-priority.
   */
  private processQueue(): void {
    if (this.processing || !this.canSend()) {
      return;
    }
    this.processing = true;

    while (this.canSend()) {
      // Try high-priority first
      let next = this.highPriorityPending.shift();

      // Fall back to low-priority if high-priority is empty
      if (!next) {
        next = this.lowPriorityPending.shift();
        if (next) {
          // Remove from the tracking map since we're processing it
          this.lowPriorityNodes.delete(next.value.id);
        }
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
    this.requestedSet.clear();
    this.highPriorityPending = new LinkedList();
    this.lowPriorityPending = new LinkedList();
    this.lowPriorityNodes.clear();
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
    return this.highPriorityPending.length + this.lowPriorityPending.length;
  }

  /**
   * Get the number of high-priority pending loads (for testing/debugging).
   */
  get highPriorityPendingCount(): number {
    return this.highPriorityPending.length;
  }

  /**
   * Get the number of low-priority pending loads (for testing/debugging).
   */
  get lowPriorityPendingCount(): number {
    return this.lowPriorityPending.length;
  }
}
