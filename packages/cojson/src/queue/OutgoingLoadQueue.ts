import { UpDownCounter, ValueType, metrics } from "@opentelemetry/api";
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

interface InFlightLoad {
  value: CoValueCore;
  sentAt: number;
}

/**
 * Mode for enqueuing load requests:
 * - "high-priority" (default): high priority, processed in order
 * - "low-priority": processed after all high priority requests
 * - "immediate": bypasses the queue entirely, executes immediately
 */
export type LoadMode = "low-priority" | "immediate" | "high-priority";
export type LoadCompletionSource = "content" | "known";

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
  private inFlightLoads: Map<RawCoID, InFlightLoad> = new Map();
  private inFlightCounter: UpDownCounter;
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
  /**
   * Tracks nodes in the high-priority queue by CoValue ID for O(1) immediate mode lookup.
   */
  private highPriorityNodes: Map<RawCoID, LinkedListNode<PendingLoad>> =
    new Map();
  private timeoutHandle: ReturnType<typeof setTimeout> | null = null;

  constructor(private peerId: PeerID) {
    this.inFlightCounter = metrics
      .getMeter("cojson")
      .createUpDownCounter("jazz.loadqueue.outgoing.inflight", {
        description: "Number of in-flight outgoing load requests",
        unit: "1",
        valueType: ValueType.INT,
      });

    // Emit an initial 0 value so the series appears immediately.
    this.inFlightCounter.add(0);
  }

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
    this.inFlightLoads.set(coValue.id, { value: coValue, sentAt: now });
    this.inFlightCounter.add(1);
    this.scheduleTimeoutCheck(CO_VALUE_LOADING_CONFIG.TIMEOUT);
  }

  private untrackInFlight(id: RawCoID): boolean {
    if (!this.inFlightLoads.delete(id)) {
      return false;
    }

    this.inFlightCounter.add(-1);
    return true;
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
    for (const { value: coValue, sentAt } of this.inFlightLoads.values()) {
      const timeout = sentAt + CO_VALUE_LOADING_CONFIG.TIMEOUT;

      if (now >= timeout) {
        if (!coValue.isAvailable()) {
          logger.warn("Load request timed out", {
            id: coValue.id,
            peerId: this.peerId,
          });
          // Re-resolve by ID to avoid mutating a stale CoValue instance.
          coValue.node.getCoValue(coValue.id).markNotFoundInPeer(this.peerId);
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

        if (this.untrackInFlight(coValue.id)) {
          this.processQueue();
        }
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
    if (!this.inFlightLoads.has(coValue.id)) {
      return;
    }

    // Refresh the timeout for the in-flight load
    this.inFlightLoads.set(coValue.id, {
      value: coValue,
      sentAt: performance.now(),
    });
  }

  /**
   * Track that a load request has completed.
   * Triggers processing of pending requests.
   */
  trackComplete(
    coValue: CoValueCore,
    source: LoadCompletionSource = "content",
  ): void {
    if (!this.inFlightLoads.has(coValue.id)) {
      return;
    }

    if (source === "content" && coValue.isStreaming()) {
      // wait for the next chunk
      return;
    }

    if (this.untrackInFlight(coValue.id)) {
      this.processQueue();
    }
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
    value: CoValueCore,
    sendCallback: () => void,
    mode: LoadMode = "high-priority",
  ): void {
    if (this.inFlightLoads.has(value.id)) {
      return;
    }

    const lowPriorityNode = this.lowPriorityNodes.get(value.id);
    const highPriorityNode = this.highPriorityNodes.get(value.id);

    switch (mode) {
      case "immediate":
        // Upgrade any low-priority or high-priority requests to immediate priority
        if (lowPriorityNode) {
          this.lowPriorityPending.remove(lowPriorityNode);
          this.lowPriorityNodes.delete(value.id);
        }
        if (highPriorityNode) {
          this.highPriorityPending.remove(highPriorityNode);
          this.highPriorityNodes.delete(value.id);
        }

        this.trackSent(value);
        sendCallback();
        break;
      case "high-priority":
        if (highPriorityNode) {
          return;
        }

        // Upgrade any low-priority requests to high-priority
        if (lowPriorityNode) {
          this.lowPriorityPending.remove(lowPriorityNode);
          this.lowPriorityNodes.delete(value.id);
        }

        this.highPriorityNodes.set(
          value.id,
          this.highPriorityPending.push({ value, sendCallback }),
        );
        this.processQueue();
        break;
      case "low-priority":
        if (lowPriorityNode || highPriorityNode) {
          return;
        }

        this.lowPriorityNodes.set(
          value.id,
          this.lowPriorityPending.push({ value, sendCallback }),
        );
        this.processQueue();
        break;
    }
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

      if (next) {
        // Remove from the tracking map since we're processing it
        this.highPriorityNodes.delete(next.value.id);
      } else {
        // Fall back to low-priority if high-priority is empty
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
    const inFlightCount = this.inFlightLoads.size;
    this.inFlightLoads.clear();
    if (inFlightCount > 0) {
      this.inFlightCounter.add(-inFlightCount);
    }

    // Drain existing queues to balance push/pull metrics
    while (this.highPriorityPending.shift()) {}
    while (this.lowPriorityPending.shift()) {}

    this.highPriorityNodes.clear();
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
