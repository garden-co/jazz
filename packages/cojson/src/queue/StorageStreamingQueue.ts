import { CO_VALUE_PRIORITY, type CoValuePriority } from "../priority.js";
import { LinkedList, meteredList } from "./LinkedList.js";

/**
 * A callback that pushes content when invoked.
 * Content is only fetched from the database when this callback is called.
 */
export type ContentCallback = () => void;

// All priorities use the queue, processed in order: HIGH > MEDIUM > LOW
const PRIORITY_TO_QUEUE_INDEX = {
  [CO_VALUE_PRIORITY.HIGH]: 0,
  [CO_VALUE_PRIORITY.MEDIUM]: 1,
  [CO_VALUE_PRIORITY.LOW]: 2,
} as const;

type StreamingQueueTuple = [
  high: LinkedList<ContentCallback>,
  medium: LinkedList<ContentCallback>,
  low: LinkedList<ContentCallback>,
];

/**
 * A priority-based queue for storage content streaming.
 *
 * This queue manages content streaming for all priority levels (HIGH, MEDIUM, LOW).
 * Content is processed in priority order: HIGH first, then MEDIUM, then LOW.
 *
 * Key features:
 * - Stores callbacks to get content (lazy evaluation) rather than content itself
 * - Priority-based ordering: HIGH > MEDIUM > LOW
 */
export class StorageStreamingQueue {
  private queues: StreamingQueueTuple;

  constructor() {
    this.queues = [
      meteredList("storage-streaming", { priority: CO_VALUE_PRIORITY.HIGH }),
      meteredList("storage-streaming", { priority: CO_VALUE_PRIORITY.MEDIUM }),
      meteredList("storage-streaming", { priority: CO_VALUE_PRIORITY.LOW }),
    ];
  }

  private getQueue(priority: CoValuePriority) {
    return this.queues[PRIORITY_TO_QUEUE_INDEX[priority]];
  }

  /**
   * Push a content callback to the queue with explicit priority.
   * The callback will be invoked when the entry is pulled and processed.
   *
   * @param entry - Callback that pushes content when invoked
   * @param priority - Priority for this entry (HIGH, MEDIUM, or LOW)
   */
  public push(entry: ContentCallback, priority: CoValuePriority): void {
    this.getQueue(priority).push(entry);
  }

  /**
   * Pull the next entry from the queue.
   * Returns undefined if no entries are available.
   * Priority order: HIGH > MEDIUM > LOW
   */
  public pull(): ContentCallback | undefined {
    // Find the first non-empty queue (HIGH > MEDIUM > LOW)
    const queueIndex = this.queues.findIndex((queue) => queue.length > 0);

    if (queueIndex === -1) {
      return undefined;
    }

    const entry = this.queues[queueIndex]?.shift();

    if (!entry) {
      return undefined;
    }

    return entry;
  }

  /**
   * Check if the queue is empty (no pending entries).
   */
  public isEmpty(): boolean {
    return this.queues.every((queue) => queue.length === 0);
  }

  private listener: (() => void) | undefined;
  setListener(listener: () => void): void {
    this.listener = listener;
  }

  emit(): void {
    this.listener?.();
  }
}
