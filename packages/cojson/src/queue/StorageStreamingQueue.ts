import { CO_VALUE_PRIORITY, type CoValuePriority } from "../priority.js";
import type { RawCoID } from "../ids.js";
import { LinkedList } from "./LinkedList.js";

/**
 * A callback that pushes content when invoked.
 * Content is only fetched from the database when this callback is called.
 */
export type ContentCallback = () => void;

// Only MEDIUM and LOW priorities use the queue (HIGH bypasses it)
const PRIORITY_TO_QUEUE_INDEX = {
  [CO_VALUE_PRIORITY.MEDIUM]: 0,
  [CO_VALUE_PRIORITY.LOW]: 1,
} as const;

type StreamingQueueTuple = [
  LinkedList<ContentCallback>,
  LinkedList<ContentCallback>,
];

/**
 * A priority-based queue for storage content streaming.
 *
 * This queue manages content streaming for MEDIUM and LOW priority CoValues.
 * HIGH priority content (accounts, groups) bypasses this queue entirely and
 * streams directly via callbacks.
 *
 * Key features:
 * - Stores callbacks to get content (lazy evaluation) rather than content itself
 * - Tracks active streaming sessions per CoValue
 * - Automatically removes CoValue from active streams when last chunk is pulled
 * - Priority-based ordering: MEDIUM priority is processed before LOW
 */
export class StorageStreamingQueue {
  private queues: StreamingQueueTuple;

  constructor() {
    this.queues = [
      new LinkedList<ContentCallback>(),
      new LinkedList<ContentCallback>(),
    ];
  }

  private getQueue(priority: CoValuePriority) {
    if (priority === CO_VALUE_PRIORITY.HIGH) {
      throw new Error(
        "HIGH priority content should bypass the queue and stream directly",
      );
    }
    return this.queues[PRIORITY_TO_QUEUE_INDEX[priority]];
  }

  /**
   * Push a content callback to the queue with explicit priority.
   * The callback will be invoked when the entry is pulled and processed.
   *
   * @param id - The CoValue ID
   * @param getContent - Callback that returns the content when invoked
   * @param priority - Explicit priority for this entry (MEDIUM or LOW only)
   * @param isLastChunk - Whether this is the final chunk for this CoValue
   */
  public push(entry: ContentCallback, priority: CoValuePriority): void {
    this.getQueue(priority).push(entry);
  }

  /**
   * Pull the next entry from the queue.
   * Returns undefined if no entries are available.
   *
   * When isLastChunk is true, the CoValue is automatically
   * removed from the active streams set.
   */
  public pull(): ContentCallback | undefined {
    // Find the first non-empty queue (MEDIUM has priority over LOW)
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
