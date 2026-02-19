import { Counter, ValueType, metrics } from "@opentelemetry/api";
import type { SyncMessage } from "../sync.js";

/**
 * Since we have a fixed range of priority values (0-7) we can create a fixed array of queues.
 */
type Tuple<T, N extends number, A extends unknown[] = []> = A extends {
  length: N;
}
  ? A
  : Tuple<T, N, [...A, T]>;
export type QueueTuple = Tuple<LinkedList<SyncMessage>, 3>;
export type LinkedListNode<T> = {
  value: T;
  prev: LinkedListNode<T> | undefined;
  next: LinkedListNode<T> | undefined;
};
/**
 * Using a linked list to make the shift operation O(1) instead of O(n)
 * as our queues can grow very large when the system is under pressure.
 */

export class LinkedList<T> {
  constructor(private meter?: QueueMeter) {}

  head: LinkedListNode<T> | undefined = undefined;
  tail: LinkedListNode<T> | undefined = undefined;
  length = 0;

  push(value: T): LinkedListNode<T> {
    const node: LinkedListNode<T> = { value, prev: undefined, next: undefined };

    if (this.head === undefined) {
      this.head = node;
      this.tail = node;
    } else if (this.tail) {
      node.prev = this.tail;
      this.tail.next = node;
      this.tail = node;
    } else {
      throw new Error("LinkedList is corrupted");
    }

    this.length++;
    this.meter?.push();
    return node;
  }

  shift() {
    if (!this.head) {
      return undefined;
    }

    const node = this.head;
    const value = node.value;
    this.head = node.next;
    node.next = undefined;

    if (this.head === undefined) {
      this.tail = undefined;
    } else {
      this.head.prev = undefined;
    }

    this.length--;

    this.meter?.pull();
    return value;
  }

  trackPushPull() {
    this.meter?.trackPushPull();
  }

  /**
   * Remove a specific node from the list in O(1) time.
   * The node must be a valid node that was returned by push().
   */
  remove(node: LinkedListNode<T>): void {
    if (node.prev) {
      node.prev.next = node.next;
    } else {
      // Node is the head
      this.head = node.next;
    }

    if (node.next) {
      node.next.prev = node.prev;
    } else {
      // Node is the tail
      this.tail = node.prev;
    }

    node.prev = undefined;
    node.next = undefined;
    this.length--;
    this.meter?.pull();
  }

  isEmpty() {
    return this.head === undefined;
  }
}
class QueueMeter {
  private pullCounter: Counter;
  private pushCounter: Counter;

  constructor(
    prefix: string,
    private attrs?: Record<string, string | number>,
  ) {
    this.pullCounter = metrics
      .getMeter("cojson")
      .createCounter(`${prefix}.pulled`, {
        description: "Number of messages pulled from the queue",
        valueType: ValueType.INT,
        unit: "1",
      });
    this.pushCounter = metrics
      .getMeter("cojson")
      .createCounter(`${prefix}.pushed`, {
        description: "Number of messages pushed to the queue",
        valueType: ValueType.INT,
        unit: "1",
      });

    /**
     * This makes sure that those metrics are generated (and emitted) as soon as the queue is created.
     * This is to avoid edge cases where one series reset is delayed, which would cause spikes or dips
     * when queried - and it also more correctly represents the actual state of the queue after a restart.
     */
    this.pullCounter.add(0, this.attrs);
    this.pushCounter.add(0, this.attrs);
  }

  public pull() {
    this.pullCounter.add(1, this.attrs);
  }

  public push() {
    this.pushCounter.add(1, this.attrs);
  }

  public trackPushPull() {
    this.pullCounter.add(1, this.attrs);
    this.pushCounter.add(1, this.attrs);
  }
}

export function meteredList<T>(
  type: "incoming" | "outgoing" | "storage-streaming" | "load-requests-queue",
  attrs?: Record<string, string | number>,
) {
  return new LinkedList<T>(new QueueMeter("jazz.messagequeue." + type, attrs));
}
