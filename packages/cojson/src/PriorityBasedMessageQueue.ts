import { Counter, ValueType, metrics } from "@opentelemetry/api";
import { CO_VALUE_PRIORITY, type CoValuePriority } from "./priority.js";
import type { SyncMessage } from "./sync.js";

/**
 * Since we have a fixed range of priority values (0-7) we can create a fixed array of queues.
 */
type Tuple<T, N extends number, A extends unknown[] = []> = A extends {
  length: N;
}
  ? A
  : Tuple<T, N, [...A, T]>;

type QueueTuple = Tuple<LinkedList<SyncMessage>, 3>;

type LinkedListNode<T> = {
  value: T;
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

  push(value: T) {
    const node = { value, next: undefined };

    if (this.head === undefined) {
      this.head = node;
      this.tail = node;
    } else if (this.tail) {
      this.tail.next = node;
      this.tail = node;
    } else {
      throw new Error("LinkedList is corrupted");
    }

    this.length++;
    this.meter?.push();
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
    }

    this.length--;

    this.meter?.pull();
    return value;
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
      .getMeter("cojosn")
      .createCounter(`${prefix}.pulled`, {
        description: "Number of messages pulled from the queue",
        valueType: ValueType.INT,
        unit: "1",
      });
    this.pushCounter = metrics
      .getMeter("cojosn")
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
}

function meteredList<T>(attrs?: Record<string, string | number>) {
  return new LinkedList<T>(new QueueMeter("jazz.messagequeue", attrs));
}

const PRIORITY_TO_QUEUE_INDEX = {
  [CO_VALUE_PRIORITY.HIGH]: 0,
  [CO_VALUE_PRIORITY.MEDIUM]: 1,
  [CO_VALUE_PRIORITY.LOW]: 2,
} as const;

export class PriorityBasedMessageQueue {
  private queues: QueueTuple;

  constructor(
    private defaultPriority: CoValuePriority,
    /**
     * Optional attributes to be added to the generated metrics.
     * By default the metrics will have the priority as an attribute.
     */
    attrs?: Record<string, string | number>,
  ) {
    this.queues = [
      meteredList({ priority: CO_VALUE_PRIORITY.HIGH, ...attrs }),
      meteredList({ priority: CO_VALUE_PRIORITY.MEDIUM, ...attrs }),
      meteredList({ priority: CO_VALUE_PRIORITY.LOW, ...attrs }),
    ];
  }

  private getQueue(priority: CoValuePriority) {
    return this.queues[PRIORITY_TO_QUEUE_INDEX[priority]];
  }

  public push(msg: SyncMessage) {
    const priority = "priority" in msg ? msg.priority : this.defaultPriority;

    this.getQueue(priority).push(msg);
  }

  public pull() {
    const priority = this.queues.findIndex((queue) => queue.length > 0);

    return this.queues[priority]?.shift();
  }
}
