import { ValueType, metrics } from "@opentelemetry/api";
import type { CoValuePriority } from "./priority.js";
import type { SyncMessage } from "./sync.js";

function promiseWithResolvers<R>() {
  let resolve = (_: R) => {};
  let reject = (_: unknown) => {};

  const promise = new Promise<R>((_resolve, _reject) => {
    resolve = _resolve;
    reject = _reject;
  });

  return {
    promise,
    resolve,
    reject,
  };
}

export type QueueEntry = {
  msg: SyncMessage;
  promise: Promise<void>;
  resolve: () => void;
  reject: (_: unknown) => void;
};

/**
 * Since we have a fixed range of priority values (0-7) we can create a fixed array of queues.
 */
type Tuple<T, N extends number, A extends unknown[] = []> = A extends {
  length: N;
}
  ? A
  : Tuple<T, N, [...A, T]>;

type QueueTuple = Tuple<LinkedList<QueueEntry>, 8>;

type LinkedListNode<T> = {
  value: T;
  next: LinkedListNode<T> | undefined;
};

/**
 * Using a linked list to make the shift operation O(1) instead of O(n)
 * as our queues can grow very large when the system is under pressure.
 */
export class LinkedList<T> {
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

    return value;
  }
}

export class PriorityBasedMessageQueue {
  private queues: QueueTuple = [
    new LinkedList<QueueEntry>(),
    new LinkedList<QueueEntry>(),
    new LinkedList<QueueEntry>(),
    new LinkedList<QueueEntry>(),
    new LinkedList<QueueEntry>(),
    new LinkedList<QueueEntry>(),
    new LinkedList<QueueEntry>(),
    new LinkedList<QueueEntry>(),
  ];

  private enqueuedCounter = metrics
    .getMeter("cojson")
    .createCounter("jazz.messagequeue.enqueued", {
      description: "Number of messages enqueued",
      valueType: ValueType.INT,
      unit: "entry",
    });

  private dequeuedCounter = metrics
    .getMeter("cojson")
    .createCounter("jazz.messagequeue.removed", {
      description: "Number of messages removed from the queue",
      valueType: ValueType.INT,
      unit: "entry",
    });

  private getQueue(priority: CoValuePriority) {
    return this.queues[priority];
  }

  constructor(private defaultPriority: CoValuePriority) {}

  public push(msg: SyncMessage) {
    const { promise, resolve, reject } = promiseWithResolvers<void>();
    const entry: QueueEntry = { msg, promise, resolve, reject };

    const priority = "priority" in msg ? msg.priority : this.defaultPriority;

    this.getQueue(priority).push(entry);

    this.enqueuedCounter.add(1, {
      priority,
    });

    return promise;
  }

  public pull() {
    const priority = this.queues.findIndex((queue) => queue.length > 0);

    if (priority === -1) {
      return;
    }

    this.dequeuedCounter.add(1, {
      priority,
    });

    return this.queues[priority]?.shift();
  }
}
