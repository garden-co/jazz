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

export type QueueEntry<V> = {
  msg: V;
  promise: Promise<void>;
  resolve: () => void;
  reject: (_: unknown) => void;
  next: QueueEntry<V> | undefined;
};

/**
 * Since we have a fixed range of priority values (0-7) we can create a fixed array of queues.
 */
type Tuple<T, N extends number, A extends unknown[] = []> = A extends {
  length: N;
}
  ? A
  : Tuple<T, N, [...A, T]>;
type QueueTuple = Tuple<Queue<SyncMessage>, 8>;

class Queue<V> {
  head: QueueEntry<V> | undefined = undefined;
  tail: QueueEntry<V> | undefined = undefined;

  push(msg: V) {
    const { promise, resolve, reject } = promiseWithResolvers<void>();
    const entry: QueueEntry<V> = {
      msg,
      promise,
      resolve,
      reject,
      next: undefined,
    };

    if (this.head === undefined) {
      this.head = entry;
    } else {
      if (this.tail === undefined) {
        throw new Error("Tail is null but head is not");
      }

      this.tail.next = entry;
    }

    this.tail = entry;

    return entry;
  }

  pull() {
    const entry = this.head;

    if (entry) {
      this.head = entry.next;
    }

    if (this.head === undefined) {
      this.tail = undefined;
    }

    return entry;
  }

  isNonEmpty() {
    return this.head !== undefined;
  }
}

export class PriorityBasedMessageQueue {
  private queues: QueueTuple = [
    new Queue(),
    new Queue(),
    new Queue(),
    new Queue(),
    new Queue(),
    new Queue(),
    new Queue(),
    new Queue(),
  ];

  queueSizeCounter = metrics
    .getMeter("cojson")
    .createUpDownCounter("jazz.messagequeue.size", {
      description: "Size of the message queue",
      valueType: ValueType.INT,
      unit: "entry",
    });

  private getQueue(priority: CoValuePriority) {
    return this.queues[priority];
  }

  constructor(private defaultPriority: CoValuePriority) {}

  public push(msg: SyncMessage) {
    const priority = "priority" in msg ? msg.priority : this.defaultPriority;

    const entry = this.getQueue(priority).push(msg);

    this.queueSizeCounter.add(1, {
      priority,
    });

    return entry.promise;
  }

  public pull() {
    const priority = this.queues.findIndex((queue) => queue.isNonEmpty());

    if (priority === -1) {
      return;
    }

    this.queueSizeCounter.add(-1, {
      priority,
    });

    return this.queues[priority]?.pull();
  }
}
