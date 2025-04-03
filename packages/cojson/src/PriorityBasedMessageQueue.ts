import { ValueType, metrics } from "@opentelemetry/api";
import { callWithTimeout } from "@opentelemetry/sdk-metrics/build/src/utils.js";
import { PeerState } from "./PeerState.js";
import type { CoValuePriority } from "./priority.js";
import type { Peer, SyncMessage } from "./sync.js";

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

interface Queue {
  getPriority: (msg: SyncMessage) => CoValuePriority;
  push: (msg: SyncMessage) => Promise<void>;
  pull: () => { priority: number; entry: QueueEntry } | undefined;
}

export function meteredQueue(queue: Queue, attrs?: Record<string, string>) {
  const pushCounter = metrics
    .getMeter("cojson")
    .createCounter("jazz.messagequeue.pushed", {
      description: "Number of messages pushed to the queue",
      valueType: ValueType.INT,
      unit: "total",
    });

  const pullCounter = metrics
    .getMeter("cojson")
    .createCounter("jazz.messagequeue.pulled", {
      description: "Number of messages pulled from the queue",
      valueType: ValueType.INT,
      unit: "total",
    });

  return {
    push(msg: SyncMessage) {
      pushCounter.add(1, {
        priority: queue.getPriority(msg),
        ...attrs,
      });
      return queue.push(msg);
    },
    pull() {
      const item = queue.pull();
      if (item) {
        pullCounter.add(1, {
          priority: item.priority,
          ...attrs,
        });
      }
      return item?.entry;
    },
  };
}

export class PriorityBasedMessageQueue implements Queue {
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

  private getQueue(priority: CoValuePriority) {
    return this.queues[priority];
  }

  constructor(private defaultPriority: CoValuePriority) {}

  public getPriority(msg: SyncMessage) {
    return "priority" in msg ? msg.priority : this.defaultPriority;
  }

  public push(msg: SyncMessage) {
    const { promise, resolve, reject } = promiseWithResolvers<void>();
    const entry: QueueEntry = { msg, promise, resolve, reject };

    this.getQueue(this.getPriority(msg)).push(entry);

    return promise;
  }

  public pull() {
    const priority = this.queues.findIndex((queue) => queue.length > 0);

    const entry = this.queues[priority]?.shift();

    if (!entry) {
      return;
    }

    return { priority, entry };
  }
}
