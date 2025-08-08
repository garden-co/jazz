import { Counter, metrics, ValueType } from "@opentelemetry/api";
import { SYNC_SCHEDULER_CONFIG } from "../config.js";
import { logger } from "../logger.js";
import type { PeerState } from "../PeerState.js";
import type { SyncMessage } from "../sync.js";
import { LinkedList } from "./LinkedList.js";

/**
 * A queue that schedules messages across different peers using a round-robin approach.
 *
 * This class manages incoming sync messages from multiple peers, ensuring fair processing
 * by cycling through each peer's message queue in a round-robin fashion. It also implements
 * collaborative scheduling on message processing, pausing when the main thread is blocked
 * for more than 50ms.
 */
export class IncomingMessagesQueue {
  private pullCounter: Counter;
  private pushCounter: Counter;

  queues: [LinkedList<SyncMessage>, PeerState][];
  peerToQueue: WeakMap<PeerState, LinkedList<SyncMessage>>;
  currentQueue = 0;

  constructor() {
    this.pullCounter = metrics
      .getMeter("cojson")
      .createCounter(`jazz.messagequeue.incoming.pulled`, {
        description: "Number of messages pulled from the queue",
        valueType: ValueType.INT,
        unit: "1",
      });
    this.pushCounter = metrics
      .getMeter("cojson")
      .createCounter(`jazz.messagequeue.incoming.pushed`, {
        description: "Number of messages pushed to the queue",
        valueType: ValueType.INT,
        unit: "1",
      });

    /**
     * This makes sure that those metrics are generated (and emitted) as soon as the queue is created.
     * This is to avoid edge cases where one series reset is delayed, which would cause spikes or dips
     * when queried - and it also more correctly represents the actual state of the queue after a restart.
     */
    this.pullCounter.add(0, {
      peerRole: "client",
    });
    this.pushCounter.add(0, {
      peerRole: "client",
    });
    this.pullCounter.add(0, {
      peerRole: "server",
    });
    this.pushCounter.add(0, {
      peerRole: "server",
    });

    this.queues = [];
    this.peerToQueue = new WeakMap();
  }

  public push(msg: SyncMessage, peer: PeerState) {
    const queue = this.peerToQueue.get(peer);

    if (!queue) {
      const newQueue = new LinkedList<SyncMessage>();
      this.peerToQueue.set(peer, newQueue);
      this.queues.push([newQueue, peer]);
      newQueue.push(msg);
    } else {
      queue.push(msg);
    }

    this.pushCounter.add(1, {
      peerRole: peer.role,
    });
  }

  public pull() {
    const entry = this.queues[this.currentQueue];

    if (!entry) {
      return undefined;
    }

    const [queue, peer] = entry;
    const msg = queue.shift();

    if (queue.isEmpty()) {
      this.queues.splice(this.currentQueue, 1);
      this.peerToQueue.delete(peer);
    } else {
      this.currentQueue++;
    }

    if (this.currentQueue >= this.queues.length) {
      this.currentQueue = 0;
    }

    if (msg) {
      this.pullCounter.add(1, {
        peerRole: peer.role,
      });

      return { msg, peer };
    }

    return undefined;
  }

  processing = false;

  async processQueue(callback: (msg: SyncMessage, peer: PeerState) => void) {
    this.processing = true;

    let entry: { msg: SyncMessage; peer: PeerState } | undefined;
    let lastTimer = performance.now();

    while ((entry = this.pull())) {
      const { msg, peer } = entry;

      try {
        callback(msg, peer);
      } catch (err) {
        logger.error("Error processing message", { err });
      }

      const currentTimer = performance.now();

      // We check if we have blocked the main thread for too long
      // and if so, we schedule a timer task to yield to the event loop
      if (
        currentTimer - lastTimer >
        SYNC_SCHEDULER_CONFIG.INCOMING_MESSAGES_TIME_BUDGET
      ) {
        await new Promise((resolve) => setTimeout(resolve, 0));
      }
    }

    this.processing = false;
  }
}
