import { CO_VALUE_PRIORITY, type CoValuePriority } from "../priority.js";
import type { SyncMessage } from "../sync.js";
import { QueueTuple, meteredList } from "./LinkedList.js";

const PRIORITY_TO_QUEUE_INDEX = {
  [CO_VALUE_PRIORITY.HIGH]: 0,
  [CO_VALUE_PRIORITY.MEDIUM]: 1,
  [CO_VALUE_PRIORITY.LOW]: 2,
} as const;

export class PriorityBasedMessageQueue {
  private queues: QueueTuple;

  constructor(
    private defaultPriority: CoValuePriority,
    type: "incoming" | "outgoing",
    /**
     * Optional attributes to be added to the generated metrics.
     * By default the metrics will have the priority as an attribute.
     */
    attrs?: Record<string, string | number>,
  ) {
    this.queues = [
      meteredList(type, { priority: CO_VALUE_PRIORITY.HIGH, ...attrs }),
      meteredList(type, { priority: CO_VALUE_PRIORITY.MEDIUM, ...attrs }),
      meteredList(type, { priority: CO_VALUE_PRIORITY.LOW, ...attrs }),
    ];
  }

  private getQueue(priority: CoValuePriority) {
    return this.queues[PRIORITY_TO_QUEUE_INDEX[priority]];
  }

  public push(msg: SyncMessage) {
    let priority = "priority" in msg ? msg.priority : this.defaultPriority;

    if (msg.action === "reconcile") {
      priority = CO_VALUE_PRIORITY.LOW;
    }

    this.getQueue(priority).push(msg);
  }

  public pull() {
    const priority = this.queues.findIndex((queue) => queue.length > 0);

    return this.queues[priority]?.shift();
  }

  public trackPushPull(msg: SyncMessage) {
    let priority = "priority" in msg ? msg.priority : this.defaultPriority;

    if (msg.action === "reconcile") {
      priority = CO_VALUE_PRIORITY.LOW;
    }

    this.getQueue(priority).trackPushPull();
  }
}
