import { type Meter, metrics, ValueType } from "@opentelemetry/api";
import type { DisconnectedError, SyncMessage } from "cojson";
import type { Peer } from "cojson";
import {
  type CojsonInternalTypes,
  PriorityBasedMessageQueue,
  cojsonInternals,
  logger,
} from "cojson";
import type { AnyWebSocket } from "./types.js";
import {
  hasWebSocketTooMuchBufferedData,
  isWebSocketOpen,
  waitForWebSocketBufferedAmount,
  waitForWebSocketOpen,
} from "./utils.js";

const { CO_VALUE_PRIORITY, getContentMessageSize, WEBSOCKET_CONFIG } =
  cojsonInternals;

export class BatchedOutgoingMessages
  implements CojsonInternalTypes.OutgoingPeerChannel
{
  private backlog: string[] = [];
  private queue: PriorityBasedMessageQueue;
  private processing = false;
  private closed = false;
  private egressBytesCounter;

  constructor(
    private websocket: AnyWebSocket,
    private batching: boolean,
    peerRole: Peer["role"],
    /**
     * Additional key-value pair of attributes to add to the egress metric.
     */
    private meta?: Record<string, string | number>,
    meter?: Meter,
  ) {
    this.egressBytesCounter = (
      meter ?? metrics.getMeter("cojson-transport-ws")
    ).createCounter("jazz.usage.egress", {
      description: "Total egress bytes",
      unit: "bytes",
      valueType: ValueType.INT,
    });

    this.queue = new PriorityBasedMessageQueue(
      CO_VALUE_PRIORITY.HIGH,
      "outgoing",
      {
        peerRole: peerRole,
      },
    );

    // Initialize the counter by adding 0
    this.egressBytesCounter.add(0, this.meta);
  }

  push(msg: SyncMessage | DisconnectedError) {
    if (msg === "Disconnected") {
      this.close();
      return;
    }

    // Don't accept messages after close - they would be queued but never pulled
    if (this.closed) {
      return;
    }

    // If already processing, queue the message for later
    if (this.processing) {
      this.queue.push(msg);
      return;
    }

    // Fast path: send directly without touching the queue
    // This avoids push/pull counter mismatch when WebSocket is ready
    if (
      isWebSocketOpen(this.websocket) &&
      !hasWebSocketTooMuchBufferedData(this.websocket)
    ) {
      this.processMessage(msg, true);
      this.queue.trackPushPull(msg);
      return;
    }

    // Slow path: queue and process asynchronously
    this.queue.push(msg);
    this.processQueue().catch((e) => {
      logger.error("Error while processing sendMessage queue", { err: e });
    });
  }

  private async processQueue() {
    const { websocket } = this;

    this.processing = true;

    let msg = this.queue.pull();

    while (msg) {
      if (this.closed) {
        return;
      }

      if (!isWebSocketOpen(websocket)) {
        await waitForWebSocketOpen(websocket);
      }

      if (hasWebSocketTooMuchBufferedData(websocket)) {
        await waitForWebSocketBufferedAmount(websocket);
      }

      if (isWebSocketOpen(websocket)) {
        this.processMessage(msg);

        msg = this.queue.pull();
      }
    }

    this.sendMessagesInBulk();
    this.processing = false;
  }

  private processMessage(msg: SyncMessage, skipBatching: boolean = false) {
    if (msg.action === "content") {
      this.egressBytesCounter.add(getContentMessageSize(msg), this.meta);
    }

    const stringifiedMsg = this.serializeMessage(msg);

    if (!this.batching || skipBatching) {
      this.websocket.send(stringifiedMsg);
      return;
    }

    const msgSize = stringifiedMsg.length;
    const newBacklogSize = this.backlog.length + msgSize;

    // If backlog+message exceeds the chunk size, send the backlog and reset it
    if (
      this.backlog.length > 0 &&
      newBacklogSize > WEBSOCKET_CONFIG.MAX_OUTGOING_MESSAGES_CHUNK_BYTES
    ) {
      this.sendMessagesInBulk();
    }

    this.appendMessage(stringifiedMsg);

    // If message itself exceeds the chunk size, send it immediately
    if (msgSize >= WEBSOCKET_CONFIG.MAX_OUTGOING_MESSAGES_CHUNK_BYTES) {
      this.sendMessagesInBulk();
    }
  }

  private serializeMessage(msg: SyncMessage) {
    return JSON.stringify(msg);
  }

  private appendMessage(msg: string) {
    this.backlog.push(msg);
  }

  private sendMessagesInBulk() {
    if (this.backlog.length > 0 && isWebSocketOpen(this.websocket)) {
      this.websocket.send(this.backlog.join("\n"));
      this.backlog.length = 0;
    }
  }

  drain() {
    while (this.queue.pull()) {}
  }

  setBatching(enabled: boolean) {
    this.batching = enabled;
  }

  private closeListeners = new Set<() => void>();
  onClose(callback: () => void) {
    this.closeListeners.add(callback);
  }

  close() {
    if (this.closed) {
      return;
    }

    let msg = this.queue.pull();

    while (msg) {
      this.processMessage(msg);
      msg = this.queue.pull();
    }

    this.closed = true;
    this.sendMessagesInBulk();

    for (const listener of this.closeListeners) {
      listener();
    }

    this.closeListeners.clear();
  }
}
