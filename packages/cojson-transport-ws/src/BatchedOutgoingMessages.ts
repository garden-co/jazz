import type { DisconnectedError, SyncMessage } from "cojson";
import type { Peer } from "cojson";
import {
  type CojsonInternalTypes,
  PriorityBasedMessageQueue,
  cojsonInternals,
  logger,
} from "cojson";
import { addMessageToBacklog } from "./serialization.js";
import type { AnyWebSocket } from "./types.js";
import {
  hasWebSocketTooMuchBufferedData,
  isWebSocketOpen,
  waitForWebSocketBufferedAmount,
  waitForWebSocketOpen,
} from "./utils.js";

const { CO_VALUE_PRIORITY } = cojsonInternals;

export const MAX_OUTGOING_MESSAGES_CHUNK_BYTES = 25_000;

export class BatchedOutgoingMessages
  implements CojsonInternalTypes.OutgoingPeerChannel
{
  private backlog = "";
  private queue: PriorityBasedMessageQueue;
  private processing = false;
  private closed = false;

  constructor(
    private websocket: AnyWebSocket,
    private batching: boolean,
    peerRole: Peer["role"],
  ) {
    this.queue = new PriorityBasedMessageQueue(CO_VALUE_PRIORITY.HIGH, {
      peerRole: peerRole,
    });
  }

  push(msg: SyncMessage | DisconnectedError) {
    if (msg === "Disconnected") {
      this.close();
      return;
    }

    this.queue.push(msg);

    if (this.processing) {
      return;
    }

    this.processQueue().catch((e) => {
      logger.error("Error while processing sendMessage queue", { err: e });
    });
  }

  private async processQueue() {
    const { websocket } = this;

    this.processing = true;

    // Delay the initiation of the queue processing to accumulate messages
    // before sending them, in order to do prioritization and batching
    await new Promise<void>((resolve) => setTimeout(resolve, 10));

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

  processMessage(msg: SyncMessage) {
    if (!this.batching) {
      this.websocket.send(JSON.stringify(msg));
      return;
    }

    const payload = addMessageToBacklog(this.backlog, msg);

    const maxChunkSizeReached =
      payload.length >= MAX_OUTGOING_MESSAGES_CHUNK_BYTES;
    const backlogExists = this.backlog.length > 0;

    if (maxChunkSizeReached && backlogExists) {
      this.sendMessagesInBulk();
      this.backlog = addMessageToBacklog("", msg);
    } else if (maxChunkSizeReached) {
      this.backlog = payload;
      this.sendMessagesInBulk();
    } else {
      this.backlog = payload;
    }
  }

  sendMessagesInBulk() {
    if (this.backlog.length > 0 && isWebSocketOpen(this.websocket)) {
      this.websocket.send(this.backlog);
      this.backlog = "";
    }
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
