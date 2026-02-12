import type {
  DisconnectedError,
  OutgoingPeerChannel,
  SyncMessage,
} from "../sync.js";
import type { MessagePortLike } from "./types.js";

/**
 * An implementation of OutgoingPeerChannel that sends messages via a MessagePortLike.
 *
 * Messages are sent directly using the port's postMessage method,
 * which uses structured cloning (no JSON serialization needed).
 */
export class MessagePortOutgoingChannel implements OutgoingPeerChannel {
  private port: MessagePortLike;
  private closed = false;
  private closeListeners = new Set<() => void>();

  constructor(port: MessagePortLike) {
    this.port = port;
  }

  push(msg: SyncMessage | DisconnectedError): void {
    if (this.closed) {
      return;
    }

    if (msg === "Disconnected") {
      this.close();
      return;
    }

    this.port.postMessage(msg);
  }

  close(): void {
    if (this.closed) {
      return;
    }

    this.closed = true;
    this.port.close();

    for (const listener of this.closeListeners) {
      listener();
    }
  }

  onClose(callback: () => void): void {
    this.closeListeners.add(callback);
  }
}
