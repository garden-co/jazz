/**
 * Type definitions for JazzMessageChannel
 *
 * These types support cross-context communication via MessageChannel API
 * and compatible implementations (e.g., Electron's MessageChannelMain).
 */

/**
 * Duck-typed interface for any object that can receive messages via postMessage.
 *
 * Covers:
 * - Worker
 * - Window (including iframes via contentWindow)
 * - MessagePort
 * - ServiceWorker
 * - Client (Service Worker clients)
 * - Electron's WebContents (renderer windows)
 */
export interface PostMessageTarget {
  postMessage(message: unknown, transfer?: MessagePortLike[]): void;
  postMessage(
    message: unknown,
    targetOrigin: string,
    transfer?: MessagePortLike[],
  ): void;
}

/**
 * MessagePort-like interface that covers browser MessagePort and Electron MessagePortMain.
 *
 * Note: Electron's MessagePortMain does not have a start() method,
 * so it's optional here.
 */
export interface MessagePortLike {
  postMessage(message: unknown): void;
  addEventListener(
    type: "message" | "messageerror",
    listener: (event: unknown) => void,
  ): void;
  removeEventListener(
    type: "message" | "messageerror",
    listener: (event: unknown) => void,
  ): void;
  addEventListener(event: "close", listener: () => void): void;
  removeEventListener(type: "close", listener: () => void): void;
  /** Optional - not present on Electron's MessagePortMain */
  start?(): void;
  close(): void;
}

/**
 * MessageChannel-like interface that covers browser MessageChannel
 * and Electron MessageChannelMain.
 */
export interface MessageChannelLike {
  port1: MessagePortLike;
  port2: MessagePortLike;
}

/**
 * Options for JazzMessageChannel.expose()
 */
export interface ExposeOptions {
  /**
   * Unique identifier for the peer connection, sent to the guest during handshake.
   * Both sides will use this same ID for the Peer object.
   * If not provided, a unique ID will be generated using `channel_${Math.random()}`.
   */
  id?: string;

  /** Role of the peer in the sync topology */
  role?: "client" | "server";

  /** Target origin for Window targets (default: "*") */
  targetOrigin?: string;

  /**
   * A pre-created MessageChannel to use instead of creating a new one.
   * Use this for environments where the global MessageChannel is not available,
   * e.g., Electron main process with MessageChannelMain.
   * If not provided, a new MessageChannel will be created.
   */
  messageChannel?: MessageChannelLike;

  /** Callback when the connection closes */
  onClose?: () => void;
}

/**
 * Options for CojsonMessageChannel.accept()
 */
export interface AcceptOptions {
  /**
   * Expected peer ID to accept.
   * If provided, only handshakes with matching id will be accepted; others are ignored.
   * If not provided, any connection will be accepted.
   */
  id?: string;

  /** Role of the peer in the sync topology */
  role?: "client" | "server";

  /** Allowed origins for Window contexts (default: ["*"]) */
  allowedOrigins?: string[];

  /** Callback when the connection closes */
  onClose?: () => void;
}

/**
 * Options for CojsonMessageChannel.acceptFromPort()
 * Note: No timeout option - acceptFromPort waits indefinitely.
 */
export interface AcceptFromPortOptions {
  /**
   * Expected peer ID to accept.
   * If provided, only handshakes with matching id will be accepted; others are ignored.
   * If not provided, any connection will be accepted.
   */
  id?: string;

  /** Role of the peer in the sync topology */
  role?: "client" | "server";

  /** Callback when the connection closes */
  onClose?: () => void;
}

/**
 * Port transfer message sent via target.postMessage.
 * The actual port is transferred via Transferable.
 */
export interface PortTransferMessage {
  type: "jazz:port";
  /** The peer ID for this connection */
  id: string;
}

/**
 * Ready signal sent from host to guest via MessagePort.
 * Contains the peer ID that both sides will use.
 */
export interface ReadyMessage {
  type: "jazz:ready";
  /** The peer ID to use on both sides */
  id: string;
}

/**
 * Acknowledgment sent from guest to host via MessagePort.
 * No id needed - guest uses the id from the host's ReadyMessage.
 */
export interface ReadyAckMessage {
  type: "jazz:ready";
}

/**
 * Union of all control messages used in the handshake protocol.
 */
export type ControlMessage =
  | PortTransferMessage
  | ReadyMessage
  | ReadyAckMessage;

/**
 * Type guard to check if a message is a Jazz control message.
 * Control messages are identified by having a "type" property starting with "jazz:".
 */
export function isControlMessage(msg: unknown): msg is ControlMessage {
  return (
    typeof msg === "object" &&
    msg !== null &&
    "type" in msg &&
    typeof (msg as { type: unknown }).type === "string" &&
    (msg as { type: string }).type.startsWith("jazz:")
  );
}

/**
 * Type guard to check if a message is a PortTransferMessage.
 */
export function isPortTransferMessage(
  msg: unknown,
): msg is PortTransferMessage {
  return (
    typeof msg === "object" &&
    msg !== null &&
    "type" in msg &&
    (msg as { type: unknown }).type === "jazz:port" &&
    "id" in msg &&
    typeof (msg as { id: unknown }).id === "string"
  );
}

/**
 * Type guard to check if a message is a ReadyMessage (with id).
 */
export function isReadyMessage(msg: unknown): msg is ReadyMessage {
  return (
    typeof msg === "object" &&
    msg !== null &&
    "type" in msg &&
    (msg as { type: unknown }).type === "jazz:ready" &&
    "id" in msg &&
    typeof (msg as { id: unknown }).id === "string"
  );
}

/**
 * Type guard to check if a message is a ReadyAckMessage (without id).
 */
export function isReadyAckMessage(msg: unknown): msg is ReadyAckMessage {
  return (
    typeof msg === "object" &&
    msg !== null &&
    "type" in msg &&
    (msg as { type: unknown }).type === "jazz:ready" &&
    !("id" in msg)
  );
}
