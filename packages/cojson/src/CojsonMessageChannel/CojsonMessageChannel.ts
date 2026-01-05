import type { Peer } from "../sync.js";
import { ConnectedPeerChannel } from "../streamUtils.js";
import { MessagePortOutgoingChannel } from "./MessagePortOutgoingChannel.js";
import type {
  AcceptFromPortOptions,
  AcceptOptions,
  ExposeOptions,
  MessageChannelLike,
  MessagePortLike,
  PostMessageTarget,
  ReadyMessage,
} from "./types.js";
import {
  isControlMessage,
  isPortTransferMessage,
  isReadyAckMessage,
  isReadyMessage,
} from "./types.js";

/**
 * CojsonMessageChannel provides a low-level API for creating cojson peers
 * that communicate via the MessageChannel API (or compatible implementations
 * like Electron's MessageChannelMain).
 *
 * Inspired by Comlink, it handles:
 * - Port management (creating MessageChannels and transferring ports)
 * - Handshake protocol (ensuring both sides are ready)
 * - Peer creation (returning a standard cojson Peer)
 */
export class CojsonMessageChannel {
  /**
   * Expose a cojson connection to a target with a postMessage API.
   * Creates a MessageChannel, transfers port2 to the target, and waits for handshake.
   *
   * @param target - Any object with a postMessage method (Worker, Window, MessagePort, etc.)
   * @param opts - Configuration options
   * @returns A promise that resolves to a Peer once the handshake completes
   */
  static expose(
    target: PostMessageTarget,
    opts: ExposeOptions = {},
  ): Promise<Peer> {
    const id = opts.id ?? `channel_${Math.random()}`;
    const role = opts.role ?? "client";

    // Create or use provided MessageChannel
    const channel: MessageChannelLike =
      opts.messageChannel ?? new MessageChannel();
    const { port1, port2 } = channel;

    return new Promise<Peer>((resolve, reject) => {
      let resolved = false;

      const cleanup = () => {
        port1.removeEventListener("message", handleMessage);
        port1.removeEventListener("messageerror", handleError);
      };

      const handleError = (evt: unknown) => {
        if (resolved) return;
        cleanup();
        port1.close();
        reject(
          new Error("MessageChannel error during handshake", {
            cause: evt,
          }),
        );
      };

      const handleMessage = (event: unknown) => {
        const data = (event as MessageEvent).data;

        // Wait for ready acknowledgment from guest
        if (isReadyAckMessage(data)) {
          if (resolved) return;
          resolved = true;
          cleanup();

          // Create the peer
          const peer = createPeerFromPort(port1, {
            id,
            role,
            onClose: opts.onClose,
          });

          resolve(peer);
        }
      };

      // Start listening on port1
      port1.addEventListener("message", handleMessage);
      port1.addEventListener("messageerror", handleError);

      // Start the port if needed (browser MessagePort requires this)
      if (port1.start) {
        port1.start();
      }

      // Transfer port2 to target
      // Detect if target is a Window (has postMessage with targetOrigin signature)
      // We use duck typing: if targetOrigin is provided, assume it's a Window
      const targetOrigin = opts.targetOrigin ?? "*";
      const portTransferMessage = { type: "jazz:port", id };

      try {
        // Try Window-style postMessage first if targetOrigin is specified
        if (opts.targetOrigin !== undefined) {
          // Window/iframe: postMessage(data, targetOrigin, [transfer])
          target.postMessage(portTransferMessage, targetOrigin, [port2]);
        } else {
          // Worker/MessagePort style: postMessage(data, [transfer])
          target.postMessage(portTransferMessage, [port2]);
        }
      } catch {
        // Fallback: try the other signature
        try {
          target.postMessage(portTransferMessage, [port2]);
        } catch (e) {
          cleanup();
          port1.close();
          reject(new Error(`Failed to transfer port to target: ${e}`));
          return;
        }
      }

      // Send ready message with our ID
      const readyMessage: ReadyMessage = { type: "jazz:ready", id };
      port1.postMessage(readyMessage);
    });
  }

  /**
   * Accept an incoming Jazz connection.
   * Listens for a port transfer message on the global scope and completes the handshake.
   *
   * @param opts - Configuration options
   * @returns A promise that resolves to a Peer once the handshake completes
   */
  static accept(opts: AcceptOptions = {}): Promise<Peer> {
    return new Promise<Peer>((resolve) => {
      let resolved = false;

      const scope = globalThis as unknown as EventTarget;

      const cleanup = () => {
        scope.removeEventListener("message", handlePortTransfer);
      };

      const handlePortTransfer = async (event: unknown) => {
        const messageEvent = event as MessageEvent;
        const data = messageEvent.data;

        // Check if this is a valid port transfer message
        if (!isPortTransferMessage(data)) {
          return;
        }

        // If id filter is provided, check it against the port transfer message
        if (opts.id !== undefined && data.id !== opts.id) {
          return; // Ignore, keep waiting for matching id
        }

        // Validate origin if in Window context and allowedOrigins is specified
        if (opts.allowedOrigins && opts.allowedOrigins.length > 0) {
          const origin = messageEvent.origin;
          const isAllowed = opts.allowedOrigins.some(
            (allowed) => allowed === "*" || allowed === origin,
          );
          if (!isAllowed) {
            return; // Ignore messages from non-allowed origins
          }
        }

        // Get the transferred port
        const port = messageEvent.ports?.[0] as MessagePortLike | undefined;
        if (!port) {
          return; // No port transferred, ignore
        }

        if (resolved) return;
        resolved = true;
        cleanup();

        // Complete the handshake using acceptFromPort
        // Pass the id from the port transfer message to acceptFromPort
        const peer = await CojsonMessageChannel.acceptFromPort(port, {
          ...opts,
          id: data.id, // Use the id from the port transfer message
        });
        resolve(peer);
      };

      // Start listening for port transfer
      scope.addEventListener("message", handlePortTransfer);
    });
  }

  /**
   * Accept an incoming Jazz connection from a specific port.
   * Lower-level API useful for testing or when you already have the port.
   * This method has no timeout - it will wait indefinitely for the handshake.
   *
   * @param port - The MessagePort to accept the connection on
   * @param opts - Configuration options
   * @returns A promise that resolves to a Peer once the handshake completes
   */
  static acceptFromPort(
    port: MessagePortLike,
    opts: AcceptFromPortOptions = {},
  ): Promise<Peer> {
    const role = opts.role ?? "client";

    return new Promise<Peer>((resolve) => {
      let resolved = false;
      let peerId: string | undefined;

      const cleanup = () => {
        port.removeEventListener("message", handleMessage);
        port.removeEventListener("messageerror", handleError);
      };

      const handleError = () => {
        if (resolved) return;
        // On error, just close and let the caller handle it
        cleanup();
        port.close();
      };

      const handleMessage = (event: unknown) => {
        const data = (event as MessageEvent).data;

        // Wait for ready message from host
        if (isReadyMessage(data)) {
          // If id filter is provided, validate it
          if (opts.id !== undefined && data.id !== opts.id) {
            return; // Ignore, keep waiting for matching id
          }

          peerId = data.id;

          // Send acknowledgment
          port.postMessage({ type: "jazz:ready" });

          if (resolved) return;
          resolved = true;
          cleanup();

          // Create the peer
          const peer = createPeerFromPort(port, {
            id: peerId,
            role,
            onClose: opts.onClose,
          });

          resolve(peer);
        }
      };

      // Start listening
      port.addEventListener("message", handleMessage);
      port.addEventListener("messageerror", handleError);

      // Start the port if needed (browser MessagePort requires this)
      if (port.start) {
        port.start();
      }
    });
  }
}

/**
 * Create a Peer from a MessagePort after handshake is complete.
 */
function createPeerFromPort(
  port: MessagePortLike,
  opts: {
    id: string;
    role: "client" | "server";
    onClose?: () => void;
  },
): Peer {
  const incoming = new ConnectedPeerChannel();
  const outgoing = new MessagePortOutgoingChannel(port);

  // Forward messages from port to incoming channel
  const handleMessage = (event: unknown) => {
    const data = (event as MessageEvent).data;

    // Skip control messages (they're for handshake only)
    if (isControlMessage(data)) {
      return;
    }

    incoming.push(data);
  };

  const handleError = () => {
    incoming.push("Disconnected");
    incoming.close();
  };

  port.addEventListener("message", handleMessage);
  port.addEventListener("messageerror", handleError);
  port.addEventListener("close", () => {
    incoming.push("Disconnected");
    incoming.close();
  });

  // Handle outgoing channel close
  outgoing.onClose(() => {
    port.removeEventListener("message", handleMessage);
    port.removeEventListener("messageerror", handleError);
    port.close();
    incoming.push("Disconnected");
    incoming.close();
    opts.onClose?.();
  });

  // Handle incoming channel close (propagate to outgoing)
  incoming.onClose(() => {
    outgoing.close();
  });

  return {
    id: opts.id,
    incoming,
    outgoing,
    role: opts.role,
  };
}
