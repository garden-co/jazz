import { type Meter, metrics, ValueType } from "@opentelemetry/api";
import { type Peer, cojsonInternals, logger } from "cojson";
import { BatchedOutgoingMessages } from "./BatchedOutgoingMessages.js";
import { deserializeMessages } from "./serialization.js";
import type { AnyWebSocket } from "./types.js";

const { ConnectedPeerChannel, getContentMessageSize } = cojsonInternals;

export type CreateWebSocketPeerOpts = {
  id: string;
  websocket: AnyWebSocket;
  role: Peer["role"];
  expectPings?: boolean;
  batchingByDefault?: boolean;
  deletePeerStateOnClose?: boolean;
  pingTimeout?: number;
  onClose?: () => void;
  onSuccess?: () => void;
  /**
   * Additional key-value attributes to add to the ingress metric.
   */
  meta?: Record<string, string | number>;
  meter?: Meter;
};

function createPingTimeoutListener(
  enabled: boolean,
  timeout: number,
  callback: () => void,
) {
  if (!enabled) {
    return {
      reset() {},
      clear() {},
    };
  }

  let pingTimeout: ReturnType<typeof setTimeout> | null = null;

  return {
    reset() {
      pingTimeout && clearTimeout(pingTimeout);
      pingTimeout = setTimeout(() => {
        callback();
      }, timeout);
    },
    clear() {
      pingTimeout && clearTimeout(pingTimeout);
    },
  };
}

function createClosedEventEmitter(callback = () => {}) {
  let disconnected = false;

  return () => {
    if (disconnected) return;
    disconnected = true;
    callback();
  };
}

export function createWebSocketPeer({
  id,
  websocket,
  role,
  expectPings = true,
  batchingByDefault = true,
  deletePeerStateOnClose = false,
  pingTimeout = 10_000,
  onSuccess,
  onClose,
  meter,
  meta,
}: CreateWebSocketPeerOpts): Peer {
  const ingressBytesCounter = (
    meter ?? metrics.getMeter("cojson-transport-ws")
  ).createCounter("jazz.usage.ingress", {
    description: "Total ingress bytes from peer",
    unit: "bytes",
    valueType: ValueType.INT,
  });

  // Initialize the counter by adding 0
  ingressBytesCounter.add(0, meta);

  const incoming = new ConnectedPeerChannel();
  const emitClosedEvent = createClosedEventEmitter(onClose);

  function handleClose() {
    incoming.push("Disconnected");
    emitClosedEvent();
  }

  websocket.addEventListener("close", handleClose);
  // TODO (#1537): Remove this any once the WebSocket error event type is fixed
  // biome-ignore lint/suspicious/noExplicitAny: WebSocket error event type
  websocket.addEventListener("error" as any, (err) => {
    if (err.message) {
      logger.warn("WebSocket error", { err });
    }

    handleClose();
  });

  const pingTimeoutListener = createPingTimeoutListener(
    expectPings,
    pingTimeout,
    () => {
      incoming.push("Disconnected");
      logger.warn("Ping timeout from peer", {
        peerId: id,
        peerRole: role,
      });
      emitClosedEvent();
    },
  );

  const outgoing = new BatchedOutgoingMessages(
    websocket,
    batchingByDefault,
    role,
    meta,
    meter,
  );
  let isFirstMessage = true;

  function handleIncomingMsg(event: { data: unknown }) {
    pingTimeoutListener.reset();

    if (event.data === "") {
      return;
    }

    const result = deserializeMessages(event.data);

    if (!result.ok) {
      logger.warn("Error while deserializing messages", { err: result.error });
      return;
    }

    if (isFirstMessage) {
      // The only way to know that the connection has been correctly established with our sync server
      // is to track that we got a message from the server.
      onSuccess?.();
      isFirstMessage = false;
    }

    const { messages } = result;

    if (messages.length > 1) {
      // If more than one message is received, the other peer supports batching
      outgoing.setBatching(true);
    }

    for (const msg of messages) {
      if (msg && "action" in msg) {
        incoming.push(msg);

        if (msg.action === "content") {
          ingressBytesCounter.add(getContentMessageSize(msg), meta);
        }
      }
    }
  }

  websocket.addEventListener("message", handleIncomingMsg);

  outgoing.onClose(() => {
    websocket.removeEventListener("message", handleIncomingMsg);
    websocket.removeEventListener("close", handleClose);
    pingTimeoutListener.clear();
    emitClosedEvent();

    if (websocket.readyState === 0) {
      websocket.addEventListener(
        "open",
        function handleClose() {
          websocket.close();
        },
        { once: true },
      );
    } else if (websocket.readyState === 1) {
      websocket.close();
    }
  });

  return {
    id,
    incoming,
    outgoing,
    role,
    persistent: !deletePeerStateOnClose,
  };
}
