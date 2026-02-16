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
  enablePingDelayLogs?: boolean;
  pingDelayLogsData?: Record<string, string | number>;
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
  let pingDeadline = 0;
  let cleared = false;

  function scheduleTimeout() {
    if (cleared || pingTimeout !== null || pingDeadline === 0) {
      return;
    }

    pingTimeout = setTimeout(
      () => {
        pingTimeout = null;

        if (cleared) {
          return;
        }

        if (Date.now() >= pingDeadline) {
          cleared = true;
          callback();
          return;
        }

        scheduleTimeout();
      },
      Math.max(0, pingDeadline - Date.now()),
    );
  }

  return {
    reset() {
      if (cleared) {
        return;
      }

      pingDeadline = Date.now() + timeout;
      scheduleTimeout();
    },
    clear() {
      cleared = true;

      if (pingTimeout !== null) {
        clearTimeout(pingTimeout);
        pingTimeout = null;
      }
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
  enablePingDelayLogs = false,
  pingDelayLogsData = {},
}: CreateWebSocketPeerOpts): Peer {
  const meterProvider = meter ?? metrics.getMeter("cojson-transport-ws");

  const ingressBytesCounter = meterProvider.createCounter(
    "jazz.usage.ingress",
    {
      description: "Total ingress bytes from peer",
      unit: "bytes",
      valueType: ValueType.INT,
    },
  );

  // Initialize the counter by adding 0
  ingressBytesCounter.add(0, meta);

  const incoming = new ConnectedPeerChannel();
  const emitClosedEvent = createClosedEventEmitter(onClose);

  function cleanup() {
    websocket.removeEventListener("message", handleIncomingMsg);
    websocket.removeEventListener("close", handleClose);
    // Only some WebSocket implementations emit the error event, so we need to do casting to any to avoid type errors
    websocket.removeEventListener("error" as any, handleError);
    pingTimeoutListener.clear();
    outgoing.drain();
  }

  function handleClose() {
    incoming.push("Disconnected");
    emitClosedEvent();
    cleanup();
  }

  function handleError(err: unknown) {
    if (err instanceof Error && err.message) {
      logger.warn("WebSocket error", { err });
    }

    handleClose();
  }

  websocket.addEventListener("close", handleClose);
  // Only some WebSocket implementations emit the error event, so we need to do casting to any to avoid type errors
  websocket.addEventListener("error" as any, handleError);

  const pingTimeoutListener = createPingTimeoutListener(
    expectPings,
    pingTimeout,
    () => {
      handleClose();
      logger.warn("Ping timeout from peer", {
        peerId: id,
        peerRole: role,
      });
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
      if (!msg) {
        continue;
      }

      if (enablePingDelayLogs && "time" in msg) {
        logger.info("Ping delay", {
          delay: Math.max(0, Date.now() - msg.time),
          server: msg.dc,
          ...pingDelayLogsData,
        });
      }

      if ("action" in msg) {
        incoming.push(msg);

        if (msg.action === "content") {
          ingressBytesCounter.add(getContentMessageSize(msg), meta);
        }
      }
    }
  }

  websocket.addEventListener("message", handleIncomingMsg);

  outgoing.onClose(() => {
    cleanup();
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
