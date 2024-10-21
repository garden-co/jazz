import {
    DisconnectedError,
    Peer,
    PingTimeoutError,
    SyncMessage,
    cojsonInternals,
} from "cojson";
import { AnyWebSocket } from "./types.js";
import { BatchedOutgoingMessages } from "./BatchedOutgoingMessages.js";
import { deserializeMessages } from "./serialization.js";

export const BUFFER_LIMIT = 100_000;
export const BUFFER_LIMIT_POLLING_INTERVAL = 10;

export type CreateWebSocketPeerOpts = {
    id: string;
    websocket: AnyWebSocket;
    role: Peer["role"];
    expectPings?: boolean;
    batchingByDefault?: boolean;
};

export function createWebSocketPeer({
    id,
    websocket,
    role,
    expectPings = true,
    batchingByDefault = true,
}: CreateWebSocketPeerOpts): Peer {
    const incoming = new cojsonInternals.Channel<
        SyncMessage | DisconnectedError | PingTimeoutError
    >();

    websocket.addEventListener("close", function handleClose() {
        incoming
            .push("Disconnected")
            .catch((e) =>
                console.error("Error while pushing disconnect msg", e),
            );
    });

    let pingTimeout: ReturnType<typeof setTimeout> | null = null;

    let supportsBatching = batchingByDefault;

    websocket.addEventListener("message", function handleIncomingMsg(event) {
        const result = deserializeMessages(event.data as string);

        if (!result.ok) {
            console.error(
                "Error while deserializing messages",
                event.data,
                result.error,
            );
            return;
        }

        const { messages } = result;

        if (!supportsBatching && messages.length > 1) {
            // If more than one message is received, the other peer supports batching
            supportsBatching = true;
        }

        if (expectPings) {
            pingTimeout && clearTimeout(pingTimeout);
            pingTimeout = setTimeout(() => {
                incoming
                    .push("PingTimeout")
                    .catch((e) =>
                        console.error("Error while pushing ping timeout", e),
                    );
            }, 10_000);
        }

        for (const msg of messages) {
            if (msg && "action" in msg) {
                incoming
                    .push(msg)
                    .catch((e) =>
                        console.error("Error while pushing incoming msg", e),
                    );
            }
        }
    });

    const websocketOpen = new Promise<void>((resolve) => {
        if (websocket.readyState === 1) {
            resolve();
        } else {
            websocket.addEventListener("open", resolve, { once: true });
        }
    });

    const outgoingMessages = new BatchedOutgoingMessages((messages) => {
        if (websocket.readyState === 1) {
            websocket.send(messages);
        }
    });

    async function pushMessage(msg: SyncMessage) {
        if (websocket.readyState !== 1) {
            await websocketOpen;
        }

        while (
            websocket.bufferedAmount > BUFFER_LIMIT &&
            websocket.readyState === 1
        ) {
            await new Promise<void>((resolve) =>
                setTimeout(resolve, BUFFER_LIMIT_POLLING_INTERVAL),
            );
        }

        if (websocket.readyState !== 1) {
            return;
        }

        if (!supportsBatching) {
            websocket.send(JSON.stringify(msg));
        } else {
            outgoingMessages.push(msg);
        }
    }

    return {
        id,
        incoming,
        outgoing: {
            push: pushMessage,
            close() {
                console.log("Trying to close", id, websocket.readyState);
                if (supportsBatching) {
                    outgoingMessages.close();
                }

                if (websocket.readyState === 0) {
                    websocket.addEventListener(
                        "open",
                        function handleClose() {
                            websocket.close();
                        },
                        { once: true },
                    );
                } else if (websocket.readyState == 1) {
                    websocket.close();
                }
            },
        },
        role,
        crashOnClose: false,
    };
}
