import type { JazzWebSocketUpgrade } from "./core-provider.js";
import type { WorkerdWebSocket } from "./core-transport.js";
import type { WsLikeSocket } from "./create-jazz.js";

declare const WebSocketPair: new () => Record<0 | 1, WorkerdWebSocket>;

export const cloudflareWebSocketUpgrade: JazzWebSocketUpgrade = (
  _request,
  accept,
  { maxPayload },
) => {
  const pair = new WebSocketPair();
  const client = pair[0];
  const server = pair[1];
  // Binary messages as ArrayBuffers, not workerd's default Blobs.
  server.binaryType = "arraybuffer";
  server.accept();
  accept(wsLike(server, maxPayload));
  return new Response(null, { status: 101, webSocket: client } as ResponseInit);
};

const textEncoder = new TextEncoder();

/**
 * workerd's server socket in the `ws` package's shape, which `acceptSocket`
 * takes. It has no `pause`/`resume`: workerd can't stop reading an accepted
 * socket. A client that ignores flow control still can't grow its session's
 * queue without bound: past `MAX_WAITING_SESSION_BYTES` its session ends.
 */
function wsLike(ws: WorkerdWebSocket, maxPayload: number): WsLikeSocket {
  const socket = {
    send: (data: Uint8Array) => ws.send(data),
    close: (code?: number, reason?: string) => {
      try {
        ws.close(code, reason);
      } catch {
        // Already closed.
      }
    },
    on(event: "message" | "close" | "error", listener: (...args: never[]) => void) {
      const call = listener as (...args: unknown[]) => void;
      if (event === "message") {
        // As the `ws` package hands them over: bytes, text included.
        ws.addEventListener("message", ({ data }) => {
          const bytes = typeof data === "string" ? textEncoder.encode(data) : data;
          if (!(bytes instanceof ArrayBuffer) && !ArrayBuffer.isView(bytes))
            return socket.close(1003, "unsupported message");
          if (bytes.byteLength > maxPayload) return socket.close(1009, "message too large");
          call(bytes, typeof data !== "string");
        });
      } else if (event === "close") {
        ws.addEventListener("close", () => call());
      } else {
        ws.addEventListener("error", (error) => call(error));
      }
      return socket;
    },
  };
  return socket as WsLikeSocket;
}
