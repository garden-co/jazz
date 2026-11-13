import type { UniversalWebSocket } from "rivetkit";
import type { WsLikeSocket } from "jazz-tools/backend";

const MAX_BYTES = 2 * 1024 * 1024;
const MAX_MESSAGES = 64;

/** Rivet keeps /request on upgrades, but strips it on ordinary HTTP. */
export function jazzRequest(request: Request): Request {
  const url = new URL(request.url);
  if (url.pathname === "/request" || url.pathname.startsWith("/request/")) {
    url.pathname = url.pathname.slice("/request".length) || "/";
  }
  return new Request(url, request);
}

/** Bound messages while Jazz pauses delivery; overflow makes the client reconnect. */
export function jazzSocket(socket: UniversalWebSocket): WsLikeSocket {
  socket.binaryType = "arraybuffer";
  let paused = false;
  let closed = false;
  let bytes = 0;
  const pending: Array<{ data: string | ArrayBuffer; size: number }> = [];
  let messageListener: ((data: unknown, binary?: boolean) => void) | undefined;
  let closeListener: (() => void) | undefined;
  let errorListener: ((error: unknown) => void) | undefined;
  function close(code?: number, reason?: string) {
    if (closed) return;
    closed = true;
    pending.length = 0;
    bytes = 0;
    socket.close(code, reason);
  }
  function drain() {
    while (!closed && !paused && messageListener && pending.length) {
      const message = pending.shift()!;
      bytes -= message.size;
      messageListener(message.data, typeof message.data !== "string");
    }
  }
  socket.addEventListener("message", (event) => {
    if (closed) return;
    const data: unknown = event.data;
    if (typeof data !== "string" && !(data instanceof ArrayBuffer)) {
      close(1003, "unsupported message type");
      return;
    }
    const size =
      typeof data === "string" ? new TextEncoder().encode(data).byteLength : data.byteLength;
    if (size > MAX_BYTES || bytes + size > MAX_BYTES || pending.length >= MAX_MESSAGES) {
      close(1009, "incoming buffer limit");
      return;
    }
    pending.push({ data, size });
    bytes += size;
    drain();
  });
  socket.addEventListener("close", () => {
    closed = true;
    pending.length = 0;
    bytes = 0;
    closeListener?.();
  });
  socket.addEventListener("error", (error) => errorListener?.(error));
  return {
    send(data) {
      if (closed) return;
      if (socket.bufferedAmount + data.byteLength > MAX_BYTES) {
        close(1013, "outgoing buffer limit");
        return;
      }
      socket.send(data);
    },
    close,
    pause: () => {
      paused = true;
    },
    resume: () => {
      paused = false;
      drain();
    },
    on(event: string, listener: (...args: any[]) => void) {
      if (event === "message") {
        messageListener = listener;
        drain();
      } else if (event === "close") {
        closeListener = listener;
        if (closed) listener();
      } else if (event === "error") errorListener = listener;
    },
  };
}
