import type { WsLikeSocket } from "./create-jazz.js";

/** How long before the deadline a socket starts looking for a quiet moment to close. */
export const DEADLINE_CLOSE_WINDOW_MS = 45_000;
/** How long before the deadline a socket closes, quiet or not. */
export const DEADLINE_CLOSE_MARGIN_MS = 15_000;
/** No frame either way for this long counts as quiet. */
export const DEADLINE_QUIET_MS = 500;
const CHECK_EVERY_MS = 100;

/**
 * `ws`, closed (1012, "reconnect") a little before `deadline`.
 *
 * Vercel ends a function at its deadline (the route's `maxDuration`), and
 * its WebSockets with it. Measured on a deployment, a socket stops being
 * answered at about the deadline and closes as 1006 some 15 s later.
 * Waiting that out stalls the client, and what it sends in that time is
 * lost. So close first, while the socket still works: the client
 * reconnects at once to an instance with a fresh deadline. From
 * `DEADLINE_CLOSE_WINDOW_MS` before, close at the first moment no frame
 * has passed either way for `DEADLINE_QUIET_MS`, so a request is unlikely
 * to be in flight. At `DEADLINE_CLOSE_MARGIN_MS` before, close anyway.
 * A short `maxDuration` shrinks both, to a share of the socket's lifetime.
 */
export function closingBeforeDeadline(
  ws: WsLikeSocket,
  deadline: number,
  now: () => number = Date.now,
): WsLikeSocket {
  const opened = now();
  const lifetime = Math.max(0, deadline - opened);
  const hardAt = deadline - Math.min(DEADLINE_CLOSE_MARGIN_MS, lifetime * 0.1);
  const quietFrom = deadline - Math.min(DEADLINE_CLOSE_WINDOW_MS, lifetime * 0.3);
  let lastFrame = opened;
  let check: ReturnType<typeof setInterval> | undefined;
  const close = () => {
    stop();
    ws.close(1012, "reconnect: the function reaches its deadline");
  };
  const stop = () => {
    clearTimeout(start);
    clearInterval(check);
  };
  const start = setTimeout(
    () => {
      check = setInterval(() => {
        const at = now();
        if (at >= hardAt || at - lastFrame >= DEADLINE_QUIET_MS) close();
      }, CHECK_EVERY_MS);
    },
    Math.max(0, quietFrom - opened),
  );
  ws.on("close", stop);
  const socket = {
    send(data: Uint8Array) {
      lastFrame = now();
      ws.send(data);
    },
    close: (code?: number, reason?: string) => {
      stop();
      ws.close(code, reason);
    },
    on(event: "message" | "close" | "error", listener: (...args: never[]) => void) {
      if (event === "message") {
        const onMessage = listener as (data: unknown, isBinary?: boolean) => void;
        ws.on("message", (data, isBinary) => {
          lastFrame = now();
          onMessage(data, isBinary);
        });
      } else {
        ws.on(event as "close", listener as () => void);
      }
      return socket;
    },
  } as WsLikeSocket;
  return socket;
}
