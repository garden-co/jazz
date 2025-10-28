import type { AnyWebSocket } from "./types.js";

export const BUFFER_LIMIT = 100_000;
export const BUFFER_LIMIT_POLLING_INTERVAL = 10;

export function isWebSocketOpen(websocket: AnyWebSocket) {
  return websocket.readyState === 1;
}

export function hasWebSocketTooMuchBufferedData(websocket: AnyWebSocket) {
  return websocket.bufferedAmount > BUFFER_LIMIT && isWebSocketOpen(websocket);
}

export function waitForWebSocketOpen(websocket: AnyWebSocket) {
  return new Promise<void>((resolve) => {
    if (websocket.readyState === 1) {
      resolve();
    } else {
      websocket.addEventListener("open", () => resolve(), { once: true });
    }
  });
}

export async function waitForWebSocketBufferedAmount(websocket: AnyWebSocket) {
  while (hasWebSocketTooMuchBufferedData(websocket)) {
    await new Promise<void>((resolve) =>
      setTimeout(resolve, BUFFER_LIMIT_POLLING_INTERVAL),
    );
  }
}
