import type { WebSocketConstructor } from "./native-runtime/websocket.js";

/**
 * Transports for a core that runs in this process (see `createJazz` in
 * `jazz-tools/backend`). Such a core gets an origin under `.invalid`, which
 * never resolves, and every client in this process that is pointed at that
 * origin reaches the core through these functions instead of the network.
 */
export interface InProcessTransport {
  fetch: typeof fetch;
  WebSocket: WebSocketConstructor;
}

const transports = new Map<string, InProcessTransport>();

/** Route `origin` (an `http://<name>.invalid` origin) to an in-process core. */
export function registerInProcessTransport(
  origin: string,
  transport: InProcessTransport,
): () => void {
  const host = new URL(origin).host;
  if (!host.endsWith(".invalid")) {
    throw new Error(`In-process origins must be under .invalid, got ${origin}`);
  }
  transports.set(host, transport);
  return () => {
    if (transports.get(host) === transport) transports.delete(host);
  };
}

function transportFor(url: string): InProcessTransport | undefined {
  if (transports.size === 0) return undefined;
  try {
    return transports.get(new URL(url).host);
  } catch {
    return undefined;
  }
}

/** `fetch` for `url`: the in-process core's when `url` names one. */
export function fetchFor(url: string): typeof fetch {
  return transportFor(url)?.fetch ?? globalThis.fetch;
}

/** The WebSocket constructor for `url` when it names an in-process core. */
export function inProcessWebSocketFor(url: string): WebSocketConstructor | undefined {
  return transportFor(url)?.WebSocket;
}
