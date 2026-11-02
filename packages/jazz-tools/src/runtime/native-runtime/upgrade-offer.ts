/**
 * What a client puts on its WebSocket upgrade (SPEC 8 §8.1.1). Kept apart
 * from `websocket.ts` so the WebSocket-shaped transports (the HTTP stream,
 * in-process sockets) can read an offer without importing the carrier.
 */

/** `{ protocols, headers }`, as Node 22+, Bun and Deno WebSockets take it. */
export type WebSocketInitWithHeaders = {
  protocols?: string | string[];
  headers?: Record<string, string>;
};

/** What a client puts on its upgrade request. */
export type WebSocketUpgradeOffer = {
  /** `Sec-WebSocket-Protocol` tokens, in preference order. */
  protocols: string[];
  /** Credential headers; empty for a browser-held credential. */
  headers: Record<string, string>;
};

/** The HTTP stream transport offers the subprotocol tokens in this header. */
export const HTTP_STREAM_PROTOCOL_HEADER = "x-jazz-protocol";

/** The upgrade offer a WebSocket-shaped transport was constructed with. */
export function upgradeOfferFromInit(
  protocols?: string | string[] | WebSocketInitWithHeaders,
): WebSocketUpgradeOffer {
  if (protocols === undefined) return { protocols: [], headers: {} };
  if (typeof protocols === "string") return { protocols: [protocols], headers: {} };
  if (Array.isArray(protocols)) return { protocols: [...protocols], headers: {} };
  const list = protocols.protocols;
  return {
    protocols: list === undefined ? [] : typeof list === "string" ? [list] : [...list],
    headers: { ...protocols.headers },
  };
}
