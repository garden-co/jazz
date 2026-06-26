import { describe, expect, it } from "vitest";
import {
  decodeDirectWebSocketFrameBatch,
  directWebSocketEndpointUrl,
  directWebSocketUrl,
  encodeDirectWireClientHello,
  encodeDirectWebSocketPrelude,
  encodeDirectWebSocketFrameBatch,
  isDirectWireHello,
  isDirectWireMessage,
} from "./direct-websocket.js";

describe("direct websocket frame carrier", () => {
  it("encodes websocket messages as postcard batches of encoded frames", () => {
    const frames = [Uint8Array.from([1, 2, 3]), Uint8Array.from([4, 5])];

    const decoded = decodeDirectWebSocketFrameBatch(encodeDirectWebSocketFrameBatch(frames));

    expect(decoded.map((frame) => [...frame])).toEqual([
      [1, 2, 3],
      [4, 5],
    ]);
  });

  it("uses app-scoped websocket URLs without identity query parameters", () => {
    expect(
      directWebSocketUrl("http://127.0.0.1:4200", "app-a", Uint8Array.from([0, 1, 10, 255])),
    ).toBe("ws://127.0.0.1:4200/apps/app-a/ws");
  });

  it("leaves already scoped websocket endpoints unchanged", () => {
    expect(
      directWebSocketEndpointUrl(
        "ws://127.0.0.1:4200/apps/app-a/ws",
        Uint8Array.from([0, 1, 10, 255]),
      ),
    ).toBe("ws://127.0.0.1:4200/apps/app-a/ws");
  });

  it("encodes peer identity and alpha-shaped auth in the direct websocket prelude", () => {
    expect(
      JSON.parse(
        new TextDecoder().decode(
          encodeDirectWebSocketPrelude('{"admin_secret":"s"}', Uint8Array.from([0, 1, 10, 255])),
        ),
      ),
    ).toEqual({
      peer_identity: "00010aff",
      auth: { admin_secret: "s" },
    });
  });

  it("encodes the client wire hello as a websocket-negotiation frame", () => {
    const hello = encodeDirectWireClientHello();

    expect(isDirectWireHello(hello)).toBe(true);
    expect(isDirectWireMessage(hello)).toBe(false);
  });
});
