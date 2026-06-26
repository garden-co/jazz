import { describe, expect, it } from "vitest";
import {
  decodeDirectWebSocketFrameBatch,
  directWebSocketUrl,
  encodeDirectWebSocketFrameBatch,
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

  it("uses app-scoped websocket URLs and carries the direct peer identity", () => {
    expect(
      directWebSocketUrl(
        "http://127.0.0.1:4200",
        "app-a",
        Uint8Array.from([0, 1, 10, 255]),
      ),
    ).toBe("ws://127.0.0.1:4200/apps/app-a/ws?identity=00010aff");
  });
});
