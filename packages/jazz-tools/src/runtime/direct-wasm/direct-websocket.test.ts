import { describe, expect, it } from "vitest";
import { PostcardWriter } from "./direct-codec.js";
import {
  DirectWebSocketCarrier,
  decodeDirectWireError,
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

  it("decodes structured wire error frames", () => {
    expect(decodeDirectWireError(encodeDirectWireError(3, 1, "bad credentials"))).toEqual({
      code: "auth_failed",
      retry: "after_auth",
      message: "bad credentials",
    });
  });

  it("surfaces structured wire error frames without forwarding them as payload frames", async () => {
    let socket: MessageWebSocket | undefined;
    const frames: Uint8Array[] = [];
    const errors: unknown[] = [];
    new DirectWebSocketCarrier({
      endpointUrl: "ws://127.0.0.1:4200/apps/app-a/ws",
      peerIdentity: new Uint8Array(16),
      onFrame: (frame) => frames.push(frame),
      onError: (error) => errors.push(error),
      WebSocket: class extends MessageWebSocket {
        constructor(url: string) {
          super(url);
          socket = this;
        }
      },
    });

    socket!.emitMessage(encodeDirectWebSocketFrameBatch([encodeDirectWireError(3, 1, "expired")]));
    await Promise.resolve();

    expect(frames).toEqual([]);
    expect(errors).toEqual([{ code: "auth_failed", retry: "after_auth", message: "expired" }]);
  });
});

function encodeDirectWireError(code: number, retry: number, message: string): Uint8Array {
  const writer = new PostcardWriter();
  writer.u64(2);
  writer.u64(code);
  writer.u64(retry);
  writer.string(message);
  return writer.finish();
}

class MessageWebSocket {
  binaryType: "arraybuffer" | "blob" = "arraybuffer";
  readonly readyState = 1;
  private readonly messageListeners: Array<(event: { data: unknown }) => void> = [];

  constructor(readonly url: string) {}

  send(_data: Uint8Array): void {}

  close(): void {}

  addEventListener(type: string, listener: (event: { data: unknown }) => void): void {
    if (type === "message") this.messageListeners.push(listener);
  }

  emitMessage(data: Uint8Array): void {
    for (const listener of this.messageListeners) listener({ data });
  }
}
