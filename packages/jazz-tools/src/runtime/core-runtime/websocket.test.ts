import { describe, expect, it } from "vitest";
import { PostcardWriter } from "./core-codec.js";
import {
  WebSocketCarrier,
  decodeWireError,
  decodeWebSocketFrameBatch,
  webSocketUrl,
  encodeWireClientHello,
  encodeWebSocketPrelude,
  encodeWebSocketFrameBatch,
  isWireHello,
  isWireMessage,
} from "./websocket.js";

describe("websocket frame carrier", () => {
  it("encodes websocket messages as postcard batches of encoded frames", () => {
    const frames = [Uint8Array.from([1, 2, 3]), Uint8Array.from([4, 5])];

    const decoded = decodeWebSocketFrameBatch(encodeWebSocketFrameBatch(frames));

    expect(decoded.map((frame) => [...frame])).toEqual([
      [1, 2, 3],
      [4, 5],
    ]);
  });

  it("uses app-scoped websocket URLs without identity query parameters", () => {
    expect(webSocketUrl("http://127.0.0.1:4200", "app-a")).toBe(
      "ws://127.0.0.1:4200/apps/app-a/ws",
    );
  });

  it("encodes peer identity and alpha-shaped auth in the websocket prelude", () => {
    expect(
      JSON.parse(
        new TextDecoder().decode(
          encodeWebSocketPrelude('{"admin_secret":"s"}', Uint8Array.from([0, 1, 10, 255])),
        ),
      ),
    ).toEqual({
      peer_identity: "00010aff",
      auth: { admin_secret: "s" },
    });
  });

  it("encodes the client wire hello as a websocket-negotiation frame", () => {
    const hello = encodeWireClientHello();

    expect(isWireHello(hello)).toBe(true);
    expect(isWireMessage(hello)).toBe(false);
  });

  it("decodes structured wire error frames", () => {
    expect(decodeWireError(encodeWireError(3, 1, "bad credentials"))).toEqual({
      code: "auth_failed",
      retry: "after_auth",
      message: "bad credentials",
    });
  });

  it("surfaces structured wire error frames without forwarding them as payload frames", async () => {
    let socket: MessageWebSocket | undefined;
    const frames: Uint8Array[] = [];
    const errors: unknown[] = [];
    new WebSocketCarrier({
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

    socket!.emitMessage(encodeWebSocketFrameBatch([encodeWireError(3, 1, "expired")]));
    await Promise.resolve();

    expect(frames).toEqual([]);
    expect(errors).toEqual([{ code: "auth_failed", retry: "after_auth", message: "expired" }]);
  });
});

function encodeWireError(code: number, retry: number, message: string): Uint8Array {
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
