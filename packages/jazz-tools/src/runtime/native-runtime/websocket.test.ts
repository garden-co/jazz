import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { PostcardReader, PostcardWriter } from "./native-codec.js";
import {
  CLIENT_WIRE_FEATURES,
  FEATURE_SYNC_MESSAGE_PAYLOAD,
  MAX_WIRE_PROTOCOL_VERSION,
  MIN_WIRE_PROTOCOL_VERSION,
  WIRE_PROTOCOL_VERSION,
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

  it("encodes the websocket auth prelude as the server AuthHandshake shape", () => {
    expect(
      JSON.parse(encodeWebSocketPrelude('{"admin_secret":"s"}', Uint8Array.from([0, 1, 10, 255]))),
    ).toEqual({
      peer_identity: "00010aff",
      auth: { sub: "00010aff", admin_secret: "s" },
      sub: "00010aff",
      admin_secret: "s",
    });
  });

  it("uses the JWT subject for the websocket auth prelude when present", () => {
    const token = `header.${btoa(JSON.stringify({ sub: "user-123" }))}.sig`;

    expect(
      JSON.parse(encodeWebSocketPrelude(JSON.stringify({ jwt_token: token }), Uint8Array.of(1))),
    ).toEqual({
      peer_identity: "01",
      auth: { sub: "user-123", jwt_token: token },
      sub: "user-123",
      jwt_token: token,
    });
  });

  it("encodes the client wire hello as a websocket-negotiation frame", () => {
    const hello = encodeWireClientHello();
    const reader = new PostcardReader(hello);

    expect(isWireHello(hello)).toBe(true);
    expect(isWireMessage(hello)).toBe(false);
    expect(reader.u64()).toBe(0);
    expect(reader.u64()).toBe(MIN_WIRE_PROTOCOL_VERSION);
    expect(reader.u64()).toBe(MAX_WIRE_PROTOCOL_VERSION);
    expect(reader.u64()).toBe(CLIENT_WIRE_FEATURES);
    expect(reader.u64()).toBe(0);
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

  it("round-trips run-bearing Rust wire fixtures through the TS websocket frame codec", () => {
    const manifest = rustWireFixtureManifest();
    const fixture = manifest.fixtures.find(
      (candidate) => candidate.name === "view_update_mixed_version_carrier_runs",
    );

    expect(manifest.protocol_version).toBe(WIRE_PROTOCOL_VERSION);
    expect(fixture?.message_family).toBe("ViewUpdate");
    expect(fixture?.decoded_debug).toContain("VersionBundleRun");

    const frame = hexToBytes(fixture!.frame_hex);
    expect(isWireMessage(frame)).toBe(true);
    expect([...decodeWebSocketFrameBatch(encodeWebSocketFrameBatch([frame]))[0]!]).toEqual([
      ...frame,
    ]);

    const reader = new PostcardReader(frame);
    expect(reader.u64()).toBe(1);
    expect(reader.u64()).toBe(WIRE_PROTOCOL_VERSION);
    expect(reader.u64()).toBe(FEATURE_SYNC_MESSAGE_PAYLOAD);
    expect(reader.option(() => "session")).toBeUndefined();
    const payload = reader.bytes();
    expect(payload[0]).toBe(11);
  });
});

type RustWireFixtureManifest = {
  protocol_version: number;
  fixtures: Array<{
    name: string;
    message_family: string;
    frame_hex: string;
    decoded_debug: string;
  }>;
};

function rustWireFixtureManifest(): RustWireFixtureManifest {
  return JSON.parse(
    readFileSync(
      new URL("../../../../../crates/jazz/fixtures/wire_message_frames.json", import.meta.url),
      "utf8",
    ),
  ) as RustWireFixtureManifest;
}

function hexToBytes(hex: string): Uint8Array {
  const bytes = new Uint8Array(hex.length / 2);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}

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

  send(_data: Uint8Array | string): void {}

  close(): void {}

  addEventListener(type: string, listener: (event: { data: unknown }) => void): void {
    if (type === "message") this.messageListeners.push(listener);
  }

  emitMessage(data: Uint8Array): void {
    for (const listener of this.messageListeners) listener({ data });
  }
}
