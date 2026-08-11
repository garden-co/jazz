import { readFileSync } from "node:fs";
import { describe, expect, expectTypeOf, it } from "vitest";
import type { BrowserWebSocket } from "./websocket.js";
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
  it("types close listeners with close event details", () => {
    type CloseListener = Parameters<BrowserWebSocket["addEventListener"]>[1];

    expectTypeOf<Parameters<CloseListener>[0]>().toEqualTypeOf<{
      code: number;
      reason: string;
    }>();
  });

  it("encodes websocket messages as postcard batches of encoded frames", () => {
    const frames = [Uint8Array.from([1, 2, 3]), Uint8Array.from([4, 5])];

    const decoded = decodeWebSocketFrameBatch(encodeWebSocketFrameBatch(frames));

    expect(decoded.map((frame) => [...frame])).toEqual([
      [1, 2, 3],
      [4, 5],
    ]);
  });

  // This is intentionally transport-level: the public Db API cannot expose
  // individual WebSocket message boundaries, which are the limit being kept.
  it("splits a burst of wire frames into server-sized websocket messages", async () => {
    let socket: RecordingWebSocket | undefined;
    const carrier = new WebSocketCarrier({
      endpointUrl: "ws://127.0.0.1:4200/apps/app-a/ws",
      peerIdentity: new Uint8Array(16),
      onFrame: () => {},
      WebSocket: class extends RecordingWebSocket {
        constructor(url: string) {
          super(url, (created) => {
            socket = created;
          });
        }
      },
    });

    await carrier.ready();
    socket!.sent.length = 0;
    const frames = [new Uint8Array(600_000), new Uint8Array(600_000), new Uint8Array(600_000)];
    await carrier.sendBatch(frames);

    const batches = socket!.sent.filter(
      (message): message is Uint8Array => message instanceof Uint8Array,
    );
    expect(batches).toHaveLength(3);
    expect(batches.every((batch) => batch.byteLength <= 1 << 20)).toBe(true);
    expect(batches.flatMap((batch) => decodeWebSocketFrameBatch(batch))).toEqual(frames);
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
    expect(reader.option((authority) => authority.bytes(false))).toBeUndefined();
  });

  it("sends an authority-unbound hello first on every reconnect", async () => {
    const sockets: RecordingWebSocket[] = [];
    const peerIdentity = Uint8Array.from({ length: 16 }, (_, index) => index + 1);
    const WebSocket = class extends RecordingWebSocket {
      constructor(url: string) {
        super(url, (socket) => sockets.push(socket));
      }
    };

    const first = new WebSocketCarrier({
      endpointUrl: "ws://127.0.0.1:4200/apps/app-a/ws",
      peerIdentity,
      onFrame: () => {},
      WebSocket,
    });
    await first.ready();
    first.close();
    const second = new WebSocketCarrier({
      endpointUrl: "ws://127.0.0.1:4200/apps/app-a/ws",
      peerIdentity,
      onFrame: () => {},
      WebSocket,
    });
    await second.ready();

    const hello = (socket: RecordingWebSocket) => {
      expect(socket.sent[0]).toEqual(encodeWebSocketPrelude("{}", peerIdentity));
      const frame = decodeWebSocketFrameBatch(socket.sent[1] as Uint8Array)[0]!;
      const reader = new PostcardReader(frame);
      expect(reader.u64()).toBe(0);
      reader.u64();
      reader.u64();
      reader.u64();
      reader.u64();
      return reader.option((authority) => authority.bytes(false));
    };
    expect(hello(sockets[0]!)).toBeUndefined();
    expect(hello(sockets[1]!)).toBeUndefined();
    const firstNegotiation = await first.ready();
    const secondNegotiation = await second.ready();
    expect(firstNegotiation.authority?.node).toEqual(Uint8Array.from({ length: 16 }, () => 0x5e));
    expect(secondNegotiation.authority?.epoch).toBeGreaterThan(
      firstNegotiation.authority?.epoch ?? 0n,
    );
  });

  it("preserves full u64 server authority epochs across stale/current hellos", async () => {
    const staleEpoch = 9_007_199_254_740_993n;
    const currentEpoch = staleEpoch + 1n;
    const sockets: MessageWebSocket[] = [];
    const WebSocket = class extends MessageWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    };
    const stale = new WebSocketCarrier({
      endpointUrl: "ws://127.0.0.1:4200/apps/app-a/ws",
      peerIdentity: new Uint8Array(16),
      onFrame: () => {},
      WebSocket,
    });
    sockets[0]!.emitMessage(encodeWebSocketFrameBatch([encodeServerHello(staleEpoch)]));
    const current = new WebSocketCarrier({
      endpointUrl: "ws://127.0.0.1:4200/apps/app-a/ws",
      peerIdentity: new Uint8Array(16),
      onFrame: () => {},
      WebSocket,
    });
    sockets[1]!.emitMessage(encodeWebSocketFrameBatch([encodeServerHello(currentEpoch)]));

    const staleNegotiation = await stale.ready();
    const currentNegotiation = await current.ready();

    expect(staleNegotiation.authority?.epoch).toBe(staleEpoch);
    expect(currentNegotiation.authority?.epoch).toBe(currentEpoch);
    expect(currentNegotiation.authority!.epoch > staleNegotiation.authority!.epoch).toBe(true);

    const writer = new PostcardWriter();
    writer.u64(staleEpoch);
    const encodedEpoch = writer.finish();
    expect(new PostcardReader(encodedEpoch).u64BigInt()).toBe(staleEpoch);
    expect(BigInt(new PostcardReader(encodedEpoch).u64())).not.toBe(staleEpoch);
  });

  it("does not send or deliver semantic frames before the server hello", async () => {
    let socket: MessageWebSocket | undefined;
    const delivered: Uint8Array[] = [];
    const carrier = new WebSocketCarrier({
      endpointUrl: "ws://127.0.0.1:4200/apps/app-a/ws",
      peerIdentity: new Uint8Array(16),
      onFrame: (frame) => delivered.push(frame),
      WebSocket: class extends MessageWebSocket {
        constructor(url: string) {
          super(url, (created) => {
            socket = created;
          });
        }
      },
    });

    socket!.emitMessage(encodeWebSocketFrameBatch([Uint8Array.of(1, 6, 0, 0)]));
    await expect(carrier.ready()).rejects.toThrow("before server hello");
    await expect(carrier.send(Uint8Array.of(1))).rejects.toThrow("before server hello");
    expect(delivered).toEqual([]);
    expect(socket!.closed).toBe(true);
  });

  it("surfaces an authentication failure before server hello without negotiating", async () => {
    let socket: MessageWebSocket | undefined;
    const frames: Uint8Array[] = [];
    const errors: unknown[] = [];
    const carrier = new WebSocketCarrier({
      endpointUrl: "ws://127.0.0.1:4200/apps/app-a/ws",
      peerIdentity: new Uint8Array(16),
      onFrame: (frame) => frames.push(frame),
      onError: (error) => errors.push(error),
      WebSocket: class extends MessageWebSocket {
        constructor(url: string) {
          super(url, (created) => {
            socket = created;
          });
        }
      },
    });

    socket!.emitMessage(encodeWebSocketFrameBatch([encodeWireError(3, 1, "invalid token")]));

    await expect(carrier.ready()).rejects.toThrow("authentication failed before server hello");
    socket!.emitMessage(encodeWebSocketFrameBatch([encodeServerHello(1n)]));
    socket!.emitMessage(encodeWebSocketFrameBatch([Uint8Array.of(1, 6, 0, 0)]));
    await Promise.resolve();

    expect(errors).toEqual([
      { code: "auth_failed", retry: "after_auth", message: "invalid token" },
    ]);
    expect(frames).toEqual([]);
    expect(socket!.closed).toBe(true);
  });

  it("rejects non-authentication errors before server hello", async () => {
    let socket: MessageWebSocket | undefined;
    const errors: unknown[] = [];
    const carrier = new WebSocketCarrier({
      endpointUrl: "ws://127.0.0.1:4200/apps/app-a/ws",
      peerIdentity: new Uint8Array(16),
      onFrame: () => {},
      onError: (error) => errors.push(error),
      WebSocket: class extends MessageWebSocket {
        constructor(url: string) {
          super(url, (created) => {
            socket = created;
          });
        }
      },
    });

    socket!.emitMessage(
      encodeWebSocketFrameBatch([encodeWireError(5, 3, "conflicting commit unit")]),
    );

    await expect(carrier.ready()).rejects.toThrow("semantic frame before server hello");
    expect(errors).toEqual([]);
    expect(socket!.closed).toBe(true);
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
          super(url, (created) => {
            socket = created;
          });
        }
      },
    });

    socket!.emitMessage(
      encodeWebSocketFrameBatch([encodeServerHello(1n), encodeWireError(3, 1, "expired")]),
    );
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
    expect(payload[0]).toBe(14);
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

function encodeServerHello(epoch: bigint): Uint8Array {
  const writer = new PostcardWriter();
  writer.u64(0); // WireFrame::Hello
  writer.u64(WIRE_PROTOCOL_VERSION);
  writer.u64(WIRE_PROTOCOL_VERSION);
  writer.u64(CLIENT_WIRE_FEATURES);
  writer.u64(1); // WirePeerRole::Core
  writer.some((authority) => {
    authority.bytes(
      Uint8Array.from({ length: 16 }, () => 0x5e),
      false,
    );
    authority.u64(epoch);
  });
  return writer.finish();
}

class MessageWebSocket {
  binaryType: "arraybuffer" | "blob" = "arraybuffer";
  readonly readyState = 1;
  private readonly messageListeners: Array<(event: { data: unknown }) => void> = [];

  constructor(
    readonly url: string,
    onCreate?: (socket: MessageWebSocket) => void,
  ) {
    onCreate?.(this);
  }

  send(_data: Uint8Array | string): void {}

  closed = false;

  close(): void {
    this.closed = true;
  }

  addEventListener(type: "open", listener: () => void): void;
  addEventListener(type: "message", listener: (event: { data: unknown }) => void): void;
  addEventListener(type: "error", listener: (event: unknown) => void): void;
  addEventListener(
    type: "close",
    listener: (event: { code: number; reason: string }) => void,
  ): void;
  addEventListener(type: string, listener: unknown): void {
    if (type === "message") {
      this.messageListeners.push(listener as (event: { data: unknown }) => void);
    }
  }

  emitMessage(data: Uint8Array): void {
    for (const listener of this.messageListeners) listener({ data });
  }
}

class RecordingWebSocket extends MessageWebSocket {
  sent: Array<Uint8Array | string> = [];

  constructor(url: string, onCreate?: (socket: RecordingWebSocket) => void) {
    super(url);
    onCreate?.(this);
  }

  override send(data: Uint8Array | string): void {
    this.sent.push(data);
    if (data instanceof Uint8Array && isWireHello(decodeWebSocketFrameBatch(data)[0]!)) {
      this.emitMessage(encodeWebSocketFrameBatch([encodeServerHello(RecordingWebSocket.epoch++)]));
    }
  }

  private static epoch = 1n;
}
