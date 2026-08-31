import { readFileSync } from "node:fs";
import { describe, expect, expectTypeOf, it } from "vitest";
import type { BrowserWebSocket } from "./websocket.js";
import { PostcardReader, PostcardWriter } from "./native-codec.js";
import {
  CLIENT_WIRE_FEATURES,
  FEATURE_PAYLOAD_ZSTD,
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
  isWireError,
  isWireMessage,
  peerIdentityForWebSocketAuth,
} from "./websocket.js";

function authorBytes(issuer: string, subject: string): Uint8Array {
  return new TextEncoder().encode(JSON.stringify([issuer, subject]));
}

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

    const suffixed = Uint8Array.from([...encodeWebSocketFrameBatch(frames), 0]);
    expect(new PostcardReader(suffixed).readVec((reader) => reader.bytes())).toHaveLength(2);
    expect(() => decodeWebSocketFrameBatch(suffixed)).toThrow(
      "websocket frame batch has trailing postcard bytes",
    );
  });

  it("bounds and canonicalizes inbound websocket batches before retaining frames", () => {
    expect(() => encodeWebSocketFrameBatch([])).toThrow(
      "websocket frame batch exceeds frame-count limit of 4096",
    );
    expect(() =>
      encodeWebSocketFrameBatch(Array.from({ length: 4097 }, () => new Uint8Array())),
    ).toThrow("websocket frame batch exceeds frame-count limit of 4096");

    // The count is intentionally not followed by any elements: this proves
    // the carrier rejects it before an attacker-declared frame array is made.
    expect(() => decodeWebSocketFrameBatch(Uint8Array.of(0x81, 0x20))).toThrow(
      "websocket frame batch exceeds frame-count limit of 4096",
    );
    expect(() => decodeWebSocketFrameBatch(Uint8Array.of(0))).toThrow(
      "websocket frame batch exceeds frame-count limit of 4096",
    );
    expect(() => decodeWebSocketFrameBatch(Uint8Array.of(0x81, 0, 1, 0x42))).toThrow(
      "postcard u64 is not minimally encoded",
    );
    expect(() => decodeWebSocketFrameBatch(Uint8Array.of(1, 0x81, 0, 0x42))).toThrow(
      "postcard u64 is not minimally encoded",
    );
    expect(() => decodeWebSocketFrameBatch(Uint8Array.of(1, 2, 0x42))).toThrow(
      "postcard bytes overflow",
    );

    // 2 MiB + 1, encoded in postcard's canonical varint form.
    expect(() => decodeWebSocketFrameBatch(Uint8Array.of(1, 0x81, 0x80, 0x80, 1))).toThrow(
      "websocket frame exceeds maximum length of 2097152 bytes",
    );

    const largestSingleton = new Uint8Array(2 * 1024 * 1024 - 4);
    const exactCarrier = encodeWebSocketFrameBatch([largestSingleton]);
    expect(exactCarrier.byteLength).toBe(2 * 1024 * 1024);
    expect(decodeWebSocketFrameBatch(exactCarrier)).toEqual([largestSingleton]);

    const rawLimit = new Uint8Array(2 * 1024 * 1024);
    expect(() => encodeWebSocketFrameBatch([rawLimit])).toThrow(
      "websocket frame batch exceeds maximum length of 2097152 bytes",
    );
    expect(() => encodeWebSocketFrameBatch([Uint8Array.of(0x11), rawLimit])).toThrow(
      "websocket frame batch exceeds maximum length of 2097152 bytes",
    );
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
    const frames = [0x11, 0x22, 0x33].map((sentinel) => {
      const frame = new Uint8Array(600_000);
      frame[0] = sentinel;
      frame[frame.byteLength - 1] = sentinel ^ 0xff;
      return frame;
    });
    await carrier.sendBatch(frames);

    const batches = socket!.sent.filter(
      (message): message is Uint8Array => message instanceof Uint8Array,
    );
    expect(batches).toHaveLength(3);
    expect(batches.every((batch) => batch.byteLength <= 1 << 20)).toBe(true);
    const decoded = batches.flatMap((batch) => decodeWebSocketFrameBatch(batch));
    expect(decoded).toHaveLength(frames.length);
    for (const [index, frame] of frames.entries()) {
      expect(bytesEqual(decoded[index]!, frame)).toBe(true);
    }
  });

  it("uses app-scoped websocket URLs without identity query parameters", () => {
    expect(webSocketUrl("http://127.0.0.1:4200", "app-a")).toBe(
      "ws://127.0.0.1:4200/apps/app-a/ws",
    );
  });

  it("encodes the websocket auth prelude as the server AuthHandshake shape", () => {
    expect(
      JSON.parse(
        encodeWebSocketPrelude('{"admin_secret":"s"}', authorBytes("urn:jazz:system", "system")),
      ),
    ).toEqual({
      peer_identity: '["urn:jazz:system","system"]',
      auth: { sub: "system", admin_secret: "s" },
      sub: "system",
      admin_secret: "s",
    });
  });

  it("uses the JWT subject for the websocket auth prelude when present", () => {
    const token = `header.${btoa(JSON.stringify({ iss: "https://issuer.example", sub: "user-123" }))}.sig`;

    expect(
      JSON.parse(
        encodeWebSocketPrelude(
          JSON.stringify({ jwt_token: token }),
          authorBytes("https://issuer.example", "user-123"),
        ),
      ),
    ).toEqual({
      peer_identity: '["https://issuer.example","user-123"]',
      auth: { sub: "user-123", jwt_token: token },
      sub: "user-123",
      jwt_token: token,
    });
  });

  it("derives websocket peer identity from the full canonical session author", () => {
    const jwt = (issuer: string) =>
      `header.${btoa(JSON.stringify({ iss: issuer, sub: "same-provider-subject" }))}.signature`;
    const fallback = new TextEncoder().encode('["https://jazz.test","cache"]');

    const issuerA = new TextDecoder().decode(
      peerIdentityForWebSocketAuth(
        JSON.stringify({ jwt_token: jwt("https://issuer-a.example") }),
        fallback,
      ),
    );
    const issuerB = new TextDecoder().decode(
      peerIdentityForWebSocketAuth(
        JSON.stringify({ jwt_token: jwt("https://issuer-b.example") }),
        fallback,
      ),
    );

    expect(issuerA).toBe('["https://issuer-a.example","same-provider-subject"]');
    expect(issuerB).toBe('["https://issuer-b.example","same-provider-subject"]');
    expect(issuerA).not.toBe(issuerB);
  });

  it("preserves verified external JWT issuer and subject bytes exactly", () => {
    const fallback = new TextEncoder().encode('["https://jazz.test","cache"]');
    const jwt = (iss: string) =>
      `header.${btoa(JSON.stringify({ iss, sub: " provider-subject " }))}.signature`;

    const normalized = new TextDecoder().decode(
      peerIdentityForWebSocketAuth(
        JSON.stringify({ jwt_token: jwt(" https://issuer.example ") }),
        fallback,
      ),
    );
    expect(normalized).toBe('[" https://issuer.example "," provider-subject "]');

    // The server rejects a verified external token whose issuer is ASCII-blank.
    // Keep the client sessionless instead of fabricating an
    // author that would only self-reject at WebSocket admission.
    expect(peerIdentityForWebSocketAuth(JSON.stringify({ jwt_token: jwt(" \t ") }), fallback)).toBe(
      fallback,
    );
  });

  it("derives impersonated websocket peer identity from the complete backend session", () => {
    const fallback = new TextEncoder().encode('["https://jazz.test","cache"]');
    const actual = new TextDecoder().decode(
      peerIdentityForWebSocketAuth(
        JSON.stringify({
          backend_secret: "not inspected by the client",
          backend_session: {
            issuer: "https://issuer.example",
            user_id: "provider-subject",
          },
        }),
        fallback,
      ),
    );

    expect(actual).toBe('["https://issuer.example","provider-subject"]');
  });

  it("matches the server's backend-session precedence over a simultaneous bearer token", () => {
    const fallback = new TextEncoder().encode('["https://jazz.test","cache"]');
    const jwt = `header.${btoa(
      JSON.stringify({ iss: "https://bearer.example", sub: "bearer-subject" }),
    )}.signature`;
    const actual = new TextDecoder().decode(
      peerIdentityForWebSocketAuth(
        JSON.stringify({
          jwt_token: jwt,
          backend_secret: "not inspected by the client",
          backend_session: {
            issuer: "https://backend.example",
            user_id: "backend-subject",
          },
        }),
        fallback,
      ),
    );

    expect(actual).toBe('["https://backend.example","backend-subject"]');
  });

  it("keeps admin websocket links sessionless despite accompanying bearer payloads", () => {
    const fallback = new TextEncoder().encode('["https://jazz.test","admin-cache"]');
    const validJwt = `header.${btoa(
      JSON.stringify({ iss: "https://issuer.example", sub: "provider-subject" }),
    )}.signature`;

    for (const jwt_token of [validJwt, "forged.token.payload", "malformed-token"]) {
      expect(
        peerIdentityForWebSocketAuth(
          JSON.stringify({ admin_secret: "not inspected by the client", jwt_token }),
          fallback,
        ),
      ).toBe(fallback);
    }

    expect(
      peerIdentityForWebSocketAuth(
        JSON.stringify({
          admin_secret: "not inspected by the client",
          backend_secret: "not inspected by the client",
          backend_session: {
            issuer: "https://backend.example",
            user_id: "backend-subject",
          },
        }),
        fallback,
      ),
    ).toBe(fallback);
  });

  it("uses validated fallback subjects over whitespace raw auth subjects in both handshake shapes", () => {
    const jwt = `header.${btoa(JSON.stringify({ sub: "jwt-user" }))}.sig`;
    const cases = [
      { auth: { sub: " \t ", jwt_token: jwt }, expected: "jwt-user" },
      {
        auth: {
          sub: " \t ",
          backend_session: {
            issuer: "https://issuer.example",
            user_id: "backend-user",
          },
        },
        expected: "backend-user",
      },
    ];

    for (const { auth, expected } of cases) {
      const prelude = JSON.parse(encodeWebSocketPrelude(JSON.stringify(auth), Uint8Array.of(1)));
      expect(prelude.sub).toBe(expected);
      expect(prelude.auth.sub).toBe(expected);
    }
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

    const suffixed = Uint8Array.from([...hello, 0]);
    expect(new PostcardReader(suffixed).u64()).toBe(0);
    expect(() => isWireHello(suffixed)).toThrow("WireFrame::Hello has trailing postcard bytes");
  });

  it("rejects a server codec that this native artifact did not advertise", async () => {
    const localFeatures = CLIENT_WIRE_FEATURES & ~FEATURE_PAYLOAD_ZSTD;
    const { carrier, socket } = carrierForTest({ features: localFeatures });
    socket.emitMessage(
      encodeWebSocketFrameBatch([
        encodeServerHello(1n, WIRE_PROTOCOL_VERSION, { features: CLIENT_WIRE_FEATURES }),
      ]),
    );
    await expect(carrier.ready()).rejects.toThrow("server accepted unsupported wire features 0x10");
    expect(socket.closed).toBe(true);
  });

  it("decodes exact Rust-produced Hello frames and rejects only true suffixes", async () => {
    const manifest = rustWireHelloFixtureManifest();

    for (const fixture of manifest.fixtures) {
      const frame = hexToBytes(fixture.frame_hex);
      expect(isWireHello(frame), fixture.name).toBe(true);
      expect(() => isWireHello(Uint8Array.from([...frame, 0])), fixture.name).toThrow(
        "WireFrame::Hello has trailing postcard bytes",
      );

      if (fixture.role !== 1) continue;
      let socket: MessageWebSocket | undefined;
      const carrier = new WebSocketCarrier({
        endpointUrl: "ws://127.0.0.1:4200/apps/app-a/ws",
        peerIdentity: new Uint8Array(16),
        onFrame: () => {},
        WebSocket: class extends MessageWebSocket {
          constructor(url: string) {
            super(url, (created) => {
              socket = created;
            });
          }
        },
      });
      socket!.emitMessage(encodeWebSocketFrameBatch([frame]));
      const negotiated = await carrier.ready();
      expect(negotiated.features, fixture.name).toBe(fixture.features);
      expect(negotiated.authority?.node, fixture.name).toEqual(
        fixture.authority_node_hex ? hexToBytes(fixture.authority_node_hex) : undefined,
      );
      expect(negotiated.authority?.epoch, fixture.name).toBe(
        fixture.authority_epoch === null ? undefined : BigInt(fixture.authority_epoch),
      );
      carrier.close();
    }
  });

  it("rejects malformed authority UUID lengths and unsupported u64 feature bits", async () => {
    const cases: Array<{ name: string; frame: Uint8Array; error: string }> = [
      {
        name: "15-byte authority UUID",
        frame: encodeServerHello(1n, WIRE_PROTOCOL_VERSION, {
          authorityNode: new Uint8Array(15),
        }),
        error: "WireHello.authority.node must be exactly 16 bytes, got 15",
      },
      {
        name: "17-byte authority UUID",
        frame: encodeServerHello(1n, WIRE_PROTOCOL_VERSION, {
          authorityNode: new Uint8Array(17),
        }),
        error: "WireHello.authority.node must be exactly 16 bytes, got 17",
      },
      {
        name: "unsupported low feature bit",
        frame: encodeServerHello(1n, WIRE_PROTOCOL_VERSION, { features: 1n << 1n }),
        error: "server accepted unsupported wire features 0x2",
      },
      {
        name: "unsupported bit 32",
        frame: encodeServerHello(1n, WIRE_PROTOCOL_VERSION, { features: 1n << 32n }),
        error: "server accepted unsupported wire features 0x100000000",
      },
      {
        name: "unsupported high feature bit",
        frame: encodeServerHello(1n, WIRE_PROTOCOL_VERSION, { features: 1n << 63n }),
        error: "server accepted unsupported wire features 0x8000000000000000",
      },
    ];

    for (const testCase of cases) {
      const { carrier, socket } = carrierForTest();
      socket.emitMessage(encodeWebSocketFrameBatch([testCase.frame]));
      await expect(carrier.ready(), testCase.name).rejects.toThrow(testCase.error);
      expect(socket.closed, testCase.name).toBe(true);
    }
  });

  it("accepts supported feature masks without losing u64 validation", async () => {
    for (const features of [0n, BigInt(CLIENT_WIRE_FEATURES)]) {
      const { carrier, socket } = carrierForTest();
      socket.emitMessage(
        encodeWebSocketFrameBatch([encodeServerHello(1n, WIRE_PROTOCOL_VERSION, { features })]),
      );
      await expect(carrier.ready()).resolves.toMatchObject({ features: Number(features) });
      carrier.close();
    }
  });

  it("rejects non-exact server wire-version advertisements before payload decode", async () => {
    for (const [minProtocolVersion, maxProtocolVersion] of [
      [0, 1],
      [1, 2],
      [1, 15],
      [12, 12],
    ]) {
      const { carrier, socket } = carrierForTest();
      socket.emitMessage(
        encodeWebSocketFrameBatch([
          encodeServerHello(1n, WIRE_PROTOCOL_VERSION, {
            minProtocolVersion,
            maxProtocolVersion,
          }),
        ]),
      );

      await expect(carrier.ready()).rejects.toThrow(
        `server must advertise exactly wire protocol ${WIRE_PROTOCOL_VERSION}, got ${minProtocolVersion}..=${maxProtocolVersion}`,
      );
      expect(socket.closed).toBe(true);
    }
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
    expect(() => new PostcardReader(encodedEpoch).u64()).toThrow(
      "postcard u64 exceeds Number.MAX_SAFE_INTEGER",
    );
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

  it("surfaces terminal structured errors before server hello", async () => {
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

    await expect(carrier.ready()).rejects.toThrow(
      "websocket internal before server hello: conflicting commit unit",
    );
    expect(errors).toEqual([
      { code: "internal", retry: "later", message: "conflicting commit unit" },
    ]);
    expect(socket!.closed).toBe(true);
  });

  it("preserves a typed retryable not-ready error before server hello", async () => {
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
      encodeWebSocketFrameBatch([encodeWireError(6, 3, "catalogue bootstrapping")]),
    );

    await expect(carrier.ready()).rejects.toMatchObject({
      name: "PreHelloWireError",
      wireError: { code: "not_ready", retry: "later", message: "catalogue bootstrapping" },
    });
    expect(errors).toEqual([
      { code: "not_ready", retry: "later", message: "catalogue bootstrapping" },
    ]);
    expect(socket!.closed).toBe(true);
  });

  it("decodes structured wire error frames", () => {
    const encoded = encodeWireError(3, 1, "bad credentials");
    expect(isWireError(encoded)).toBe(true);
    expect(decodeWireError(encoded)).toEqual({
      code: "auth_failed",
      retry: "after_auth",
      message: "bad credentials",
    });

    const suffixed = Uint8Array.from([...encoded, 0]);
    expect(new PostcardReader(suffixed).u64()).toBe(2);
    expect(() => isWireError(suffixed)).toThrow("WireFrame::Error has trailing postcard bytes");
    expect(() => decodeWireError(suffixed)).toThrow("WireFrame::Error has trailing postcard bytes");

    expect(encoded[0]).toBe(2);
    const nonminimalTag = Uint8Array.from([0x82, 0x00, ...encoded.slice(1)]);
    expect(() => isWireError(nonminimalTag)).toThrow("postcard u64 is not minimally encoded");
    expect(() => decodeWireError(nonminimalTag)).toThrow("postcard u64 is not minimally encoded");

    const unknownCode = encodeWireError(7, 3, "future code");
    expect(() => isWireError(unknownCode)).toThrow("unknown WireErrorCode discriminant 7");
    expect(() => decodeWireError(unknownCode)).toThrow("unknown WireErrorCode discriminant 7");

    const unknownRetry = encodeWireError(6, 4, "future retry");
    expect(() => isWireError(unknownRetry)).toThrow("unknown WireRetry discriminant 4");
    expect(() => decodeWireError(unknownRetry)).toThrow("unknown WireRetry discriminant 4");
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
    for (const candidate of manifest.fixtures) {
      const candidateFrame = hexToBytes(candidate.frame_hex);
      expect(isWireMessage(candidateFrame), candidate.name).toBe(true);
      expect(
        bytesEqual(
          decodeWebSocketFrameBatch(encodeWebSocketFrameBatch([candidateFrame]))[0]!,
          candidateFrame,
        ),
        candidate.name,
      ).toBe(true);
      const suffixed = Uint8Array.from([...candidateFrame, 0]);
      expect(() => isWireMessage(suffixed), candidate.name).toThrow(
        "WireFrame::Message has trailing postcard bytes",
      );
    }
    expect(fixture?.name).toBe("view_update_mixed_version_carrier_runs");
    expect(fixture?.message_family).toBe("ViewUpdate");

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
    // Protocol v11 removed the legacy branch-metadata variants; the two
    // auxiliary chunk-I/O variants now precede ViewUpdate in the postcard enum.
    expect(payload[0]).toBe(14);
  });
});

type RustWireFixtureManifest = {
  protocol_version: number;
  fixtures: Array<{
    name: string;
    message_family: string;
    frame_hex: string;
  }>;
};

type RustWireHelloFixtureManifest = {
  fixtures: Array<{
    name: string;
    features: number;
    role: number;
    authority_node_hex: string | null;
    authority_epoch: number | string | null;
    frame_hex: string;
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

function rustWireHelloFixtureManifest(): RustWireHelloFixtureManifest {
  return JSON.parse(
    readFileSync(
      new URL("../../../../../crates/jazz/fixtures/wire_hello_frames.json", import.meta.url),
      "utf8",
    ),
  ) as RustWireHelloFixtureManifest;
}

function hexToBytes(hex: string): Uint8Array {
  const bytes = new Uint8Array(hex.length / 2);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}

function bytesEqual(left: Uint8Array, right: Uint8Array): boolean {
  if (left.byteLength !== right.byteLength) return false;
  return (
    Buffer.compare(
      Buffer.from(left.buffer, left.byteOffset, left.byteLength),
      Buffer.from(right.buffer, right.byteOffset, right.byteLength),
    ) === 0
  );
}

function encodeWireError(code: number, retry: number, message: string): Uint8Array {
  const writer = new PostcardWriter();
  writer.u64(2);
  writer.u64(code);
  writer.u64(retry);
  writer.string(message);
  return writer.finish();
}

function encodeServerHello(
  epoch: bigint,
  protocolVersion = WIRE_PROTOCOL_VERSION,
  options: {
    features?: number | bigint;
    authorityNode?: Uint8Array | null;
    minProtocolVersion?: number;
    maxProtocolVersion?: number;
  } = {},
): Uint8Array {
  const writer = new PostcardWriter();
  writer.u64(0); // WireFrame::Hello
  writer.u64(options.minProtocolVersion ?? protocolVersion);
  writer.u64(options.maxProtocolVersion ?? protocolVersion);
  writer.u64(options.features ?? CLIENT_WIRE_FEATURES);
  writer.u64(1); // WirePeerRole::Core
  const authorityNode =
    options.authorityNode === undefined
      ? Uint8Array.from({ length: 16 }, () => 0x5e)
      : options.authorityNode;
  if (authorityNode === null) {
    writer.none();
  } else {
    writer.some((authority) => {
      authority.bytes(authorityNode);
      authority.u64(epoch);
    });
  }
  return writer.finish();
}

function carrierForTest(options: { features?: number } = {}): {
  carrier: WebSocketCarrier;
  socket: MessageWebSocket;
} {
  let socket: MessageWebSocket | undefined;
  const carrier = new WebSocketCarrier({
    endpointUrl: "ws://127.0.0.1:4200/apps/app-a/ws",
    peerIdentity: new Uint8Array(16),
    features: options.features,
    onFrame: () => {},
    WebSocket: class extends MessageWebSocket {
      constructor(url: string) {
        super(url, (created) => {
          socket = created;
        });
      }
    },
  });
  return { carrier, socket: socket! };
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
