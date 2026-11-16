import { afterEach, describe, expect, it, vi } from "vitest";
import { PostcardWriter } from "./native-codec.js";
import {
  SESSION_RENEWAL_HEADER,
  SESSION_RENEWAL_PRELUDE_FIELD,
  decodeRenewFrame,
  decodeRenewSoonFrame,
  encodeRenewFrame,
  encodeRenewSoonFrame,
  renewalUrlFor,
  sessionRefreshLockName,
} from "./session-renewal-wire.js";
import {
  CLIENT_WIRE_FEATURES,
  WIRE_PROTOCOL_VERSION,
  WebSocketCarrier,
  decodeWebSocketFrameBatch,
  encodeWebSocketFrameBatch,
  isWireHello,
} from "./websocket.js";

afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
});

describe("session renewal frames", () => {
  it("round-trip the session id, the delay and the ticket", () => {
    expect(decodeRenewSoonFrame(encodeRenewSoonFrame("ses-1", 1234.7))).toEqual({
      session: "ses-1",
      renewInMs: 1234,
    });
    expect(decodeRenewFrame(encodeRenewFrame("payload.mac"))).toBe("payload.mac");
  });

  it("are not mistaken for other frames", () => {
    expect(decodeRenewFrame(Uint8Array.of(2, 0, 0))).toBeNull();
    expect(decodeRenewFrame(encodeRenewSoonFrame("ses-1", 1))).toBeNull();
    expect(() => decodeRenewSoonFrame(encodeRenewFrame("x"))).toThrow();
  });

  it("share one refresh lock between renewals and sign-in checks, per origin", () => {
    expect(sessionRefreshLockName("wss://app.example.com/jazz/apps/a/ws")).toBe(
      sessionRefreshLockName("https://app.example.com/jazz/apps/a/accounts"),
    );
    expect(sessionRefreshLockName("wss://app.example.com/jazz/apps/a/ws")).not.toBe(
      sessionRefreshLockName("https://other.example.com/jazz/apps/a/accounts"),
    );
  });

  it("renew at the backend's who-is-signed-in route next to the socket", () => {
    expect(renewalUrlFor("wss://app.example.com/jazz/apps/app-a/ws")).toBe(
      "https://app.example.com/jazz/apps/app-a/accounts/backend-user",
    );
    expect(renewalUrlFor("ws://localhost:3000/api/jazz/apps/a%2Fb/ws")).toBe(
      "http://localhost:3000/api/jazz/apps/a%2Fb/accounts/backend-user",
    );
    expect(renewalUrlFor("wss://app.example.com/elsewhere")).toBeNull();
  });
});

const cookieAuth = JSON.stringify({
  backend_session: { user_id: "alice", issuer: "urn:jazz:backend", account_id: "acc" },
  jwt_token: null,
});

describe("a cookie session's carrier", () => {
  it("asks for renewal in its prelude; a bearer session's doesn't", async () => {
    const cookie = openCarrier(cookieAuth);
    await cookie.carrier.ready();
    expect(JSON.parse(cookie.socket.prelude())[SESSION_RENEWAL_PRELUDE_FIELD]).toBe(1);

    const bearer = openCarrier(JSON.stringify({ jwt_token: "a.b.c" }));
    await bearer.carrier.ready();
    expect(JSON.parse(bearer.socket.prelude())[SESSION_RENEWAL_PRELUDE_FIELD]).toBeUndefined();
  });

  it("renews when the backend says, sending the ticket back over the open socket", async () => {
    vi.useFakeTimers();
    const fetch = vi.fn(async (_url: string, init: RequestInit) => {
      expect(new Headers(init.headers).get(SESSION_RENEWAL_HEADER)).toBe("ses-1");
      expect(init.method).toBe("POST");
      expect(init.credentials).toBe("same-origin");
      return Response.json({ identity: {}, renewal: "ticket-1" });
    });
    vi.stubGlobal("fetch", fetch);
    const { carrier, socket, frames } = openCarrier(cookieAuth);
    await carrier.ready();
    socket.sent.length = 0;

    socket.emitMessage(encodeWebSocketFrameBatch([encodeRenewSoonFrame("ses-1", 5_000)]));
    await vi.advanceTimersByTimeAsync(4_999);
    expect(fetch).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    await vi.waitFor(() => expect(socket.sent).toHaveLength(1));

    expect(fetch.mock.calls[0]![0]).toBe("http://127.0.0.1:4200/apps/app-a/accounts/backend-user");
    const [sent] = decodeWebSocketFrameBatch(socket.sent[0] as Uint8Array);
    expect(decodeRenewFrame(sent!)).toBe("ticket-1");
    // The core never sees either frame.
    expect(frames).toHaveLength(0);
    expect(socket.closed).toBe(false);
  });

  it("sends nothing when the backend doesn't renew, leaving the session to its expiry", async () => {
    vi.useFakeTimers();
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("signed_out", { status: 401 })),
    );
    const { carrier, socket } = openCarrier(cookieAuth);
    await carrier.ready();
    socket.sent.length = 0;
    socket.emitMessage(encodeWebSocketFrameBatch([encodeRenewSoonFrame("ses-1", 10)]));
    await vi.advanceTimersByTimeAsync(50);
    expect(socket.sent).toHaveLength(0);
    expect(socket.closed).toBe(false);
  });

  it("never renews against a backend that doesn't offer it (an older one)", async () => {
    vi.useFakeTimers();
    const fetch = vi.fn();
    vi.stubGlobal("fetch", fetch);
    const { carrier, socket } = openCarrier(cookieAuth);
    await carrier.ready();
    socket.sent.length = 0;
    await vi.advanceTimersByTimeAsync(10 * 60_000);
    expect(fetch).not.toHaveBeenCalled();
    expect(socket.sent).toHaveLength(0);
  });

  it("never renews once closed", async () => {
    vi.useFakeTimers();
    const fetch = vi.fn();
    vi.stubGlobal("fetch", fetch);
    const { carrier, socket } = openCarrier(cookieAuth);
    await carrier.ready();
    socket.emitMessage(encodeWebSocketFrameBatch([encodeRenewSoonFrame("ses-1", 10)]));
    carrier.close();
    await vi.advanceTimersByTimeAsync(50);
    expect(fetch).not.toHaveBeenCalled();
  });
});

function openCarrier(authJson: string) {
  let socket!: FakeSocket;
  const frames: Uint8Array[] = [];
  const carrier = new WebSocketCarrier({
    endpointUrl: "ws://127.0.0.1:4200/apps/app-a/ws",
    peerIdentity: new TextEncoder().encode(JSON.stringify(["urn:jazz:backend", "alice"])),
    authJson,
    onFrame: (frame) => frames.push(frame),
    WebSocket: class extends FakeSocket {
      constructor(url: string) {
        super(url, (created) => {
          socket = created;
        });
      }
    },
  });
  return { carrier, socket, frames };
}

class FakeSocket {
  binaryType: "arraybuffer" | "blob" = "arraybuffer";
  readonly readyState = 1;
  sent: Array<Uint8Array | string> = [];
  closed = false;
  private readonly listeners = new Map<string, Array<(event: never) => void>>();

  constructor(
    readonly url: string,
    onCreate?: (socket: FakeSocket) => void,
  ) {
    onCreate?.(this);
  }

  prelude(): string {
    return this.sentPrelude!;
  }

  private sentPrelude: string | undefined;

  send(data: Uint8Array | string): void {
    if (typeof data === "string") {
      this.sentPrelude = data;
      return;
    }
    if (isWireHello(decodeWebSocketFrameBatch(data)[0]!)) {
      this.emitMessage(encodeWebSocketFrameBatch([serverHello()]));
      return;
    }
    this.sent.push(data);
  }

  close(): void {
    this.closed = true;
  }

  addEventListener(type: string, listener: (event: never) => void): void {
    this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
  }

  emitMessage(data: Uint8Array): void {
    for (const listener of this.listeners.get("message") ?? [])
      (listener as (event: { data: unknown }) => void)({ data });
  }
}

function serverHello(): Uint8Array {
  const writer = new PostcardWriter();
  writer.u64(0); // WireFrame::Hello
  writer.u64(WIRE_PROTOCOL_VERSION);
  writer.u64(WIRE_PROTOCOL_VERSION);
  writer.u64(CLIENT_WIRE_FEATURES);
  writer.u64(1); // WirePeerRole::Core
  writer.some((authority) => {
    authority.bytes(Uint8Array.from({ length: 16 }, () => 0x5e));
    authority.u64(1n);
  });
  return writer.finish();
}
