import { afterEach, describe, expect, it, vi } from "vitest";
import {
  SOCKET_OPEN_DEADLINE_MS,
  STREAM_RECORD_SESSION,
  UPGRADE_FAILURES_BEFORE_STREAM,
  encodeStreamRecord,
  httpStreamFallbackWebSocket,
  streamUrlForSocketUrl,
} from "./http-stream-socket.js";
import type { WebSocketConstructor } from "./websocket.js";

type Listener = (event?: unknown) => void;

/** A native socket that fails before it opens, as one a host can't upgrade. */
function failingNative(): WebSocketConstructor {
  return class FailingSocket {
    binaryType = "blob";
    readyState = 0;
    private listeners = new Map<string, Listener[]>();
    constructor(_url: string) {
      queueMicrotask(() => {
        this.readyState = 3;
        for (const listener of this.listeners.get("error") ?? []) listener({});
        for (const listener of this.listeners.get("close") ?? [])
          listener({ code: 1006, reason: "" });
      });
    }
    addEventListener(type: string, listener: Listener) {
      this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
    }
    send() {}
    close() {}
  } as unknown as WebSocketConstructor;
}

/** A native socket that opens, as one a host upgrades. */
function openingNative(): WebSocketConstructor {
  return class OpeningSocket {
    binaryType = "blob";
    readyState = 0;
    private listeners = new Map<string, Listener[]>();
    constructor(_url: string) {
      queueMicrotask(() => {
        this.readyState = 1;
        for (const listener of this.listeners.get("open") ?? []) listener({});
      });
    }
    addEventListener(type: string, listener: Listener) {
      this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
    }
    send() {}
    close() {}
  } as unknown as WebSocketConstructor;
}

/** A native socket whose upgrade never gets an answer, behind a proxy that holds it. */
function hangingNative(closed: string[]): WebSocketConstructor {
  return class HangingSocket {
    binaryType = "blob";
    readyState = 0;
    private listeners = new Map<string, Listener[]>();
    constructor(private readonly url: string) {}
    addEventListener(type: string, listener: Listener) {
      this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
    }
    send() {}
    close() {
      closed.push(this.url);
      this.readyState = 3;
      for (const listener of this.listeners.get("close") ?? [])
        listener({ code: 1006, reason: "" });
    }
  } as unknown as WebSocketConstructor;
}

function outcome(socket: { addEventListener(type: string, listener: Listener): void }) {
  return new Promise<string>((resolve) => {
    socket.addEventListener("open", () => resolve("open"));
    socket.addEventListener("close", () => resolve("closed"));
  });
}

/** A stream response that opens session `id` and stays open. */
function openStreamResponse(id: string): Response {
  return new Response(
    new ReadableStream({
      start(controller) {
        controller.enqueue(
          encodeStreamRecord(
            STREAM_RECORD_SESSION,
            new TextEncoder().encode(JSON.stringify({ id, key: "k" })),
          ),
        );
      },
    }),
  );
}

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  vi.useRealTimers();
});

/** A createJazz backend that upgrades sockets (426, no stream marker) and serves the stream. */
function backendThatUpgrades(host: string, url: string, session: string) {
  return vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
    const target = String(input);
    if (init?.method === "POST" && target === `http://${host}/jazz/apps/a1/ws`) {
      return new Response("Upgrade to a WebSocket", { status: 426 });
    }
    if (init?.method === "POST" && target === streamUrlForSocketUrl(url)) {
      return openStreamResponse(session);
    }
    return new Response(null, { status: 404 });
  });
}

type HeldSocket = { closed: boolean; fire(type: string, event?: unknown): void };

/** Native sockets the test opens (or not) by hand. */
function heldNative(sockets: HeldSocket[]): WebSocketConstructor {
  return class Held {
    binaryType = "blob";
    readyState = 0;
    closed = false;
    private listeners = new Map<string, Listener[]>();
    constructor(_url: string) {
      sockets.push(this);
    }
    addEventListener(type: string, listener: Listener) {
      this.listeners.set(type, [...(this.listeners.get(type) ?? []), listener]);
    }
    fire(type: string, event: unknown = {}) {
      for (const listener of this.listeners.get(type) ?? []) listener(event);
    }
    send() {}
    close() {
      this.closed = true;
    }
  } as unknown as WebSocketConstructor;
}

describe("same-origin socket fallback", () => {
  it("switches to the HTTP stream only when a createJazz backend says it serves one", async () => {
    const url = "ws://app.test/jazz/apps/a1/ws";
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const target = String(input);
      if (init?.method === "POST" && target === "http://app.test/jazz/apps/a1/ws") {
        return new Response("use the stream", { status: 426, headers: { "x-jazz-stream": "1" } });
      }
      if (init?.method === "POST" && target === streamUrlForSocketUrl(url)) {
        return openStreamResponse("s1");
      }
      return new Response(null, { status: 404 });
    });
    vi.stubGlobal("fetch", fetchMock);

    const Socket = httpStreamFallbackWebSocket(failingNative());
    const socket = new Socket(url);
    expect(await outcome(socket)).toBe("open");
    socket.close();
    // The endpoint is remembered: the next session goes straight to the stream.
    const next = new Socket(url);
    expect(await outcome(next)).toBe("open");
    next.close();
    expect(
      fetchMock.mock.calls.filter(([input]) => String(input) === "http://app.test/jazz/apps/a1/ws"),
    ).toHaveLength(1);
  });

  it("switches to the HTTP stream when upgrades never get through to a backend that upgrades", async () => {
    // A proxy that passes plain requests and streams on, but not upgrades.
    const url = "ws://behind-proxy.test/jazz/apps/a1/ws";
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const target = String(input);
      if (init?.method === "POST" && target === "http://behind-proxy.test/jazz/apps/a1/ws") {
        return new Response("Upgrade to a WebSocket", { status: 426 });
      }
      if (init?.method === "POST" && target === streamUrlForSocketUrl(url)) {
        return openStreamResponse("s2");
      }
      return new Response(null, { status: 404 });
    });
    vi.stubGlobal("fetch", fetchMock);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);

    const Socket = httpStreamFallbackWebSocket(failingNative());
    // A restart or a draining instance fails a socket or two: retried as sockets.
    for (let attempt = 1; attempt < UPGRADE_FAILURES_BEFORE_STREAM; attempt++) {
      expect(await outcome(new Socket(url))).toBe("closed");
    }
    expect(warn).not.toHaveBeenCalled();
    // One more in a row, and the stream takes over, saying why.
    const socket = new Socket(url);
    expect(await outcome(socket)).toBe("open");
    socket.close();
    expect(warn.mock.calls.flat().join(" ")).toMatch(/doesn't pass WebSocket upgrades on/);
    warn.mockRestore();
  });

  it("switches to the HTTP stream when upgrades get no answer at all, twice in a row", async () => {
    // A proxy (vite preview without ws) that passes plain requests on and
    // holds upgrades: the socket neither opens nor fails.
    vi.useFakeTimers();
    const url = "ws://holding-proxy.test/jazz/apps/a1/ws";
    const fetchMock = backendThatUpgrades("holding-proxy.test", url, "s3");
    vi.stubGlobal("fetch", fetchMock);
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const closed: string[] = [];
    const Socket = httpStreamFallbackWebSocket(hangingNative(closed));
    // The first hang fails as a socket: a scale-to-zero edge holds the
    // first upgrade after idle while the instance starts.
    const first = outcome(new Socket(url));
    await vi.advanceTimersByTimeAsync(SOCKET_OPEN_DEADLINE_MS - 1);
    expect(fetchMock).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    expect(await first).toBe("closed");
    expect(closed).toEqual([url]);
    expect(warn).not.toHaveBeenCalled();
    // A second in a row, and the stream takes over, saying why.
    const socket = new Socket(url);
    const second = outcome(socket);
    await vi.advanceTimersByTimeAsync(SOCKET_OPEN_DEADLINE_MS);
    expect(await second).toBe("open");
    expect(warn.mock.calls.flat().join(" ")).toMatch(/got no answer/);
    socket.close();
  });

  it("keeps a socket that opens just before its deadline, and nothing it said after", async () => {
    vi.useFakeTimers();
    const url = "ws://slow-open.test/jazz/apps/a1/ws";
    const fetchMock = backendThatUpgrades("slow-open.test", url, "s4");
    vi.stubGlobal("fetch", fetchMock);
    const natives: HeldSocket[] = [];
    const Socket = httpStreamFallbackWebSocket(heldNative(natives));
    const socket = new Socket(url);
    const result = outcome(socket);
    await vi.advanceTimersByTimeAsync(SOCKET_OPEN_DEADLINE_MS - 1);
    natives[0]!.fire("open");
    expect(await result).toBe("open");
    await vi.advanceTimersByTimeAsync(SOCKET_OPEN_DEADLINE_MS);
    expect(fetchMock).not.toHaveBeenCalled();
    expect(natives[0]!.closed).toBe(false);
    socket.close();

    // A socket past its deadline is given up on: an open it reports later
    // is not taken.
    const late = new Socket(url);
    const events: string[] = [];
    late.addEventListener("open", () => events.push("open"));
    late.addEventListener("message", () => events.push("message"));
    late.addEventListener("close", () => events.push("close"));
    await vi.advanceTimersByTimeAsync(SOCKET_OPEN_DEADLINE_MS);
    natives[1]!.fire("open");
    natives[1]!.fire("message", { data: "late" });
    await vi.advanceTimersByTimeAsync(0);
    expect(events).toEqual(["close"]);
  });

  it("stops the deadline when the socket is closed while connecting", async () => {
    vi.useFakeTimers();
    const url = "ws://closed-early.test/jazz/apps/a1/ws";
    const fetchMock = backendThatUpgrades("closed-early.test", url, "s5");
    vi.stubGlobal("fetch", fetchMock);
    const natives: HeldSocket[] = [];
    const socket = new (httpStreamFallbackWebSocket(heldNative(natives)))(url);
    socket.close();
    await vi.advanceTimersByTimeAsync(2 * SOCKET_OPEN_DEADLINE_MS);
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("leaves a hung upgrade to fail as a socket when the endpoint is not a createJazz backend", async () => {
    vi.useFakeTimers();
    const url = "ws://elsewhere.test/jazz/apps/a1/ws";
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response(null, { status: 404 })),
    );
    const Socket = httpStreamFallbackWebSocket(hangingNative([]));
    for (let attempt = 0; attempt < 3; attempt++) {
      const result = outcome(new Socket(url));
      await vi.advanceTimersByTimeAsync(SOCKET_OPEN_DEADLINE_MS);
      expect(await result).toBe("closed");
    }
  });

  it("starts the count of failed upgrades again once a socket opens", async () => {
    const url = "ws://flaky.test/jazz/apps/a1/ws";
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => new Response("Upgrade to a WebSocket", { status: 426 })),
    );
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const Failing = httpStreamFallbackWebSocket(failingNative());
    const Opening = httpStreamFallbackWebSocket(openingNative());
    for (let round = 0; round < 2; round++) {
      for (let attempt = 1; attempt < UPGRADE_FAILURES_BEFORE_STREAM; attempt++) {
        expect(await outcome(new Failing(url))).toBe("closed");
      }
      const opened = new Opening(url);
      expect(await outcome(opened)).toBe("open");
      opened.close();
    }
    // Never three failures in a row: it stayed on sockets.
    expect(warn).not.toHaveBeenCalled();
    warn.mockRestore();
  });

  it("starts the count again when the backend is down or refuses the origin in between", async () => {
    const url = "ws://between.test/jazz/apps/a1/ws";
    const answers: Array<() => Response> = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => answers.shift()!()),
    );
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const Socket = httpStreamFallbackWebSocket(failingNative());
    const fail = async (answer: () => Response) => {
      answers.push(answer);
      expect(await outcome(new Socket(url))).toBe("closed");
    };
    const upgrades = () => new Response("Upgrade to a WebSocket", { status: 426 });
    // Two failures, then a restart (503), then two more: never three in a row.
    await fail(upgrades);
    await fail(upgrades);
    await fail(() => new Response("Jazz is unavailable", { status: 503 }));
    await fail(upgrades);
    await fail(upgrades);
    // An origin the backend refuses (a proxy that rewrites Host) is said as
    // such, once, and isn't counted as upgrades not getting through.
    await fail(() => new Response("origin not allowed", { status: 403 }));
    await fail(() => new Response("origin not allowed", { status: 403 }));
    await fail(upgrades);
    await fail(upgrades);
    expect(warn.mock.calls.flat()).toEqual([
      expect.stringMatching(/refuses this page's origin \(403\)/),
    ]);
    warn.mockRestore();
  });

  it("fails as a socket when the endpoint is not a createJazz backend", async () => {
    // A same-origin proxy to a sync server: no stream marker on the socket path.
    const fetchMock = vi.fn(
      async (_input: string | URL | Request, _init?: RequestInit) =>
        new Response("not found", { status: 404 }),
    );
    vi.stubGlobal("fetch", fetchMock);

    const Socket = httpStreamFallbackWebSocket(failingNative());
    const url = "ws://proxy.test/apps/a1/ws";
    expect(await outcome(new Socket(url))).toBe("closed");
    // Not remembered: the next session tries a native socket again.
    expect(await outcome(new Socket(url))).toBe("closed");
    // Only probes of the socket path: no stream was tried.
    expect(
      fetchMock.mock.calls.every(([input]) => String(input) === "http://proxy.test/apps/a1/ws"),
    ).toBe(true);
  });

  it("fails as a socket when the backend can't be reached at all", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => {
        await new Promise((resolve) => setTimeout(resolve, 20));
        throw new TypeError("network down");
      }),
    );
    const Socket = httpStreamFallbackWebSocket(failingNative());
    const socket = new Socket("ws://down.test/jazz/apps/a1/ws");
    const result = outcome(socket);
    // The native socket has failed, but while the probe runs it's still connecting.
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect(socket.readyState).toBe(0);
    expect(await result).toBe("closed");
  });
});
