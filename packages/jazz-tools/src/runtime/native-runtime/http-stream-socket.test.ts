import { afterEach, describe, expect, it, vi } from "vitest";
import {
  STREAM_RECORD_SESSION,
  encodeStreamRecord,
  sameOriginWebSocketConstructor,
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
});

describe("same-origin socket fallback", () => {
  it("switches to the HTTP stream only when a createJazz backend says it serves one", async () => {
    const url = "ws://app.test/jazz/apps/a1/ws";
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const target = String(input);
      if (init?.method === "GET" && target === "http://app.test/jazz/apps/a1/ws") {
        return new Response("use the stream", { status: 426, headers: { "x-jazz-stream": "1" } });
      }
      if (init?.method === "POST" && target === streamUrlForSocketUrl(url)) {
        return openStreamResponse("s1");
      }
      return new Response(null, { status: 404 });
    });
    vi.stubGlobal("fetch", fetchMock);

    const Socket = sameOriginWebSocketConstructor(failingNative());
    const socket = new Socket(url);
    expect(await outcome(socket)).toBe("open");
    socket.close();
    // The endpoint is remembered: the next session goes straight to the stream.
    const next = new Socket(url);
    expect(await outcome(next)).toBe("open");
    next.close();
    expect(fetchMock.mock.calls.filter(([, init]) => init?.method === "GET")).toHaveLength(1);
  });

  it("fails as a socket when the endpoint is not a createJazz backend", async () => {
    // A same-origin proxy to a sync server: no stream marker on the socket path.
    const fetchMock = vi.fn(async () => new Response("not found", { status: 404 }));
    vi.stubGlobal("fetch", fetchMock);

    const Socket = sameOriginWebSocketConstructor(failingNative());
    const url = "ws://proxy.test/apps/a1/ws";
    expect(await outcome(new Socket(url))).toBe("closed");
    // Not remembered: the next session tries a native socket again.
    expect(await outcome(new Socket(url))).toBe("closed");
    expect(fetchMock.mock.calls.every(([, init]) => (init as RequestInit)?.method === "GET")).toBe(
      true,
    );
  });

  it("fails as a socket when the backend can't be reached at all", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(async () => {
        await new Promise((resolve) => setTimeout(resolve, 20));
        throw new TypeError("network down");
      }),
    );
    const Socket = sameOriginWebSocketConstructor(failingNative());
    const socket = new Socket("ws://down.test/jazz/apps/a1/ws");
    const result = outcome(socket);
    // The native socket has failed, but while the probe runs it's still connecting.
    await new Promise((resolve) => setTimeout(resolve, 5));
    expect(socket.readyState).toBe(0);
    expect(await result).toBe("closed");
  });
});
