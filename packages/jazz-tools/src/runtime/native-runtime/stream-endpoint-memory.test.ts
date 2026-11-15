import { IDBFactory } from "fake-indexeddb";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { WebSocketConstructor } from "./websocket.js";

// #5187: on a host that can't upgrade sockets, every new page tried a
// socket first, and the browser logged its failure (426) each time. Once the
// backend has said it serves the HTTP stream instead, that is remembered per
// endpoint across page loads (in IndexedDB, which workers have too).

type Listener = (event?: unknown) => void;

/** Native sockets opened, by URL: none at all is what a remembered endpoint means. */
let opened: string[] = [];

/** A native socket that fails before it opens, as one a host can't upgrade. */
class FailingSocket {
  binaryType = "blob";
  readyState = 0;
  private listeners = new Map<string, Listener[]>();
  constructor(url: string) {
    opened.push(url);
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
}
const failingNative = FailingSocket as unknown as WebSocketConstructor;

function openStreamResponse(
  id: string,
  encodeStreamRecord: typeof import("./http-stream-socket.js").encodeStreamRecord,
  signal?: AbortSignal | null,
) {
  return new Response(
    new ReadableStream({
      start(controller) {
        controller.enqueue(
          encodeStreamRecord(0, new TextEncoder().encode(JSON.stringify({ id, key: "k" }))),
        );
        signal?.addEventListener("abort", () => controller.error(new Error("aborted")));
      },
    }),
  );
}

/** A createJazz backend that can't upgrade sockets: the probe gets the stream marker. */
function backendServingTheStream(
  url: string,
  module: typeof import("./http-stream-socket.js"),
  options: { streamStatus?: number; marker?: boolean } = {},
) {
  const calls: Array<{ method: string; url: string; streamAborted?: boolean }> = [];
  let streamSignal: AbortSignal | null | undefined;
  const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
    const target = String(input);
    const method = init?.method ?? "GET";
    calls.push({ method, url: target, streamAborted: streamSignal?.aborted });
    if (method === "POST" && target === url.replace(/^ws/, "http")) {
      return new Response("use the stream", {
        status: 426,
        headers: options.marker === false ? {} : { "x-jazz-stream": "1" },
      });
    }
    if (method === "POST" && target === module.streamUrlForSocketUrl(url)) {
      if (options.streamStatus) return new Response(null, { status: options.streamStatus });
      streamSignal = init?.signal;
      return openStreamResponse("s1", module.encodeStreamRecord, init?.signal);
    }
    if (method === "DELETE") return new Response(null, { status: 204 });
    return new Response(null, { status: 404 });
  });
  vi.stubGlobal("fetch", fetchMock);
  return calls;
}

function outcome(socket: { addEventListener(type: string, listener: Listener): void }) {
  return new Promise<string>((resolve) => {
    socket.addEventListener("open", () => resolve("open"));
    socket.addEventListener("close", () => resolve("closed"));
  });
}

/** A fresh page (or worker): the module's in-memory state starts over, IndexedDB stays. */
async function newPage() {
  vi.resetModules();
  return await import("./http-stream-socket.js");
}

beforeEach(() => {
  opened = [];
  vi.stubGlobal("indexedDB", new IDBFactory());
});
afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

describe("endpoints remembered across page loads", () => {
  it("goes straight to the stream on a new page once the backend said it serves one", async () => {
    const url = "ws://app.test/jazz/apps/a1/ws";
    let page = await newPage();
    backendServingTheStream(url, page);
    const first = new (page.httpStreamFallbackWebSocket(failingNative))(url);
    expect(await outcome(first)).toBe("open");
    first.close();
    expect(opened).toEqual([url]);

    page = await newPage();
    const calls = backendServingTheStream(url, page);
    const next = new (page.httpStreamFallbackWebSocket(failingNative))(url);
    expect(await outcome(next)).toBe("open");
    next.close();
    // No socket, so nothing for the browser to log, and no probe either.
    expect(opened).toEqual([url]);
    expect(calls.filter((call) => call.url === url.replace(/^ws/, "http"))).toEqual([]);
  });

  it("forgets after the memory's time is up, and when the stream is refused", async () => {
    const memory = await import("./stream-endpoint-memory.js");
    const url = "ws://app.test/jazz/apps/a2/ws";
    const now = Date.now();
    await memory.rememberStreamEndpoint(url, now);
    expect(await memory.recallStreamEndpoint(url, now + 1)).toBe(true);
    expect(await memory.recallStreamEndpoint(url, now + memory.STREAM_ENDPOINT_MEMORY_MS + 1)).toBe(
      false,
    );

    // Remembered, but the backend now refuses the stream: the session
    // fails as it would, and the next page tries a socket again.
    const page = await newPage();
    backendServingTheStream(url, page, { streamStatus: 404 });
    const refused = new (page.httpStreamFallbackWebSocket(failingNative))(url);
    expect(await outcome(refused)).toBe("closed");
    expect(opened).toEqual([]);
    await vi.waitFor(async () => expect(await memory.recallStreamEndpoint(url)).toBe(false));
  });

  it("keeps a switch made because upgrades fail in between to this page only", async () => {
    // A proxy that doesn't pass upgrades on may be fixed meanwhile: only
    // the backend's own word is kept.
    const url = "ws://behind-proxy.test/jazz/apps/a3/ws";
    vi.spyOn(console, "warn").mockImplementation(() => undefined);
    let page = await newPage();
    backendServingTheStream(url, page, { marker: false });
    const Socket = page.httpStreamFallbackWebSocket(failingNative);
    for (let attempt = 1; attempt < page.UPGRADE_FAILURES_BEFORE_STREAM; attempt++) {
      expect(await outcome(new Socket(url))).toBe("closed");
    }
    const switched = new Socket(url);
    expect(await outcome(switched)).toBe("open");
    switched.close();

    page = await newPage();
    backendServingTheStream(url, page, { marker: false });
    const before = opened.length;
    const next = new (page.httpStreamFallbackWebSocket(failingNative))(url);
    expect(await outcome(next)).toBe("closed");
    expect(opened.length).toBe(before + 1);
  });

  it("emits a close, and opens nothing, when closed while looking the endpoint up", async () => {
    const url = "ws://app.test/jazz/apps/a4/ws";
    const page = await newPage();
    backendServingTheStream(url, page);
    const socket = new (page.httpStreamFallbackWebSocket(failingNative))(url);
    const result = outcome(socket);
    socket.close();
    expect(await result).toBe("closed");
    expect(socket.readyState).toBe(3);
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(opened).toEqual([]);
  });
});

/** A page: a document, and Web Storage kept in memory (`refused`: storage
 * refused altogether; `full`: reads work, writes throw QuotaExceededError). */
function onAPage(options: { refused?: boolean; full?: boolean } = {}): Map<string, string> {
  const items = new Map<string, string>();
  vi.stubGlobal("document", {});
  vi.stubGlobal("localStorage", {
    getItem(key: string) {
      if (options.refused) throw new Error("SecurityError");
      return items.get(key) ?? null;
    },
    setItem(key: string, value: string) {
      if (options.refused) throw new Error("SecurityError");
      if (options.full) throw new Error("QuotaExceededError");
      items.set(key, value);
    },
    removeItem(key: string) {
      items.delete(key);
    },
  });
  return items;
}

describe("a page's copy of the memory", () => {
  it("asks IndexedDB once per endpoint, then opens the socket at once without asking", async () => {
    const items = onAPage();
    const url = "ws://app.test/jazz/apps/b1/ws";
    let page = await newPage();
    new (page.httpStreamFallbackWebSocket(failingNative))(url);
    // Nothing kept yet: the first page asks IndexedDB, and keeps its answer.
    expect(opened).toEqual([]);
    await vi.waitFor(() => expect(opened).toEqual([url]));
    expect(items.get(`jazz-stream-endpoint:${url}`)).toMatch(/^none:\d+$/);

    const lookups = vi.spyOn(globalThis.indexedDB, "open");
    page = await newPage();
    new (page.httpStreamFallbackWebSocket(failingNative))(url);
    // In the constructor already, not after a lookup.
    expect(opened).toEqual([url, url]);
    expect(lookups).not.toHaveBeenCalled();
  });

  it("goes straight to the stream on a new page once the backend said it serves one, also from the copy alone", async () => {
    const items = onAPage();
    const url = "ws://app.test/jazz/apps/b2/ws";
    let page = await newPage();
    backendServingTheStream(url, page);
    const first = new (page.httpStreamFallbackWebSocket(failingNative))(url);
    expect(await outcome(first)).toBe("open");
    first.close();
    expect(opened).toEqual([url]);
    await vi.waitFor(() => expect(items.get(`jazz-stream-endpoint:${url}`)).toMatch(/^stream:/));

    // IndexedDB emptied meanwhile: the page's copy still says so.
    vi.stubGlobal("indexedDB", new IDBFactory());
    page = await newPage();
    backendServingTheStream(url, page);
    const next = new (page.httpStreamFallbackWebSocket(failingNative))(url);
    expect(await outcome(next)).toBe("open");
    next.close();
    expect(opened).toEqual([url]);
  });

  it("goes to the stream when only IndexedDB knows the endpoint, as after an upgrade or a worker learning it", async () => {
    const memory = await import("./stream-endpoint-memory.js");
    const url = "ws://app.test/jazz/apps/b5/ws";
    // Learned where there is no page copy (a worker, or before the copy existed).
    await memory.rememberStreamEndpoint(url);
    onAPage();
    const page = await newPage();
    backendServingTheStream(url, page);
    const socket = new (page.httpStreamFallbackWebSocket(failingNative))(url);
    expect(await outcome(socket)).toBe("open");
    socket.close();
    expect(opened).toEqual([]);
    expect(memory.recallStreamEndpointNow(url)).toBe(true);
  });

  it("asks IndexedDB on every page while storage is full, and fails no socket for it", async () => {
    onAPage({ full: true });
    const memory = await import("./stream-endpoint-memory.js");
    const url = "ws://app.test/jazz/apps/b6/ws";
    for (let load = 0; load < 2; load++) {
      const page = await newPage();
      backendServingTheStream(url, page);
      const socket = new (page.httpStreamFallbackWebSocket(failingNative))(url);
      expect(await outcome(socket)).toBe("open");
      socket.close();
      expect(memory.recallStreamEndpointNow(url)).toBeUndefined();
    }
    // The first page learned it the hard way, once; the second asked IndexedDB.
    expect(opened).toEqual([url]);
  });

  it("forgets the stream when it is refused, and removes what expired", async () => {
    const items = onAPage();
    const memory = await import("./stream-endpoint-memory.js");
    const url = "ws://app.test/jazz/apps/b3/ws";
    const now = Date.now();
    await memory.rememberStreamEndpoint(url, now);
    expect(memory.recallStreamEndpointNow(url, now + 1)).toBe(true);
    // Expired: no answer, and nothing left behind.
    expect(
      memory.recallStreamEndpointNow(url, now + memory.STREAM_ENDPOINT_MEMORY_MS + 1),
    ).toBeUndefined();
    expect(items.size).toBe(0);

    await memory.rememberStreamEndpoint(url);
    const page = await newPage();
    backendServingTheStream(url, page, { streamStatus: 404 });
    const refused = new (page.httpStreamFallbackWebSocket(failingNative))(url);
    expect(await outcome(refused)).toBe("closed");
    await vi.waitFor(() => expect(memory.recallStreamEndpointNow(url)).toBe(false));
  });

  it("asks IndexedDB, as a worker does, where the page's storage is refused", async () => {
    onAPage({ refused: true });
    const memory = await import("./stream-endpoint-memory.js");
    const url = "ws://app.test/jazz/apps/b4/ws";
    expect(memory.recallStreamEndpointNow(url)).toBeUndefined();
    await memory.rememberStreamEndpoint(url);
    const page = await newPage();
    backendServingTheStream(url, page);
    const socket = new (page.httpStreamFallbackWebSocket(failingNative))(url);
    expect(await outcome(socket)).toBe("open");
    socket.close();
    expect(opened).toEqual([]);
  });
});

describe("closing a stream session", () => {
  it("ends it on the backend before letting the response go", async () => {
    // Aborted first, the response would end the session on its own, and
    // the DELETE would find it gone: an error status the browser logs.
    const url = "ws://app.test/jazz/apps/a5/ws";
    const page = await newPage();
    const calls = backendServingTheStream(url, page);
    const stream = new page.HttpStreamSocket(url);
    expect(await outcome(stream)).toBe("open");
    stream.close();
    await vi.waitFor(() => expect(calls.some((call) => call.method === "DELETE")).toBe(true));
    const deleted = calls.find((call) => call.method === "DELETE")!;
    expect(deleted.url).toBe(`${page.streamUrlForSocketUrl(url)}/s1`);
    expect(deleted.streamAborted).toBe(false);
  });
});
