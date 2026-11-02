import type { BrowserWebSocket, WebSocketConstructor } from "./native-runtime/websocket.js";

/** A backend client's credential: its workload's OIDC token, or how to get a fresh one. */
export type BackendClientCredential = string | (() => string | Promise<string>);

/**
 * `fetch` and `WebSocket` that send `credential` as `Authorization: Bearer`
 * to `serverUrl`'s origin (and nowhere else), fetching a fresh token for
 * every request and socket. For backend clients: Node processes that sync
 * through an app's main backend with their workload's token.
 */
export function credentialedTransport(
  serverUrl: string,
  credential: BackendClientCredential,
  base: { fetch: typeof fetch; WebSocket: unknown } = globalThis,
): { fetch: typeof fetch; WebSocket: WebSocketConstructor } {
  const origin = new URL(serverUrl.replace(/^ws/, "http")).origin;
  const token = async () => {
    const value = typeof credential === "function" ? await credential() : credential;
    if (typeof value !== "string" || !/^[A-Za-z0-9_\-.]+$/.test(value)) {
      throw new Error("Jazz: a backend client's credential must be a token (a JWT)");
    }
    return value;
  };
  const forOrigin = (url: string) => {
    try {
      return new URL(url.replace(/^ws/, "http")).origin === origin;
    } catch {
      return false;
    }
  };
  const credentialedFetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
    const request = new Request(input, init);
    if (forOrigin(request.url)) request.headers.set("authorization", `Bearer ${await token()}`);
    return base.fetch(request);
  }) as typeof fetch;
  const Base = base.WebSocket as new (
    url: string,
    options?: { headers?: Record<string, string> },
  ) => BrowserWebSocket;
  if (typeof Base !== "function") throw new Error("Jazz: this platform has no WebSocket");
  return {
    fetch: credentialedFetch,
    WebSocket: lazySocket(Base, (url) => (forOrigin(url) ? token() : null)),
  };
}

type Listener = (event?: unknown) => void;

/**
 * A WebSocket that opens once its token is ready: until then it is
 * CONNECTING, keeps its listeners, and a close cancels it.
 */
function lazySocket(
  Base: new (url: string, options?: { headers?: Record<string, string> }) => BrowserWebSocket,
  tokenFor: (url: string) => Promise<string> | null,
): WebSocketConstructor {
  return class LazySocket {
    private type: "arraybuffer" | "blob" = "arraybuffer";
    private inner: BrowserWebSocket | undefined;
    private closed = false;
    private readonly listeners: Array<[string, Listener]> = [];

    constructor(url: string) {
      const pending = tokenFor(url);
      if (!pending) {
        this.attach(new Base(url));
        return;
      }
      pending.then(
        (token) => {
          if (this.closed) return;
          this.attach(new Base(url, { headers: { authorization: `Bearer ${token}` } }));
        },
        (error: unknown) => {
          if (this.closed) return;
          this.closed = true;
          console.error("Jazz: the backend client's credential failed", error);
          this.emit("error", error);
          this.emit("close", { code: 1006, reason: "credential unavailable" });
        },
      );
    }

    get binaryType(): "arraybuffer" | "blob" {
      return this.inner?.binaryType ?? this.type;
    }

    set binaryType(value: "arraybuffer" | "blob") {
      this.type = value;
      if (this.inner) this.inner.binaryType = value;
    }

    private attach(inner: BrowserWebSocket) {
      inner.binaryType = this.type;
      this.inner = inner;
      for (const [type, listener] of this.listeners) {
        inner.addEventListener(type as "open", listener as () => void);
      }
    }

    private emit(type: string, event: unknown) {
      for (const [kind, listener] of this.listeners) if (kind === type) listener(event);
    }

    get readyState(): number {
      if (this.inner) return this.inner.readyState;
      return this.closed ? 3 : 0;
    }

    send(data: Uint8Array | string): void {
      if (!this.inner) throw new Error("WebSocket is not open");
      this.inner.send(data);
    }

    close(): void {
      if (this.inner) return this.inner.close();
      if (this.closed) return;
      this.closed = true;
      // Like a WebSocket's, the close event comes after close() returns.
      queueMicrotask(() => this.emit("close", { code: 1000, reason: "" }));
    }

    addEventListener(type: string, listener: Listener): void {
      this.listeners.push([type, listener]);
      this.inner?.addEventListener(type as "open", listener as () => void);
    }
  } as unknown as WebSocketConstructor;
}
