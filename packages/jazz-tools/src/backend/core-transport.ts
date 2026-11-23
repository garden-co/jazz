/**
 * A backend sends HTTP requests and opens a socket per core session.
 * Node's WebSocket accepts upgrade headers directly. A Worker upgrades with
 * fetch, using the global fetch for a remote core or stub.fetch for a binding.
 */

import { backendNative } from "./backend-native.js";

/** What `openRemoteCore` uses of a WebSocket. */
export interface CoreWebSocket {
  binaryType: string;
  readonly readyState: number;
  send(data: Uint8Array): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: "open", listener: () => void): void;
  addEventListener(type: "message", listener: (event: { data: unknown }) => void): void;
  addEventListener(
    type: "close",
    listener: (event: { code: number; reason: string }) => void,
  ): void;
  addEventListener(type: "error", listener: () => void): void;
}

export interface CoreTransport {
  fetch(url: string, init: RequestInit): Promise<Response>;
  /** A socket to `url` (ws: or wss:), its upgrade carrying `headers`. */
  openWebSocket(url: string, headers: Record<string, string>): CoreWebSocket;
}

/** A remote core reached through the host's HTTP and WebSocket APIs. */
export function networkTransport(): CoreTransport {
  // A Worker sends upgrade headers through fetch. Keep the backend credential
  // on the configured origin if the core answers with a redirect.
  if (backendNative().host === "workerd") {
    return bindingTransport({
      fetch: (input, init) => fetch(input, { ...init, redirect: "manual" }),
    });
  }
  return {
    fetch: (url, init) => fetch(url, init),
    openWebSocket(url, headers) {
      if (!globalThis.WebSocket) {
        throw new Error("remote() needs a global WebSocket (Node 22 or newer)");
      }
      // Node's WebSocket (undici) takes upgrade headers as a second argument.
      const NodeWebSocket = globalThis.WebSocket as unknown as new (
        url: string,
        options?: { headers?: Record<string, string> },
      ) => CoreWebSocket;
      return new NodeWebSocket(url, { headers });
    },
  };
}

/** A Durable Object namespace binding, or a stub/service binding (anything with `fetch`). */
export type CoreBinding =
  | { idFromName(name: string): unknown; get(id: unknown): { fetch: StubFetch } }
  | { fetch: StubFetch };

type StubFetch = (input: string | Request, init?: RequestInit) => Promise<Response>;

/** The name `routeJazzRequest` (jazz-wasm's Durable Object host) gives the app's one object. */
const CORE_OBJECT_NAME = "jazz";

/** The origin requests through a binding carry; the object serves its one app whatever it is. */
export const BINDING_ORIGIN = "https://jazz-core.internal";

/** A core in a Durable Object of this Worker, through its binding. */
export function bindingTransport(binding: CoreBinding): CoreTransport {
  const stub =
    "idFromName" in binding ? binding.get(binding.idFromName(CORE_OBJECT_NAME)) : binding;
  const stubFetch: StubFetch = (input, init) => stub.fetch(input, init);
  return {
    fetch: (url, init) => stubFetch(url, init),
    openWebSocket(url, headers) {
      return upgradedSocket(
        stubFetch(url.replace(/^ws/, "http"), { headers: { ...headers, Upgrade: "websocket" } }),
      );
    },
  };
}

/**
 * A WebSocket a `fetch` upgrade answers (workerd's `response.webSocket`),
 * usable at once: messages sent before the upgrade completes are queued by
 * the caller, which waits for `open`.
 */
function upgradedSocket(upgrade: Promise<Response>): CoreWebSocket {
  type Listener = (event?: never) => void;
  const listeners = new Map<string, Listener[]>();
  const emit = (type: string, event?: unknown) => {
    for (const listener of listeners.get(type) ?? []) (listener as (event: unknown) => void)(event);
  };
  let socket: WorkerdWebSocket | undefined;
  let state = 0;
  let closing: { code?: number; reason?: string } | undefined;
  upgrade.then(
    (response) => {
      const ws = (response as Response & { webSocket?: WorkerdWebSocket }).webSocket;
      if (response.status !== 101 || !ws) {
        state = 3;
        emit("error");
        emit("close", { code: 1006, reason: `the core refused the socket (${response.status})` });
        return;
      }
      socket = ws;
      // workerd delivers binary messages as Blobs unless told otherwise.
      ws.binaryType = "arraybuffer";
      ws.accept();
      ws.addEventListener("message", (event) => emit("message", { data: event.data }));
      ws.addEventListener("close", (event) => {
        state = 3;
        emit("close", { code: event.code, reason: event.reason });
      });
      ws.addEventListener("error", () => emit("error"));
      if (closing) {
        state = 2;
        ws.close(closing.code, closing.reason);
        return;
      }
      state = 1;
      emit("open");
    },
    () => {
      state = 3;
      emit("error");
      emit("close", { code: 1006, reason: "connection to the core failed" });
    },
  );
  return {
    binaryType: "arraybuffer",
    get readyState() {
      return state;
    },
    send(data) {
      if (state !== 1 || !socket) throw new Error("the socket to the core is not open");
      socket.send(data);
    },
    close(code, reason) {
      if (state >= 2) return;
      if (!socket) {
        closing = { code, reason };
        return;
      }
      state = 2;
      socket.close(code, reason);
    },
    addEventListener(type: string, listener: Listener) {
      listeners.set(type, [...(listeners.get(type) ?? []), listener]);
    },
  } as CoreWebSocket;
}

/** workerd's server-side WebSocket, as a fetch upgrade or `WebSocketPair` hands it over. */
export interface WorkerdWebSocket {
  binaryType?: string;
  accept(): void;
  send(data: string | ArrayBuffer | ArrayBufferView): void;
  close(code?: number, reason?: string): void;
  addEventListener(type: "message", listener: (event: { data: unknown }) => void): void;
  addEventListener(
    type: "close",
    listener: (event: { code: number; reason: string }) => void,
  ): void;
  addEventListener(type: "error", listener: (event: unknown) => void): void;
}
