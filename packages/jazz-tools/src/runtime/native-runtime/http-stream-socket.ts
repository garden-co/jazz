import type { BrowserWebSocket, WebSocketConstructor } from "./websocket.js";

/**
 * The HTTP stream transport: a WebSocket-shaped session over plain requests,
 * for hosts that can't upgrade a same-origin socket (such as `next dev`).
 *
 * - `POST <base>/apps/<appId>/stream` opens a session. Its response body is a
 *   stream of records; the first carries the session id.
 * - `POST <base>/apps/<appId>/stream/<session>` passes messages to the core,
 *   as records in the request body, in order.
 * - `DELETE <base>/apps/<appId>/stream/<session>` ends the session.
 *
 * A record is `[kind u8][length u32 BE][payload]`.
 */
export const STREAM_RECORD_SESSION = 0;
/** Header carrying the stream session's key, which the session record hands over. */
const STREAM_KEY_HEADER = "x-jazz-stream-key";
export const STREAM_RECORD_MESSAGE = 1;
export const STREAM_RECORD_CLOSE = 2;

export function encodeStreamRecord(kind: number, payload: Uint8Array): Uint8Array {
  const record = new Uint8Array(5 + payload.byteLength);
  record[0] = kind;
  new DataView(record.buffer).setUint32(1, payload.byteLength, false);
  record.set(payload, 5);
  return record;
}

export function concatBytes(parts: readonly Uint8Array[]): Uint8Array {
  const total = parts.reduce((sum, part) => sum + part.byteLength, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.byteLength;
  }
  return out;
}

/** Incrementally splits a byte stream into records. */
export class StreamRecordReader {
  private buffered: Uint8Array = new Uint8Array(0);

  push(chunk: Uint8Array): Array<{ kind: number; payload: Uint8Array }> {
    this.buffered = this.buffered.byteLength ? concatBytes([this.buffered, chunk]) : chunk;
    const records: Array<{ kind: number; payload: Uint8Array }> = [];
    let offset = 0;
    while (this.buffered.byteLength - offset >= 5) {
      const view = new DataView(this.buffered.buffer, this.buffered.byteOffset + offset);
      const length = view.getUint32(1, false);
      if (this.buffered.byteLength - offset < 5 + length) break;
      records.push({
        kind: this.buffered[offset]!,
        payload: this.buffered.slice(offset + 5, offset + 5 + length),
      });
      offset += 5 + length;
    }
    this.buffered = this.buffered.slice(offset);
    return records;
  }

  /** Bytes of an unfinished record. */
  get pending(): number {
    return this.buffered.byteLength;
  }
}

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export function encodeStreamClose(code: number, reason: string): Uint8Array {
  return encodeStreamRecord(
    STREAM_RECORD_CLOSE,
    textEncoder.encode(JSON.stringify({ code, reason })),
  );
}

/** `ws(s)://host/base/apps/<id>/ws` → `http(s)://host/base/apps/<id>/stream`. */
export function streamUrlForSocketUrl(url: string): string {
  const parsed = new URL(url);
  if (parsed.protocol === "ws:") parsed.protocol = "http:";
  if (parsed.protocol === "wss:") parsed.protocol = "https:";
  parsed.pathname = parsed.pathname.replace(/\/ws\/?$/, "/stream");
  return parsed.toString();
}

type Listener = (event?: any) => void;

/** Event plumbing shared by the WebSocket-shaped classes below. */
class SocketEvents {
  private readonly listeners = new Map<string, Listener[]>();

  addEventListener(type: string, listener: Listener): void {
    const list = this.listeners.get(type) ?? [];
    list.push(listener);
    this.listeners.set(type, list);
  }

  protected emit(type: string, event?: unknown): void {
    for (const listener of this.listeners.get(type) ?? []) listener(event);
  }
}

/** One sync session over the HTTP stream transport. */
export class HttpStreamSocket extends SocketEvents implements BrowserWebSocket {
  binaryType: "arraybuffer" | "blob" = "arraybuffer";
  readyState = 0;
  private readonly streamUrl: string;
  private readonly abort = new AbortController();
  private session: { id: string; key: string } | undefined;
  private queued: Uint8Array[] = [];
  private sending = false;

  constructor(url: string) {
    super();
    this.streamUrl = streamUrlForSocketUrl(url);
    void this.run();
  }

  send(data: Uint8Array | string): void {
    if (this.readyState !== 1) throw new Error("HTTP stream session is not open");
    const bytes = typeof data === "string" ? textEncoder.encode(data) : data;
    this.queued.push(encodeStreamRecord(STREAM_RECORD_MESSAGE, bytes));
    void this.flush();
  }

  close(): void {
    if (this.readyState >= 2) return;
    const session = this.session;
    this.finish(1000, "client closed");
    if (session) {
      void fetch(`${this.streamUrl}/${encodeURIComponent(session.id)}`, {
        method: "DELETE",
        credentials: "same-origin",
        headers: { [STREAM_KEY_HEADER]: session.key },
      }).catch(() => undefined);
    }
  }

  private async run(): Promise<void> {
    try {
      const response = await fetch(this.streamUrl, {
        method: "POST",
        credentials: "same-origin",
        signal: this.abort.signal,
      });
      if (!response.ok || !response.body) {
        throw new Error(`HTTP stream refused (${response.status})`);
      }
      const reader = response.body.getReader();
      const records = new StreamRecordReader();
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        for (const record of records.push(value)) {
          this.receive(record.kind, record.payload);
          if (this.readyState >= 2) return;
        }
      }
      this.fail(1006, "HTTP stream ended");
    } catch (error) {
      if (this.readyState >= 2) return;
      this.emit("error", error);
      this.fail(1006, error instanceof Error ? error.message : String(error));
    }
  }

  private receive(kind: number, payload: Uint8Array): void {
    if (kind === STREAM_RECORD_SESSION) {
      this.session = JSON.parse(textDecoder.decode(payload)) as { id: string; key: string };
      this.readyState = 1;
      this.emit("open");
    } else if (kind === STREAM_RECORD_MESSAGE) {
      const data = payload.buffer.slice(
        payload.byteOffset,
        payload.byteOffset + payload.byteLength,
      );
      this.emit("message", { data });
    } else if (kind === STREAM_RECORD_CLOSE) {
      const { code, reason } = JSON.parse(textDecoder.decode(payload)) as {
        code: number;
        reason: string;
      };
      this.finish(code, reason);
    }
  }

  private async flush(): Promise<void> {
    if (this.sending || !this.session) return;
    this.sending = true;
    try {
      while (this.queued.length > 0 && this.readyState === 1) {
        const body = concatBytes(this.queued);
        this.queued = [];
        const response = await fetch(`${this.streamUrl}/${encodeURIComponent(this.session.id)}`, {
          method: "POST",
          credentials: "same-origin",
          headers: {
            "Content-Type": "application/octet-stream",
            [STREAM_KEY_HEADER]: this.session.key,
          },
          body: body as BodyInit,
        });
        if (!response.ok) throw new Error(`HTTP stream send refused (${response.status})`);
      }
    } catch (error) {
      this.emit("error", error);
      this.close();
    } finally {
      this.sending = false;
    }
  }

  private fail(code: number, reason: string): void {
    this.finish(code, reason);
  }

  private finish(code: number, reason: string): void {
    if (this.readyState >= 2) return;
    this.readyState = 3;
    this.abort.abort();
    this.emit("close", { code, reason });
  }
}

/**
 * Endpoints on this page known to serve the HTTP stream and not upgrade
 * sockets: a createJazz backend answered a plain request for the socket
 * path with 426 and the stream marker. Only that answer switches an
 * endpoint; a socket that fails for any other reason (a restart, a proxy to
 * a sync server that has no stream) fails as a socket would.
 */
const streamEndpoints = new Set<string>();

/** Header on createJazz's 426 for a socket path (see `createJazz`). */
const STREAM_AVAILABLE_HEADER = "x-jazz-stream";

async function servesStreamInstead(url: string): Promise<boolean> {
  try {
    const response = await fetch(url.replace(/^ws/, "http"), {
      method: "GET",
      credentials: "same-origin",
    });
    void response.body?.cancel().catch(() => undefined);
    return response.status === 426 && response.headers.get(STREAM_AVAILABLE_HEADER) === "1";
  } catch {
    return false;
  }
}

/**
 * A WebSocket constructor for a same-origin Jazz endpoint: a native socket
 * when the host upgrades it, else, on a createJazz backend that can't, the
 * HTTP stream transport. The switch happens before the session opens, so the
 * carrier never sees it.
 */
export function sameOriginWebSocketConstructor(native: WebSocketConstructor): WebSocketConstructor {
  return class SameOriginSocket extends SocketEvents implements BrowserWebSocket {
    binaryType: "arraybuffer" | "blob" = "arraybuffer";
    private inner: BrowserWebSocket;
    private settled = false;
    /** The native socket failed and we're asking whether to use the stream. */
    private probing = false;

    constructor(private readonly url: string) {
      super();
      const stream = streamEndpoints.has(url);
      this.inner = stream ? new HttpStreamSocket(url) : this.openNative();
      if (stream) this.forward(this.inner);
    }

    get readyState(): number {
      // Still connecting while the probe runs: the session hasn't failed yet.
      return this.probing ? 0 : this.inner.readyState;
    }

    send(data: Uint8Array | string): void {
      this.inner.send(data);
    }

    close(): void {
      this.settled = true;
      this.probing = false;
      this.inner.close();
    }

    private openNative(): BrowserWebSocket {
      const socket = new native(this.url);
      socket.binaryType = "arraybuffer";
      let opened = false;
      let failure: { error?: unknown; close?: unknown } | undefined;
      const failedBeforeOpen = (kind: "error" | "close", event: unknown) => {
        if (this.settled) return;
        if (failure) {
          failure[kind] ??= event;
          return;
        }
        failure = { [kind]: event };
        this.probing = true;
        void servesStreamInstead(this.url).then((stream) => {
          this.probing = false;
          if (this.settled) return;
          this.settled = true;
          if (stream) {
            streamEndpoints.add(this.url);
            this.inner = new HttpStreamSocket(this.url);
            this.forward(this.inner);
            return;
          }
          if (failure?.error !== undefined) this.emit("error", failure.error);
          this.emit("close", failure?.close ?? { code: 1006, reason: "" });
        });
      };
      socket.addEventListener("open", () => {
        opened = true;
        this.settled = true;
        this.emit("open");
      });
      socket.addEventListener("message", (event) => this.emit("message", event));
      socket.addEventListener("error", (event) => {
        if (opened) this.emit("error", event);
        else failedBeforeOpen("error", event);
      });
      socket.addEventListener("close", (event) => {
        if (opened) this.emit("close", event);
        else failedBeforeOpen("close", event);
      });
      return socket;
    }

    private forward(socket: BrowserWebSocket): void {
      socket.addEventListener("open", () => this.emit("open"));
      socket.addEventListener("message", (event) => this.emit("message", event));
      socket.addEventListener("error", (event) => this.emit("error", event));
      socket.addEventListener("close", (event) => this.emit("close", event));
    }
  };
}
