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

/** The most one send request carries, unless a single record is larger. */
const MAX_SEND_BYTES = 4 * 1024 * 1024;

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
        // A bounded body per request, so the backend's pace (it answers a
        // send once the core has taken it) holds the next one back.
        let take = 0;
        let bytes = 0;
        while (
          take < this.queued.length &&
          (take === 0 || bytes + this.queued[take]!.byteLength <= MAX_SEND_BYTES)
        ) {
          bytes += this.queued[take]!.byteLength;
          take += 1;
        }
        const body = concatBytes(this.queued.splice(0, take));
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
 * Endpoints on this page that sync over the HTTP stream instead of a
 * socket. Two answers to a plain request for the socket path, after a socket
 * failed before it opened, switch an endpoint:
 * - a createJazz backend that can't upgrade sockets: 426 with the stream marker;
 * - a createJazz backend that does upgrade them (426, no marker), reached
 *   this way while `UPGRADE_FAILURES_BEFORE_STREAM` sockets in a row failed:
 *   something in between (a proxy, a dev server, a PaaS edge) doesn't pass
 *   upgrades on.
 * A socket that fails for any other reason (a restart, a proxy to a sync
 * server that has no stream) fails as a socket would.
 */
const streamEndpoints = new Set<string>();

/** Sockets in a row that failed to an endpoint whose backend answers and upgrades. */
const failedUpgrades = new Map<string, number>();

/**
 * How many failed sockets in a row, to a backend that answers and says it
 * upgrades them, mean they never get through: past a restart or a draining
 * instance, which fail a socket once or twice and then let it through.
 */
export const UPGRADE_FAILURES_BEFORE_STREAM = 3;

/**
 * How long a socket may stay connecting before it counts as failed before
 * opening. Some proxies (`vite preview` without `ws`, a fetch-based proxy)
 * never answer an upgrade at all, so the socket neither opens nor fails.
 * The probe that follows gets as long.
 */
export const SOCKET_OPEN_DEADLINE_MS = 10_000;

/**
 * How many sockets in a row that got no answer at all, to a backend that
 * answers and upgrades them, mean upgrades never get through. Two, not one:
 * a scale-to-zero host's edge holds the first upgrade after idle while the
 * instance cold-starts, and lets the next one through.
 */
export const UPGRADE_HANGS_BEFORE_STREAM = 2;

/** Sockets in a row that got no answer, to an endpoint whose backend answers and upgrades. */
const hungUpgrades = new Map<string, number>();

/** Header on createJazz's 426 for a socket path (see `createJazz`). */
const STREAM_AVAILABLE_HEADER = "x-jazz-stream";

/** Endpoints already warned that the backend refuses this page's origin. */
const refusedOrigins = new Set<string>();

/**
 * What a plain request for a socket path says about the endpoint. A POST,
 * so the browser sends the page's Origin and the backend checks it as it
 * checks the socket's: a refused origin fails the socket too, and is not a
 * proxy's doing.
 */
async function probeSocketPath(
  url: string,
): Promise<"stream" | "upgrades" | "refused" | "unknown"> {
  try {
    const response = await fetch(url.replace(/^ws/, "http"), {
      method: "POST",
      credentials: "same-origin",
      // A stall that holds plain requests too must not keep the socket
      // connecting: a probe that gets no answer says nothing.
      signal: AbortSignal.timeout?.(SOCKET_OPEN_DEADLINE_MS),
    });
    void response.body?.cancel().catch(() => undefined);
    if (response.status === 403) return "refused";
    if (response.status !== 426) return "unknown";
    return response.headers.get(STREAM_AVAILABLE_HEADER) === "1" ? "stream" : "upgrades";
  } catch {
    return "unknown";
  }
}

/** Whether a socket to `url` that just failed before opening should give way to the stream.
 * Upgrades that got no answer at all (`hung`) count on their own: a backend
 * that restarts or drains refuses a socket, it doesn't leave one hanging,
 * so fewer of them in a row ({@link UPGRADE_HANGS_BEFORE_STREAM}) switch. */
async function servesStreamInstead(url: string, hung = false): Promise<boolean> {
  const answer = await probeSocketPath(url);
  if (answer === "stream") return true;
  if (answer !== "upgrades") {
    // Not a backend that answered and upgrades (down, restarting, or
    // refusing this origin): the failures are no longer in a row.
    failedUpgrades.delete(url);
    hungUpgrades.delete(url);
    if (answer === "refused" && !refusedOrigins.has(url)) {
      refusedOrigins.add(url);
      console.warn(
        `jazz: the backend at ${url} refuses this page's origin (403). If a proxy rewrites the Host header, set the backend's publicUrl or allowedOrigins.`,
      );
    }
    return false;
  }
  const failures = (failedUpgrades.get(url) ?? 0) + 1;
  failedUpgrades.set(url, failures);
  const hangs = hung ? (hungUpgrades.get(url) ?? 0) + 1 : 0;
  if (hung) hungUpgrades.set(url, hangs);
  else hungUpgrades.delete(url);
  const hangsSwitch = hangs >= UPGRADE_HANGS_BEFORE_STREAM;
  if (failures < UPGRADE_FAILURES_BEFORE_STREAM && !hangsSwitch) return false;
  failedUpgrades.delete(url);
  hungUpgrades.delete(url);
  if (hangsSwitch) {
    console.warn(
      `jazz: ${hangs} WebSockets in a row to ${url} got no answer in ${SOCKET_OPEN_DEADLINE_MS / 1000} s, although the backend answers and upgrades them: something in between (a proxy or dev server) doesn't pass WebSocket upgrades on. Syncing over the HTTP stream instead, for as long as this page is open.`,
    );
    return true;
  }
  console.warn(
    `jazz: ${failures} WebSockets in a row to ${url} failed, although the backend answers and upgrades them: something in between (a proxy or dev server) doesn't pass WebSocket upgrades on. Syncing over the HTTP stream instead, for as long as this page is open.`,
  );
  return true;
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
    /** Ends a socket that stays connecting (see {@link SOCKET_OPEN_DEADLINE_MS}). */
    private openDeadline: ReturnType<typeof setTimeout> | undefined;

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
      clearTimeout(this.openDeadline);
      this.inner.close();
    }

    private openNative(): BrowserWebSocket {
      const socket = new native(this.url);
      socket.binaryType = "arraybuffer";
      let opened = false;
      let failure: { error?: unknown; close?: unknown } | undefined;
      const failedBeforeOpen = (kind: "error" | "close", event: unknown, hung = false) => {
        clearTimeout(this.openDeadline);
        if (this.settled) return;
        if (failure) {
          failure[kind] ??= event;
          return;
        }
        failure = { [kind]: event };
        this.probing = true;
        void servesStreamInstead(this.url, hung).then((stream) => {
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
      this.openDeadline = setTimeout(() => {
        failedBeforeOpen(
          "close",
          { code: 1006, reason: "the WebSocket upgrade got no answer" },
          true,
        );
        socket.close();
      }, SOCKET_OPEN_DEADLINE_MS);
      socket.addEventListener("open", () => {
        // A socket given up on (its open deadline passed) says nothing more.
        if (failure) return;
        clearTimeout(this.openDeadline);
        opened = true;
        failedUpgrades.delete(this.url);
        hungUpgrades.delete(this.url);
        this.settled = true;
        this.emit("open");
      });
      socket.addEventListener("message", (event) => {
        if (!failure) this.emit("message", event);
      });
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
